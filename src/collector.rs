use crate::ch::ClickHouse;
use crate::config::CollectorConfig;
use crate::model::{Accumulator, Match, StateRow};
use crate::steam::SteamClient;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

#[derive(Deserialize)]
struct StateValue {
    value: String,
}

const SPEEDUP_FACTOR: f64 = 0.70;
const ERROR_BACKOFF_FACTOR: f64 = 2.0;
const MIN_ERROR_BACKOFF: Duration = Duration::from_secs(1);
const LAG_SMOOTHING: f64 = 0.8;

/// Keeps the processing lag bounded without targeting a fixed lag.
///
/// Lag is measured from the newest processed match end time
/// (`start_time + duration`) to now.
///
/// - Above `max_lag`: speed up; this is the safety ceiling that prevents
///   accumulating days of backlog.
/// - Below `min_lag`: slow down; we are effectively caught up.
/// - Inside the band: react to the lag trend. If lag is growing, speed up;
///   if it is shrinking, slow down; if it is stable, hold the interval.
/// - Empty batches slow down; errors back off multiplicatively.
///
/// The controller does not try to sit at a specific lag. It holds wherever
/// the lag stabilises inside the acceptable band.
#[derive(Debug, Clone)]
struct LagController {
    current: Duration,
    min: Duration,
    max: Duration,
    step: Duration,
    max_lag: u64,
    min_lag: u64,
    trend_threshold: f64,
    smoothed_lag: Option<f64>,
    last_match_end: Option<u64>,
}

impl LagController {
    fn new(
        initial: Duration,
        min: Duration,
        max: Duration,
        step: Duration,
        max_lag: Duration,
        min_lag: Duration,
        trend_threshold: Duration,
    ) -> Self {
        let min = min.max(Duration::from_millis(1));
        let max = max.max(min);
        Self {
            current: initial.max(min).min(max),
            min,
            max,
            step: step.max(Duration::from_millis(1)),
            max_lag: max_lag.as_secs(),
            min_lag: min_lag.as_secs(),
            trend_threshold: trend_threshold.as_secs_f64(),
            smoothed_lag: None,
            last_match_end: None,
        }
    }

    fn on_batch(&mut self, newest_match_end: Option<u64>, now: u64) {
        if let Some(end) = newest_match_end {
            self.last_match_end = Some(self.last_match_end.map_or(end, |old| old.max(end)));
        }
        let Some(lag) = self.lag(now) else {
            return;
        };

        let lag_value = lag as f64;
        let trend = match self.smoothed_lag {
            None => {
                self.smoothed_lag = Some(lag_value);
                0.0
            }
            Some(previous) => {
                let smoothed = LAG_SMOOTHING * previous + (1.0 - LAG_SMOOTHING) * lag_value;
                self.smoothed_lag = Some(smoothed);
                lag_value - smoothed
            }
        };

        if lag > self.max_lag {
            self.speed_up();
        } else if lag < self.min_lag {
            self.slow_down();
        } else if trend > self.trend_threshold {
            self.speed_up();
        } else if trend < -self.trend_threshold {
            self.slow_down();
        }
    }

    fn on_empty(&mut self) {
        self.slow_down();
    }

    fn on_error(&mut self) {
        let backoff = self
            .current
            .mul_f64(ERROR_BACKOFF_FACTOR)
            .max(MIN_ERROR_BACKOFF);
        self.current = backoff.min(self.max).max(self.min);
    }

    fn lag(&self, now: u64) -> Option<u64> {
        self.last_match_end.map(|end| now.saturating_sub(end))
    }

    fn max_lag_secs(&self) -> u64 {
        self.max_lag
    }

    fn speed_up(&mut self) {
        self.current = self.current.mul_f64(SPEEDUP_FACTOR).max(self.min);
    }

    fn slow_down(&mut self) {
        self.current = (self.current + self.step).min(self.max);
    }

    fn delay(&self) -> Duration {
        self.current
    }
}

pub async fn run(
    ch: ClickHouse,
    steam: SteamClient,
    cfg: CollectorConfig,
    ct: CancellationToken,
) -> Result<()> {
    let mut cursor = match load_cursor(&ch).await? {
        Some(cursor) => {
            info!(cursor, "resuming collector from checkpoint");
            cursor
        }
        None => {
            let cursor = match cfg.start_seq {
                Some(cursor) => cursor,
                None => loop {
                    if ct.is_cancelled() {
                        return Ok(());
                    }
                    match steam.fetch_latest_sequence().await {
                        Ok(cursor) => break cursor,
                        Err(error) => {
                            warn!(%error, "failed to determine startup sequence, retrying");
                            if !sleep_or_cancel(&ct, Duration::from_secs(10)).await {
                                return Ok(());
                            }
                        }
                    }
                },
            };
            save_cursor(&ch, cursor).await?;
            info!(cursor, "starting collector at sequence");
            cursor
        }
    };

    let mut rate = LagController::new(
        cfg.poll_interval,
        cfg.min_poll_interval,
        cfg.max_poll_interval,
        cfg.poll_step,
        cfg.max_lag,
        cfg.min_lag,
        cfg.lag_trend_threshold,
    );

    loop {
        if ct.is_cancelled() {
            break;
        }

        match steam
            .fetch_by_sequence(cursor.saturating_add(1), cfg.batch_size)
            .await
        {
            Ok(matches) if matches.is_empty() => {
                rate.on_empty();
                let now = now_secs();
                debug!(
                    cursor,
                    interval_ms = rate.delay().as_millis(),
                    lag_secs = rate.lag(now).unwrap_or(0),
                    max_lag_secs = rate.max_lag_secs(),
                    "no new matches from the steam sequence stream"
                );
                if !sleep_or_cancel(&ct, rate.delay()).await {
                    break;
                }
            }
            Ok(matches) => {
                let newest_end = newest_match_end(&matches);
                let batch_end = matches
                    .iter()
                    .filter_map(|m| m.get("match_seq_num").and_then(|v| v.as_u64()))
                    .max()
                    .unwrap_or(cursor);
                if batch_end <= cursor {
                    rate.on_error();
                    warn!(
                        cursor,
                        batch_end,
                        interval_ms = rate.delay().as_millis(),
                        "Steam returned a non-advancing sequence batch; backing off"
                    );
                    if !sleep_or_cancel(&ct, rate.delay()).await {
                        break;
                    }
                    continue;
                }

                let token = format!("{}-{}", cursor.saturating_add(1), batch_end);
                let (stored, rated) = match store_batch(&ch, &matches, &token).await {
                    Ok(result) => result,
                    Err(error) => {
                        rate.on_error();
                        warn!(
                            %error,
                            token,
                            interval_ms = rate.delay().as_millis(),
                            "failed to store batch, will retry"
                        );
                        if !sleep_or_cancel(&ct, rate.delay()).await {
                            break;
                        }
                        continue;
                    }
                };

                cursor = batch_end.max(cursor);
                if let Err(error) = save_cursor(&ch, cursor).await {
                    rate.on_error();
                    warn!(
                        %error,
                        cursor,
                        interval_ms = rate.delay().as_millis(),
                        "failed to save collector checkpoint, will retry"
                    );
                    if !sleep_or_cancel(&ct, rate.delay()).await {
                        break;
                    }
                    continue;
                }

                let now = now_secs();
                rate.on_batch(newest_end, now);
                debug!(
                    cursor,
                    fetched = matches.len(),
                    stored,
                    rated,
                    interval_ms = rate.delay().as_millis(),
                    lag_secs = rate.lag(now).unwrap_or(0),
                    max_lag_secs = rate.max_lag_secs(),
                    "stored sequence batch"
                );
                if !sleep_or_cancel(&ct, rate.delay()).await {
                    break;
                }
            }
            Err(error) => {
                rate.on_error();
                warn!(
                    %error,
                    interval_ms = rate.delay().as_millis(),
                    "steam fetch failed; backing off"
                );
                if !sleep_or_cancel(&ct, rate.delay()).await {
                    break;
                }
            }
        }
    }

    info!(cursor, "collector stopped");
    Ok(())
}
async fn store_batch(
    ch: &ClickHouse,
    values: &[serde_json::Value],
    token: &str,
) -> Result<(usize, usize)> {
    let mut games = Vec::new();
    for value in values {
        match Match::from_value(value) {
            Ok(game) if game.is_ranked() => games.push(game),
            Ok(_) => {}
            Err(error) => debug!(%error, "skipping malformed match"),
        }
    }
    if games.is_empty() {
        return Ok((0, 0));
    }

    let match_rows: Vec<_> = games.iter().map(Match::match_row).collect();
    let player_rows: Vec<_> = games.iter().flat_map(Match::player_rows).collect();
    let mut accumulator = Accumulator::default();
    for game in &games {
        accumulator.add(game);
    }
    let rated = accumulator.matches;
    let (matchup_rows, hero_rows) = accumulator.into_rows();

    ch.insert("matches", &match_rows, Some(token))
        .await
        .context("failed to insert matches")?;
    ch.insert("match_players", &player_rows, Some(token))
        .await
        .context("failed to insert match players")?;
    ch.insert(
        "hero_matchups_hourly",
        &matchup_rows,
        Some(&format!("{token}-mu")),
    )
    .await
    .context("failed to insert matchup aggregates")?;
    ch.insert(
        "hero_stats_hourly",
        &hero_rows,
        Some(&format!("{token}-hs")),
    )
    .await
    .context("failed to insert hero aggregates")?;

    Ok((games.len(), rated as usize))
}

async fn load_cursor(ch: &ClickHouse) -> Result<Option<u64>> {
    let rows: Vec<StateValue> = ch
        .query(&format!(
            "SELECT value FROM {} WHERE key = 'last_seq' ORDER BY updated_at DESC LIMIT 1",
            ch.table("collector_state")
        ))
        .await?;
    Ok(rows
        .into_iter()
        .next()
        .and_then(|row| row.value.parse::<u64>().ok()))
}

async fn save_cursor(ch: &ClickHouse, cursor: u64) -> Result<()> {
    let row = StateRow {
        key: "last_seq".into(),
        value: cursor.to_string(),
        updated_at: now_millis(),
    };
    ch.insert("collector_state", &[row], None).await
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn newest_match_end(matches: &[serde_json::Value]) -> Option<u64> {
    matches
        .iter()
        .filter_map(|m| {
            let start = m
                .get("start_time")
                .and_then(|v| v.as_u64())
                .filter(|v| *v > 0)?;
            let duration = m.get("duration").and_then(|v| v.as_u64()).unwrap_or(0);
            Some(start.saturating_add(duration))
        })
        .max()
}

async fn sleep_or_cancel(ct: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = ct.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: u64 = 1_800_000_000;

    fn controller() -> LagController {
        LagController::new(
            Duration::from_secs(2),
            Duration::from_millis(250),
            Duration::from_secs(30),
            Duration::from_millis(500),
            Duration::from_secs(3600),
            Duration::from_secs(300),
            Duration::from_secs(30),
        )
    }

    fn observe(rate: &mut LagController, lag_secs: u64, now: u64) {
        rate.on_batch(Some(now.saturating_sub(lag_secs)), now);
    }

    #[test]
    fn holds_inside_the_band_when_lag_is_stable() {
        let mut rate = controller();
        for index in 0..10 {
            observe(&mut rate, 1200, NOW + index * 60);
        }
        assert_eq!(rate.delay(), Duration::from_secs(2));
    }

    #[test]
    fn speeds_up_when_lag_grows() {
        let mut rate = controller();
        for index in 1..=20 {
            observe(&mut rate, 1200 + index * 60, NOW + index * 60);
        }
        assert_eq!(rate.delay(), Duration::from_millis(250));
    }

    #[test]
    fn slows_down_when_lag_shrinks() {
        let mut rate = controller();
        for index in 1..=20 {
            observe(&mut rate, 1800 - index * 60, NOW + index * 60);
        }
        assert!(rate.delay() > Duration::from_secs(2));
    }

    #[test]
    fn above_the_ceiling_speeds_up() {
        let mut rate = controller();
        for index in 0..20 {
            observe(&mut rate, 7200, NOW + index * 60);
        }
        assert_eq!(rate.delay(), Duration::from_millis(250));
    }

    #[test]
    fn below_the_caught_up_floor_slows_down() {
        let mut rate = controller();
        for index in 0..100 {
            observe(&mut rate, 60, NOW + index * 60);
        }
        assert_eq!(rate.delay(), Duration::from_secs(30));
    }

    #[test]
    fn empty_batches_slow_down_to_max() {
        let mut rate = controller();
        for _ in 0..100 {
            rate.on_empty();
        }
        assert_eq!(rate.delay(), Duration::from_secs(30));
    }

    #[test]
    fn errors_back_off_and_cap_at_max() {
        let mut rate = controller();
        rate.on_error();
        assert!(rate.delay() >= Duration::from_secs(1));
        for _ in 0..20 {
            rate.on_error();
        }
        assert_eq!(rate.delay(), Duration::from_secs(30));
    }

    #[test]
    fn behind_speeds_up_after_errors() {
        let mut rate = controller();
        for _ in 0..10 {
            rate.on_error();
        }
        let before = rate.delay();
        observe(&mut rate, 7200, NOW);
        assert!(rate.delay() < before);
    }

    #[test]
    fn initial_interval_is_clamped() {
        let max = Duration::from_secs(30);
        assert_eq!(
            LagController::new(
                Duration::from_secs(100),
                Duration::from_millis(250),
                max,
                Duration::from_millis(500),
                Duration::from_secs(3600),
                Duration::from_secs(300),
                Duration::from_secs(30),
            )
            .delay(),
            max
        );
        assert_eq!(
            LagController::new(
                Duration::from_millis(1),
                Duration::from_millis(250),
                max,
                Duration::from_millis(500),
                Duration::from_secs(3600),
                Duration::from_secs(300),
                Duration::from_secs(30),
            )
            .delay(),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn lag_uses_the_newest_match_end_time() {
        let matches = vec![
            json!({ "start_time": 1000, "duration": 500 }),
            json!({ "start_time": 2000, "duration": 100 }),
            json!({ "start_time": 2500 }),
        ];
        assert_eq!(newest_match_end(&matches), Some(2500));
    }

    #[test]
    fn ignores_matches_without_start_time() {
        let matches = vec![json!({ "duration": 500 }), json!({ "start_time": 0 })];
        assert_eq!(newest_match_end(&matches), None);
    }
}
