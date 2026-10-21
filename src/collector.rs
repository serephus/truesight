use crate::ch::ClickHouse;
use crate::config::CollectorConfig;
use crate::model::{Accumulator, Match, StateRow};
use crate::steam::SteamClient;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

#[derive(Deserialize)]
struct StateValue {
    value: String,
}

const MIN_ERROR_BACKOFF: Duration = Duration::from_secs(5);
const MAX_ERROR_BACKOFF: Duration = Duration::from_secs(60);

/// Rate controller ported from dota2-match-subscriber.
///
/// It estimates the stream rate in matches per minute (`mpm`) and keeps a
/// `distance` correction. Requests are scheduled on a fixed deadline; the
/// target is roughly `batch_size - 1` matches per request.
#[derive(Debug, Clone)]
struct MpmController {
    mpm: i64,
    distance: i64,
    target: i64,
    batch_size: usize,
    min_interval: Duration,
    max_interval: Duration,
}

impl MpmController {
    fn new(
        initial_interval: Duration,
        min_interval: Duration,
        max_interval: Duration,
        batch_size: u32,
    ) -> Self {
        let min_interval = min_interval.max(Duration::from_millis(1));
        let max_interval = max_interval.max(min_interval);
        let batch_size = batch_size.max(2) as usize;
        let target = (batch_size - 1) as i64;
        let initial_ms = initial_interval.as_millis().max(1) as i64;
        let mpm = (target * 60_000 / initial_ms).max(1);
        Self {
            mpm,
            distance: 0,
            target,
            batch_size,
            min_interval,
            max_interval,
        }
    }

    fn interval(&self) -> Duration {
        let rate = (self.mpm + self.distance * 5).max(1);
        let ms = (self.target * 60_000 / rate).max(1) as u64;
        Duration::from_millis(ms)
            .max(self.min_interval)
            .min(self.max_interval)
    }

    fn initial_delay(&self) -> Duration {
        self.interval()
    }

    fn on_batch(&mut self, fetched: usize, interval: Duration) {
        let interval_ms = interval.as_millis() as i64;
        let full_threshold = self.batch_size.saturating_sub(4);
        if fetched > full_threshold {
            self.mpm += 5;
            self.distance = self.distance.max(0) + self.mpm * interval_ms / 60_000 / 20;
        } else {
            self.mpm -= 5;
            self.distance = fetched as i64 - self.batch_size as i64;
        }
        self.mpm = self.mpm.max(1);
    }

    fn mpm(&self) -> i64 {
        self.mpm
    }

    fn distance(&self) -> i64 {
        self.distance
    }
}

fn error_backoff(consecutive_errors: u32) -> Duration {
    let seconds = MIN_ERROR_BACKOFF
        .as_secs()
        .saturating_mul(1u64 << consecutive_errors.min(4));
    Duration::from_secs(seconds.min(MAX_ERROR_BACKOFF.as_secs()))
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

    let mut controller = MpmController::new(
        cfg.poll_interval,
        cfg.min_poll_interval,
        cfg.max_poll_interval,
        cfg.batch_size,
    );
    let mut consecutive_errors = 0u32;
    let mut deadline = Instant::now();

    info!(
        cursor,
        mpm = controller.mpm(),
        interval_ms = controller.initial_delay().as_millis(),
        "collector rate initialized"
    );
    if !sleep_or_cancel(&ct, controller.initial_delay()).await {
        info!(cursor, "collector stopped");
        return Ok(());
    }

    loop {
        if ct.is_cancelled() {
            break;
        }

        let interval = controller.interval();
        deadline += interval;

        match steam
            .fetch_by_sequence(cursor.saturating_add(1), cfg.batch_size)
            .await
        {
            Ok(matches) if matches.is_empty() => {
                controller.on_batch(0, interval);
                consecutive_errors = 0;
                debug!(
                    cursor,
                    fetched = 0,
                    interval_ms = interval.as_millis(),
                    mpm = controller.mpm(),
                    distance = controller.distance(),
                    "no new matches from the steam sequence stream"
                );
                if !sleep_until_or_cancel(&ct, deadline).await {
                    break;
                }
            }
            Ok(matches) => {
                let batch_end = matches
                    .iter()
                    .filter_map(|m| m.get("match_seq_num").and_then(|v| v.as_u64()))
                    .max()
                    .unwrap_or(cursor);
                if batch_end <= cursor {
                    warn!(
                        cursor,
                        batch_end,
                        mpm = controller.mpm(),
                        distance = controller.distance(),
                        "Steam returned a non-advancing sequence batch; backing off"
                    );
                    if !error_wait(&ct, &mut consecutive_errors).await {
                        break;
                    }
                    deadline = Instant::now();
                    continue;
                }

                let token = format!("{}-{}", cursor.saturating_add(1), batch_end);
                let (stored, rated) = match store_batch(&ch, &matches, &token).await {
                    Ok(result) => result,
                    Err(error) => {
                        warn!(
                            %error,
                            token,
                            mpm = controller.mpm(),
                            distance = controller.distance(),
                            "failed to store batch, will retry"
                        );
                        if !error_wait(&ct, &mut consecutive_errors).await {
                            break;
                        }
                        deadline = Instant::now();
                        continue;
                    }
                };

                cursor = batch_end.max(cursor);
                if let Err(error) = save_cursor(&ch, cursor).await {
                    warn!(
                        %error,
                        cursor,
                        mpm = controller.mpm(),
                        distance = controller.distance(),
                        "failed to save collector checkpoint, will retry"
                    );
                    if !error_wait(&ct, &mut consecutive_errors).await {
                        break;
                    }
                    deadline = Instant::now();
                    continue;
                }

                controller.on_batch(matches.len(), interval);
                consecutive_errors = 0;
                debug!(
                    cursor,
                    fetched = matches.len(),
                    stored,
                    rated,
                    interval_ms = interval.as_millis(),
                    mpm = controller.mpm(),
                    distance = controller.distance(),
                    "stored sequence batch"
                );
                if !sleep_until_or_cancel(&ct, deadline).await {
                    break;
                }
            }
            Err(error) => {
                warn!(
                    %error,
                    mpm = controller.mpm(),
                    distance = controller.distance(),
                    "steam fetch failed; backing off"
                );
                if !error_wait(&ct, &mut consecutive_errors).await {
                    break;
                }
                deadline = Instant::now();
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

async fn error_wait(ct: &CancellationToken, consecutive_errors: &mut u32) -> bool {
    let delay = error_backoff(*consecutive_errors);
    *consecutive_errors = consecutive_errors.saturating_add(1);
    sleep_or_cancel(ct, delay).await
}

async fn sleep_or_cancel(ct: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = ct.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

async fn sleep_until_or_cancel(ct: &CancellationToken, deadline: Instant) -> bool {
    tokio::select! {
        _ = ct.cancelled() => false,
        _ = tokio::time::sleep_until(deadline) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn controller() -> MpmController {
        MpmController::new(
            Duration::from_millis(5000),
            Duration::from_millis(2000),
            Duration::from_secs(30),
            100,
        )
    }

    #[test]
    fn derives_initial_mpm_from_interval() {
        let controller = controller();
        assert_eq!(controller.mpm(), 1188);
        assert_eq!(controller.interval(), Duration::from_millis(5000));
    }

    #[test]
    fn full_batches_speed_up() {
        let mut controller = controller();
        let initial = controller.interval();
        for _ in 0..50 {
            let interval = controller.interval();
            controller.on_batch(100, interval);
        }
        assert!(controller.interval() < initial);
    }

    #[test]
    fn partial_batches_slow_down() {
        let mut controller = controller();
        let initial = controller.interval();
        for _ in 0..100 {
            let interval = controller.interval();
            controller.on_batch(50, interval);
        }
        assert!(controller.interval() > initial);
    }

    #[test]
    fn empty_batches_slow_down() {
        let mut controller = controller();
        let initial = controller.interval();
        for _ in 0..100 {
            let interval = controller.interval();
            controller.on_batch(0, interval);
        }
        assert!(controller.interval() > initial);
    }

    #[test]
    fn full_batch_increases_mpm() {
        let mut controller = controller();
        let before = controller.mpm();
        let interval = controller.interval();
        controller.on_batch(100, interval);
        assert!(controller.mpm() > before);
    }

    #[test]
    fn partial_batch_sets_negative_distance() {
        let mut controller = controller();
        let interval = controller.interval();
        controller.on_batch(50, interval);
        assert!(controller.distance() < 0);
    }

    #[test]
    fn initial_interval_is_clamped() {
        let max = Duration::from_secs(30);
        assert_eq!(
            MpmController::new(
                Duration::from_secs(100),
                Duration::from_millis(2000),
                max,
                100,
            )
            .interval(),
            max
        );
        assert_eq!(
            MpmController::new(
                Duration::from_millis(1),
                Duration::from_millis(2000),
                max,
                100,
            )
            .interval(),
            Duration::from_millis(2000)
        );
    }

    #[test]
    fn error_backoff_increases_and_caps() {
        assert_eq!(error_backoff(0), Duration::from_secs(5));
        assert_eq!(error_backoff(1), Duration::from_secs(10));
        assert_eq!(error_backoff(2), Duration::from_secs(20));
        assert_eq!(error_backoff(3), Duration::from_secs(40));
        assert_eq!(error_backoff(4), Duration::from_secs(60));
        assert_eq!(error_backoff(10), Duration::from_secs(60));
    }

    #[test]
    fn one_match_batches_use_a_safe_minimum() {
        let controller = MpmController::new(
            Duration::from_millis(5000),
            Duration::from_millis(2000),
            Duration::from_secs(30),
            1,
        );
        assert_eq!(controller.interval(), Duration::from_millis(5000));
    }
}
