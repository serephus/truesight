use crate::ch::ClickHouse;
use crate::config::CollectorConfig;
use crate::model::{Accumulator, Match, StateRow};
use crate::steam::{Head, SteamClient};
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

const HEAD_REFRESH: Duration = Duration::from_secs(30);
const RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(30);
const RATE_PROBE_FACTOR: f64 = 1.05;
const RATE_LIMIT_SAFE_FRACTION: f64 = 0.80;
const RATE_CAP_RECOVERY_PERIOD: Duration = Duration::from_secs(300);
const RATE_CAP_RECOVERY_FACTOR: f64 = 1.01;
const MIN_ERROR_BACKOFF: Duration = Duration::from_secs(5);
const MAX_ERROR_BACKOFF: Duration = Duration::from_secs(60);
const DEFAULT_STREAM_RATE_MPS: f64 = 1000.0 / 60.0;

/// Discovers a request-rate ceiling that does not trigger Valve 429s.
///
/// The rate is in requests per second. On a 429 it halves and cools down; while
/// behind and outside the cooldown it probes upward by 5%. The interval safety
/// clamps define the absolute floor/ceiling.
#[derive(Debug, Clone)]
struct RateGovernor {
    rate: f64,
    min_rate: f64,
    max_rate: f64,
    absolute_max_rate: f64,
    last_rate_limit: Option<Instant>,
    last_cap_recovery: Option<Instant>,
}

impl RateGovernor {
    fn new(initial_interval: Duration, min_interval: Duration, max_interval: Duration) -> Self {
        let min_rate = 1.0 / max_interval.as_secs_f64().max(0.001);
        let max_rate = 1.0 / min_interval.as_secs_f64().max(0.001);
        let requested = 1.0 / initial_interval.as_secs_f64().max(0.001);
        Self {
            rate: requested.clamp(min_rate, max_rate),
            min_rate,
            max_rate,
            absolute_max_rate: max_rate,
            last_rate_limit: None,
            last_cap_recovery: None,
        }
    }

    fn rate(&self) -> f64 {
        self.rate
    }

    fn rate_cap(&self) -> f64 {
        self.max_rate
    }

    fn min_rate(&self) -> f64 {
        self.min_rate
    }

    fn on_rate_limit(&mut self, now: Instant) {
        let cap = (self.rate * RATE_LIMIT_SAFE_FRACTION).max(self.min_rate);
        self.max_rate = self.max_rate.min(cap);
        self.rate = (self.rate * 0.5).max(self.min_rate).min(self.max_rate);
        self.last_rate_limit = Some(now);
        self.last_cap_recovery = Some(now);
    }

    fn on_success(&mut self, behind: bool, now: Instant) {
        if let Some(last) = self.last_rate_limit
            && now.saturating_duration_since(last) >= RATE_CAP_RECOVERY_PERIOD
            && self
                .last_cap_recovery
                .map(|last| now.saturating_duration_since(last) >= RATE_CAP_RECOVERY_PERIOD)
                .unwrap_or(true)
            && self.max_rate < self.absolute_max_rate
        {
            self.max_rate = (self.max_rate * RATE_CAP_RECOVERY_FACTOR).min(self.absolute_max_rate);
            self.last_cap_recovery = Some(now);
        }

        if !behind {
            return;
        }
        if let Some(last) = self.last_rate_limit
            && now.saturating_duration_since(last) < RATE_LIMIT_COOLDOWN
        {
            return;
        }
        self.rate = (self.rate * RATE_PROBE_FACTOR).min(self.max_rate);
    }

    fn mpm(&self, batch_size: u32) -> f64 {
        self.rate * batch_size as f64 * 60.0
    }
}

/// Serializes all Steam requests at the current rate and supports cooldowns.
#[derive(Debug)]
struct RequestGate {
    next: Instant,
    rate: f64,
}

impl RequestGate {
    fn new(rate: f64) -> Self {
        Self {
            next: Instant::now(),
            rate: rate.max(0.0001),
        }
    }

    fn set_rate(&mut self, rate: f64) {
        self.rate = rate.max(0.0001);
    }

    fn defer(&mut self, delay: Duration) {
        self.next = self.next.max(Instant::now() + delay);
    }

    async fn wait(&mut self, ct: &CancellationToken) -> bool {
        let now = Instant::now();
        if self.next > now && !sleep_until_or_cancel(ct, self.next).await {
            return false;
        }
        let now = Instant::now();
        self.next = now + Duration::from_secs_f64(1.0 / self.rate);
        true
    }
}

#[derive(Debug, Clone)]
struct HeadState {
    seq: Option<u64>,
    rate_mps: f64,
    sampled_at: Option<Instant>,
}

impl HeadState {
    fn new(seq: Option<u64>) -> Self {
        Self {
            seq,
            rate_mps: DEFAULT_STREAM_RATE_MPS,
            sampled_at: None,
        }
    }

    fn update(&mut self, head: Head, now: Instant) {
        if let Some(previous_seq) = self.seq
            && let Some(previous_at) = self.sampled_at
        {
            let elapsed = now.saturating_duration_since(previous_at).as_secs_f64();
            if elapsed > 1.0 && head.seq >= previous_seq {
                let instant_rate = (head.seq - previous_seq) as f64 / elapsed;
                self.rate_mps = 0.7 * self.rate_mps + 0.3 * instant_rate;
            }
        }
        self.seq = Some(self.seq.map_or(head.seq, |previous| previous.max(head.seq)));
        self.sampled_at = Some(now);
    }

    fn lag_matches(&self, cursor: u64) -> Option<u64> {
        self.seq.map(|seq| seq.saturating_sub(cursor))
    }

    fn lag_seconds(&self, cursor: u64) -> Option<f64> {
        let lag = self.lag_matches(cursor)? as f64;
        if self.rate_mps > 0.0 {
            Some(lag / self.rate_mps)
        } else {
            None
        }
    }

    fn rate_mps(&self) -> f64 {
        self.rate_mps
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

    let mut governor = RateGovernor::new(
        cfg.poll_interval,
        cfg.min_poll_interval,
        cfg.max_poll_interval,
    );
    let mut gate = RequestGate::new(governor.rate());
    let mut head = HeadState::new(Some(cursor));
    let mut last_head_refresh: Option<Instant> = None;
    let mut consecutive_errors = 0u32;

    info!(
        cursor,
        request_rate = governor.rate(),
        rate_cap = governor.rate_cap(),
        mpm = governor.mpm(cfg.batch_size),
        "collector rate initialized"
    );

    loop {
        if ct.is_cancelled() {
            break;
        }

        if last_head_refresh
            .map(|last| last.elapsed() >= HEAD_REFRESH)
            .unwrap_or(true)
        {
            if !gate.wait(&ct).await {
                break;
            }
            let before = steam.rate_limit_events();
            let result = steam.fetch_head().await;
            let limited = steam.rate_limit_events() > before;
            let now = Instant::now();
            if limited {
                governor.on_rate_limit(now);
                gate.defer(RATE_LIMIT_COOLDOWN);
            }
            match result {
                Ok(update) => {
                    head.update(update, now);
                    debug!(
                        head_seq = update.seq,
                        head_start_time = update.start_time,
                        head_rate_mps = head.rate_mps(),
                        lag_matches = head.lag_matches(cursor).unwrap_or(0),
                        request_rate = governor.rate(),
                        "refreshed steam head"
                    );
                }
                Err(error) => {
                    head.seq = None;
                    warn!(%error, limited, "failed to refresh steam head");
                }
            }
            last_head_refresh = Some(now);
        }

        let lag_matches = head.lag_matches(cursor);
        let lag_seconds = head.lag_seconds(cursor);
        let behind = lag_matches.map(|lag| lag > 0).unwrap_or(true);
        let effective_rate = if behind {
            governor.rate()
        } else {
            let idle = head.rate_mps() / cfg.batch_size.max(1) as f64;
            idle.clamp(governor.min_rate(), governor.rate())
        };
        gate.set_rate(effective_rate);

        if !gate.wait(&ct).await {
            break;
        }

        let before = steam.rate_limit_events();
        let result = steam
            .fetch_by_sequence(cursor.saturating_add(1), cfg.batch_size)
            .await;
        let limited = steam.rate_limit_events() > before;
        let now = Instant::now();
        if limited {
            governor.on_rate_limit(now);
            gate.defer(RATE_LIMIT_COOLDOWN);
        }

        match result {
            Ok(matches) if matches.is_empty() => {
                consecutive_errors = 0;
                governor.on_success(false, now);
                debug!(
                    cursor,
                    fetched = 0,
                    request_rate = effective_rate,
                    mpm = effective_rate * cfg.batch_size as f64 * 60.0,
                    lag_matches = lag_matches.unwrap_or(0),
                    lag_secs = lag_seconds.unwrap_or(0.0),
                    behind,
                    limited,
                    "no new matches from the steam sequence stream"
                );
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
                        lag_matches = lag_matches.unwrap_or(0),
                        "Steam returned a non-advancing sequence batch; backing off"
                    );
                    gate.defer(error_backoff(consecutive_errors));
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    continue;
                }

                let token = format!("{}-{}", cursor.saturating_add(1), batch_end);
                let (stored, rated) = match store_batch(&ch, &matches, &token).await {
                    Ok(result) => result,
                    Err(error) => {
                        warn!(
                            %error,
                            token,
                            lag_matches = lag_matches.unwrap_or(0),
                            "failed to store batch, will retry"
                        );
                        gate.defer(error_backoff(consecutive_errors));
                        consecutive_errors = consecutive_errors.saturating_add(1);
                        continue;
                    }
                };

                cursor = batch_end.max(cursor);
                if let Err(error) = save_cursor(&ch, cursor).await {
                    warn!(
                        %error,
                        cursor,
                        lag_matches = lag_matches.unwrap_or(0),
                        "failed to save collector checkpoint, will retry"
                    );
                    gate.defer(error_backoff(consecutive_errors));
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    continue;
                }

                consecutive_errors = 0;
                let behind_after = head.seq.map(|seq| seq > cursor).unwrap_or(true);
                governor.on_success(behind_after, now);
                debug!(
                    cursor,
                    fetched = matches.len(),
                    stored,
                    rated,
                    request_rate = effective_rate,
                    ceiling_rate = governor.rate(),
                    rate_cap = governor.rate_cap(),
                    mpm = effective_rate * cfg.batch_size as f64 * 60.0,
                    ceiling_mpm = governor.mpm(cfg.batch_size),
                    lag_matches = head.lag_matches(cursor).unwrap_or(0),
                    lag_secs = head.lag_seconds(cursor).unwrap_or(0.0),
                    behind = behind_after,
                    limited,
                    "stored sequence batch"
                );
            }
            Err(error) => {
                if limited {
                    warn!(
                        %error,
                        request_rate = governor.rate(),
                        lag_matches = lag_matches.unwrap_or(0),
                        "Steam rate limited the request; backing off"
                    );
                } else {
                    let delay = error_backoff(consecutive_errors);
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    gate.defer(delay);
                    warn!(
                        %error,
                        delay_ms = delay.as_millis(),
                        lag_matches = lag_matches.unwrap_or(0),
                        "Steam request failed; backing off"
                    );
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

    fn governor() -> RateGovernor {
        RateGovernor::new(
            Duration::from_secs(5),
            Duration::from_secs(2),
            Duration::from_secs(30),
        )
    }

    #[test]
    fn initial_rate_is_derived_from_interval() {
        assert!((governor().rate() - 0.2).abs() < 1e-9);
    }

    #[test]
    fn rate_limit_halves_the_rate() {
        let mut governor = governor();
        governor.on_rate_limit(Instant::now());
        assert!((governor.rate() - 0.1).abs() < 1e-9);
    }

    #[test]
    fn rate_limit_sets_a_durable_cap() {
        let mut governor = governor();
        let now = Instant::now();
        governor.on_rate_limit(now);
        let cap = governor.rate_cap();
        assert!(cap < 0.2);
        for _ in 0..100 {
            governor.on_success(true, now + RATE_LIMIT_COOLDOWN + Duration::from_secs(1));
        }
        assert!(governor.rate() <= cap);
    }

    #[test]
    fn cap_recovers_after_a_quiet_period() {
        let mut governor = governor();
        let now = Instant::now();
        governor.on_rate_limit(now);
        let cap = governor.rate_cap();
        governor.on_success(
            false,
            now + RATE_CAP_RECOVERY_PERIOD - Duration::from_secs(1),
        );
        assert_eq!(governor.rate_cap(), cap);
        governor.on_success(
            false,
            now + RATE_CAP_RECOVERY_PERIOD + Duration::from_secs(1),
        );
        assert!(governor.rate_cap() > cap);
    }

    #[test]
    fn cap_recovery_is_bounded_by_absolute_max() {
        let mut governor = governor();
        let now = Instant::now();
        governor.on_rate_limit(now);
        for index in 1..=1000 {
            governor.on_success(false, now + RATE_CAP_RECOVERY_PERIOD * index);
        }
        assert!((governor.rate_cap() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn rate_only_ramps_after_cooldown() {
        let mut governor = governor();
        let now = Instant::now();
        governor.on_rate_limit(now);
        governor.on_success(true, now + Duration::from_secs(1));
        assert!((governor.rate() - 0.1).abs() < 1e-9);
        governor.on_success(true, now + RATE_LIMIT_COOLDOWN + Duration::from_secs(1));
        assert!(governor.rate() > 0.1);
    }

    #[test]
    fn rate_does_not_ramp_when_not_behind() {
        let mut governor = governor();
        let now = Instant::now();
        let before = governor.rate();
        governor.on_success(false, now + Duration::from_secs(60));
        assert_eq!(governor.rate(), before);
    }

    #[test]
    fn rate_respects_interval_clamps() {
        let slow = RateGovernor::new(
            Duration::from_secs(100),
            Duration::from_secs(2),
            Duration::from_secs(30),
        );
        assert!((slow.rate() - 1.0 / 30.0).abs() < 1e-9);

        let fast = RateGovernor::new(
            Duration::from_millis(1),
            Duration::from_secs(2),
            Duration::from_secs(30),
        );
        assert!((fast.rate() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn head_state_estimates_match_rate_and_lag() {
        let mut head = HeadState::new(Some(1000));
        let now = Instant::now();
        head.update(
            Head {
                seq: 1600,
                start_time: 1_800_000_000,
            },
            now,
        );
        assert_eq!(head.lag_matches(500), Some(1100));

        head.update(
            Head {
                seq: 2200,
                start_time: 1_800_000_100,
            },
            now + Duration::from_secs(60),
        );
        assert_eq!(head.lag_matches(500), Some(1700));
        assert!(head.rate_mps() > 0.0);
        assert!(head.lag_seconds(500).unwrap() > 0.0);
    }

    #[test]
    fn head_without_sequence_reports_unknown_lag() {
        let head = HeadState::new(None);
        assert_eq!(head.lag_matches(100), None);
        assert_eq!(head.lag_seconds(100), None);
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
}
