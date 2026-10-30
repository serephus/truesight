use crate::ch::ClickHouse;
use crate::config::CollectorConfig;
use crate::model::{Accumulator, Match, StateRow};
use crate::steam::{Head, SteamClient, Throttle};
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

const HEAD_REFRESH: Duration = Duration::from_secs(600);
const HEAD_EXTRAPOLATION_LIMIT: Duration = Duration::from_secs(30 * 60);
const HEAD_RATE_SMOOTHING_SECS: f64 = 300.0;
const RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(30);
const RATE_LIMIT_RETRY: Duration = Duration::from_secs(10);
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(300);
const RATE_PROBE_FACTOR: f64 = 1.05;
const RATE_LIMIT_SAFE_FRACTION: f64 = 0.80;
const RATE_CAP_RECOVERY_DELAY: Duration = Duration::from_secs(300);
const RATE_CAP_RECOVERY_FAST_PERIOD: Duration = Duration::from_secs(60);
const RATE_CAP_RECOVERY_SLOW_PERIOD: Duration = Duration::from_secs(120);
const RATE_CAP_RECOVERY_FACTOR: f64 = 1.02;
const RATE_CAP_RECOVERY_FAST_FRACTION: f64 = 0.90;
const CATCH_UP_HORIZON: Duration = Duration::from_secs(2 * 60 * 60);
const MIN_ERROR_BACKOFF: Duration = Duration::from_secs(5);
const MAX_ERROR_BACKOFF: Duration = Duration::from_secs(30);
const DEFAULT_STREAM_RATE_MPS: f64 = 1000.0 / 60.0;

/// Discovers a request-rate ceiling that does not trigger Valve 429s.
///
/// The rate is in requests per second. Global 429 evidence halves the current
/// rate and lowers a durable cap; while behind and outside the cooldown the
/// rate probes upward toward a lag-aware target. The interval safety clamps
/// define the absolute floor/ceiling.
#[derive(Debug, Clone)]
struct RateGovernor {
    rate: f64,
    min_rate: f64,
    max_rate: f64,
    absolute_max_rate: f64,
    last_rate_limit: Option<Instant>,
    last_throttled_rate: Option<f64>,
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
            last_throttled_rate: None,
            last_cap_recovery: None,
        }
    }

    fn rate(&self) -> f64 {
        self.rate
    }

    fn rate_cap(&self) -> f64 {
        self.max_rate
    }

    /// Only call this on global/repeated 429 evidence. A single per-key 429 is
    /// handled by the key pool and must not slow every other key down.
    fn on_rate_limit(&mut self, now: Instant) {
        // A 429 while running well below the cap is usually bursty retries or
        // a per-key quota, not evidence that the sustainable ceiling is low.
        // Only cut the durable cap when we were actually using it.
        let at_cap = self.rate >= self.max_rate * 0.85;
        if at_cap {
            self.last_throttled_rate = Some(self.rate);
            let cap = (self.rate * RATE_LIMIT_SAFE_FRACTION).max(self.min_rate);
            self.max_rate = self.max_rate.min(cap);
        }
        self.rate = (self.rate * 0.5).max(self.min_rate).min(self.max_rate);
        self.last_rate_limit = Some(now);
        self.last_cap_recovery = Some(now);
    }

    fn on_success(&mut self, behind: bool, target: f64, now: Instant) {
        self.recover_cap(now);

        let target = target.clamp(self.min_rate, self.max_rate);
        let cooling_down = self
            .last_rate_limit
            .map(|last| now.saturating_duration_since(last) < RATE_LIMIT_COOLDOWN)
            .unwrap_or(false);
        if cooling_down || !behind {
            // Never jump back up during the global 429 cooldown, and don't
            // raise the ceiling while already caught up.
            self.rate = self.rate.min(target);
            return;
        }
        if self.rate < target {
            self.rate = (self.rate * RATE_PROBE_FACTOR)
                .min(target)
                .min(self.max_rate);
        } else if self.rate > target {
            // The backlog no longer justifies extra pressure; settle down.
            self.rate = target;
        }
    }

    /// Request rate needed to keep up with the stream and drain the backlog
    /// over [`CATCH_UP_HORIZON`]. The durable cap remains the hard ceiling.
    fn target_rate(&self, stream_mps: f64, lag_matches: u64, batch_size: u32) -> f64 {
        let batch = batch_size.max(1) as f64;
        let catch_up = lag_matches as f64 / CATCH_UP_HORIZON.as_secs_f64();
        ((stream_mps.max(0.0) + catch_up) / batch).clamp(self.min_rate, self.max_rate)
    }

    fn recover_cap(&mut self, now: Instant) {
        let Some(last) = self.last_rate_limit else {
            return;
        };
        if now.saturating_duration_since(last) < RATE_CAP_RECOVERY_DELAY
            || self.max_rate >= self.absolute_max_rate
        {
            return;
        }

        // Recover quickly up to 90% of the rate that last triggered a global
        // limit, then slowly probe beyond it.
        let fast_ceiling = self
            .last_throttled_rate
            .map(|rate| rate * RATE_CAP_RECOVERY_FAST_FRACTION);
        let period = if fast_ceiling
            .map(|ceiling| self.max_rate < ceiling)
            .unwrap_or(false)
        {
            RATE_CAP_RECOVERY_FAST_PERIOD
        } else {
            RATE_CAP_RECOVERY_SLOW_PERIOD
        };
        if self
            .last_cap_recovery
            .map(|last| now.saturating_duration_since(last) >= period)
            .unwrap_or(true)
        {
            self.max_rate = (self.max_rate * RATE_CAP_RECOVERY_FACTOR).min(self.absolute_max_rate);
            self.last_cap_recovery = Some(now);
        }
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
                let weight = (elapsed / (elapsed + HEAD_RATE_SMOOTHING_SECS)).clamp(0.1, 0.9);
                self.rate_mps = (1.0 - weight) * self.rate_mps + weight * instant_rate;
            }
        }
        self.seq = Some(self.seq.map_or(head.seq, |previous| previous.max(head.seq)));
        self.sampled_at = Some(now);
    }

    /// Extrapolate the head between refreshes so the lag estimate does not go
    /// stale during the 10-minute sampling interval.
    fn estimated_seq(&self, now: Instant) -> Option<u64> {
        let seq = self.seq?;
        let sampled_at = self.sampled_at?;
        let elapsed = now.saturating_duration_since(sampled_at);
        if elapsed > HEAD_EXTRAPOLATION_LIMIT {
            // The head sample is too old to trust; report unknown so the
            // collector errs on the side of catching up.
            return None;
        }
        Some(seq.saturating_add((elapsed.as_secs_f64() * self.rate_mps) as u64))
    }

    /// Snap the head to a known-good sequence. Used when the history stream
    /// returns repeated empty batches, which is stronger evidence than a stale
    /// extrapolated head.
    fn anchor(&mut self, seq: u64, now: Instant) {
        self.seq = Some(seq);
        self.sampled_at = Some(now);
    }

    fn lag_matches(&self, cursor: u64, now: Instant) -> Option<u64> {
        self.estimated_seq(now)
            .map(|seq| seq.saturating_sub(cursor))
    }

    fn lag_seconds(&self, cursor: u64, now: Instant) -> Option<f64> {
        let lag = self.lag_matches(cursor, now)? as f64;
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

/// Tracks 429s across calls so repeated pressure on different keys can be
/// distinguished from a single key hitting its own quota.
#[derive(Debug)]
struct ThrottleTracker {
    calls: Vec<(Instant, Vec<usize>)>,
    last_global_limit: Option<Instant>,
    key_count: usize,
}

impl ThrottleTracker {
    fn new(key_count: usize) -> Self {
        Self {
            calls: Vec::new(),
            last_global_limit: None,
            key_count,
        }
    }

    /// A single logical call is one piece of evidence, even if its internal
    /// retries happen to hit multiple keys. Global evidence requires repeated
    /// calls across the window, not bursty retries inside one call.
    fn observe(&mut self, throttle: &Throttle, now: Instant) -> bool {
        if !throttle.is_limited() {
            return false;
        }
        self.calls.push((now, throttle.keys().to_vec()));
        self.calls
            .retain(|(at, _)| now.saturating_duration_since(*at) <= RATE_LIMIT_WINDOW);

        let mut keys: Vec<usize> = self
            .calls
            .iter()
            .flat_map(|(_, keys)| keys.iter().copied())
            .collect();
        keys.sort_unstable();
        keys.dedup();

        let calls = self.calls.len();
        let repeated = calls >= 2 && keys.len() >= 2;
        let all_keys = calls >= 2 && self.key_count > 0 && keys.len() >= self.key_count;
        let allowed = self
            .last_global_limit
            .map(|last| now.saturating_duration_since(last) >= RATE_LIMIT_WINDOW)
            .unwrap_or(true);

        if (all_keys || repeated) && allowed {
            self.last_global_limit = Some(now);
            true
        } else {
            false
        }
    }
}

fn error_backoff(consecutive_errors: u32) -> Duration {
    let seconds = MIN_ERROR_BACKOFF
        .as_secs()
        .saturating_mul(1u64 << consecutive_errors.min(4));
    Duration::from_secs(seconds.min(MAX_ERROR_BACKOFF.as_secs()))
}

/// Applies 429 evidence from one Steam call.
///
/// Global/repeated evidence lowers the governor cap. A single key being
/// throttled while another key completes the call does not slow the whole
/// collector; only a failed call gets a short retry delay in that case.
fn assess_throttle(
    throttle: &Throttle,
    succeeded: bool,
    tracker: &mut ThrottleTracker,
    governor: &mut RateGovernor,
    gate: &mut RequestGate,
    now: Instant,
) -> bool {
    if !throttle.is_limited() {
        return false;
    }
    let global = tracker.observe(throttle, now);
    if global {
        governor.on_rate_limit(now);
    }
    if !succeeded {
        gate.defer(if global {
            RATE_LIMIT_COOLDOWN
        } else {
            RATE_LIMIT_RETRY
        });
    }
    global
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
                            warn!(error = %format!("{error:#}"), "failed to determine startup sequence, retrying");
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
    let mut consecutive_empty = 0u32;
    let mut throttle_tracker = ThrottleTracker::new(steam.key_count());

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
            let response = steam.fetch_head().await;
            let now = Instant::now();
            let limited = response.throttle.is_limited();
            let global = assess_throttle(
                &response.throttle,
                response.result.is_ok(),
                &mut throttle_tracker,
                &mut governor,
                &mut gate,
                now,
            );
            if limited && !global {
                debug!(
                    keys = ?response.throttle.keys(),
                    "per-key rate limit on head request; continuing"
                );
            }
            match response.result {
                Ok(update) => {
                    head.update(update, now);
                    consecutive_empty = 0;
                    debug!(
                        head_seq = update.seq,
                        head_start_time = update.start_time,
                        head_rate_mps = head.rate_mps(),
                        lag_matches = head.lag_matches(cursor, now).unwrap_or(0),
                        request_rate = governor.rate(),
                        "refreshed steam head"
                    );
                }
                Err(error) => {
                    warn!(
                        error = %format!("{error:#}"),
                        limited,
                        global,
                        last_known_seq = head.seq,
                        "failed to refresh steam head; keeping the last known sequence"
                    );
                }
            }
            last_head_refresh = Some(now);
        }

        let now = Instant::now();
        let lag_matches = head.lag_matches(cursor, now);
        let lag_seconds = head.lag_seconds(cursor, now);
        let mut behind = lag_matches.map(|lag| lag > 0).unwrap_or(true);
        let mut target_rate = match lag_matches {
            Some(lag) => governor.target_rate(head.rate_mps(), lag, cfg.batch_size),
            None => governor.rate_cap(),
        };
        let effective_rate = governor.rate().min(target_rate);
        gate.set_rate(effective_rate);

        if !gate.wait(&ct).await {
            break;
        }

        let response = steam
            .fetch_by_sequence(cursor.saturating_add(1), cfg.batch_size)
            .await;
        let now = Instant::now();
        let limited = response.throttle.is_limited();
        let global = assess_throttle(
            &response.throttle,
            response.result.is_ok(),
            &mut throttle_tracker,
            &mut governor,
            &mut gate,
            now,
        );
        if limited && !global {
            debug!(
                keys = ?response.throttle.keys(),
                "per-key rate limit on history request; continuing"
            );
        }

        match response.result {
            Ok(matches) if matches.is_empty() => {
                consecutive_errors = 0;
                if behind {
                    // A stale/extrapolated head can make us look behind while
                    // the stream is already caught up. Two empty batches are
                    // stronger evidence than the estimate, so re-anchor the
                    // head and stop polling at catch-up speed.
                    consecutive_empty = consecutive_empty.saturating_add(1);
                    if consecutive_empty >= 2 {
                        head.anchor(cursor, now);
                        consecutive_empty = 0;
                        behind = false;
                        target_rate = governor.target_rate(head.rate_mps(), 0, cfg.batch_size);
                        debug!(
                            cursor,
                            "history stream is empty; re-anchored head at cursor"
                        );
                    }
                    gate.defer(MIN_ERROR_BACKOFF);
                } else {
                    consecutive_empty = 0;
                }
                governor.on_success(behind, target_rate, now);
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
                            error = %format!("{error:#}"),
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
                        error = %format!("{error:#}"),
                        cursor,
                        lag_matches = lag_matches.unwrap_or(0),
                        "failed to save collector checkpoint, will retry"
                    );
                    gate.defer(error_backoff(consecutive_errors));
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    continue;
                }

                consecutive_errors = 0;
                consecutive_empty = 0;
                let lag_after = head.lag_matches(cursor, now);
                let behind_after = lag_after.map(|lag| lag > 0).unwrap_or(true);
                let target_after = match lag_after {
                    Some(lag) => governor.target_rate(head.rate_mps(), lag, cfg.batch_size),
                    None => governor.rate_cap(),
                };
                governor.on_success(behind_after, target_after, now);
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
                    lag_matches = lag_after.unwrap_or(0),
                    lag_secs = head.lag_seconds(cursor, now).unwrap_or(0.0),
                    behind = behind_after,
                    limited,
                    global,
                    "stored sequence batch"
                );
            }
            Err(error) => {
                if limited {
                    warn!(
                        error = %format!("{error:#}"),
                        request_rate = governor.rate(),
                        global,
                        lag_matches = lag_matches.unwrap_or(0),
                        "Steam rate limited the request; backing off"
                    );
                } else {
                    let delay = error_backoff(consecutive_errors);
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    gate.defer(delay);
                    warn!(
                        error = %format!("{error:#}"),
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
            Err(error) => debug!(error = %format!("{error:#}"), "skipping malformed match"),
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

    fn at_cap_governor() -> RateGovernor {
        let mut governor = governor();
        let now = Instant::now();
        for _ in 0..100 {
            governor.on_success(true, 0.5, now);
        }
        assert!((governor.rate() - governor.rate_cap()).abs() < 1e-9);
        governor
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
        let mut governor = at_cap_governor();
        let now = Instant::now();
        governor.on_rate_limit(now);
        let cap = governor.rate_cap();
        assert!(cap < 0.5);
        for _ in 0..100 {
            governor.on_success(
                true,
                0.5,
                now + RATE_LIMIT_COOLDOWN + Duration::from_secs(1),
            );
        }
        assert!(governor.rate() <= cap);
    }

    #[test]
    fn cap_recovers_after_a_quiet_period() {
        let mut governor = at_cap_governor();
        let now = Instant::now();
        governor.on_rate_limit(now);
        let cap = governor.rate_cap();
        governor.on_success(
            false,
            0.0,
            now + RATE_CAP_RECOVERY_DELAY - Duration::from_secs(1),
        );
        assert_eq!(governor.rate_cap(), cap);
        governor.on_success(
            false,
            0.0,
            now + RATE_CAP_RECOVERY_DELAY + Duration::from_secs(1),
        );
        assert!(governor.rate_cap() > cap);
    }

    #[test]
    fn cap_recovery_is_faster_below_the_last_throttled_rate() {
        let mut governor = at_cap_governor();
        let now = Instant::now();
        governor.on_rate_limit(now);
        let cap = governor.rate_cap();
        let fast_until = 0.5 * RATE_CAP_RECOVERY_FAST_FRACTION;
        governor.on_success(false, 0.0, now + RATE_CAP_RECOVERY_DELAY);
        let after_first = governor.rate_cap();
        governor.on_success(
            false,
            0.0,
            now + RATE_CAP_RECOVERY_DELAY + RATE_CAP_RECOVERY_FAST_PERIOD,
        );
        assert!(governor.rate_cap() > after_first);
        assert!(governor.rate_cap() < fast_until);
        assert!(after_first > cap);
    }

    #[test]
    fn cap_recovery_is_bounded_by_absolute_max() {
        let mut governor = at_cap_governor();
        let now = Instant::now();
        governor.on_rate_limit(now);
        for index in 1..=2000 {
            governor.on_success(
                false,
                0.0,
                now + RATE_CAP_RECOVERY_DELAY + RATE_CAP_RECOVERY_FAST_PERIOD * index,
            );
        }
        assert!((governor.rate_cap() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn rate_only_ramps_after_cooldown() {
        let mut governor = governor();
        let now = Instant::now();
        governor.on_rate_limit(now);
        governor.on_success(true, 0.5, now + Duration::from_secs(1));
        assert!((governor.rate() - 0.1).abs() < 1e-9);
        governor.on_success(
            true,
            0.5,
            now + RATE_LIMIT_COOLDOWN + Duration::from_secs(1),
        );
        assert!(governor.rate() > 0.1);
    }

    #[test]
    fn rate_settles_to_a_lag_aware_target() {
        let mut governor = governor();
        let now = Instant::now();
        governor.on_success(true, 0.1, now);
        assert!((governor.rate() - 0.1).abs() < 1e-9);
    }

    #[test]
    fn rate_does_not_ramp_when_not_behind() {
        let mut governor = governor();
        let now = Instant::now();
        let before = governor.rate();
        governor.on_success(false, before, now + Duration::from_secs(60));
        assert_eq!(governor.rate(), before);
    }

    #[test]
    fn target_rate_grows_with_backlog_and_respects_cap() {
        let governor = governor();
        let idle = governor.target_rate(10.0, 0, 100);
        let small = governor.target_rate(10.0, 10_000, 100);
        let huge = governor.target_rate(10.0, 10_000_000, 100);
        assert!((idle - 0.1).abs() < 1e-9);
        assert!(small > idle);
        assert!((huge - governor.rate_cap()).abs() < 1e-9);
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
        assert_eq!(head.lag_matches(500, now), Some(1100));

        let later = now + Duration::from_secs(60);
        head.update(
            Head {
                seq: 2200,
                start_time: 1_800_000_100,
            },
            later,
        );
        assert_eq!(head.lag_matches(500, later), Some(1700));
        assert!(head.rate_mps() > 0.0);
        assert!(head.lag_seconds(500, later).unwrap() > 0.0);
    }

    #[test]
    fn head_extrapolates_between_sparse_refreshes() {
        let mut head = HeadState::new(Some(1000));
        let now = Instant::now();
        head.update(
            Head {
                seq: 1600,
                start_time: 1_800_000_000,
            },
            now,
        );
        head.update(
            Head {
                seq: 2200,
                start_time: 1_800_000_100,
            },
            now + Duration::from_secs(60),
        );
        let fresh = head
            .lag_matches(500, now + Duration::from_secs(60))
            .unwrap();
        let later = head
            .lag_matches(500, now + Duration::from_secs(120))
            .unwrap();
        assert!(later > fresh);
    }

    #[test]
    fn stale_head_reports_unknown_lag() {
        let mut head = HeadState::new(Some(1000));
        let now = Instant::now();
        head.update(
            Head {
                seq: 1600,
                start_time: 1_800_000_000,
            },
            now,
        );
        assert_eq!(
            head.lag_matches(500, now + HEAD_EXTRAPOLATION_LIMIT + Duration::from_secs(1)),
            None
        );
    }

    #[test]
    fn head_without_sequence_reports_unknown_lag() {
        let head = HeadState::new(None);
        let now = Instant::now();
        assert_eq!(head.lag_matches(100, now), None);
        assert_eq!(head.lag_seconds(100, now), None);
    }

    fn throttle(keys: &[usize]) -> Throttle {
        let mut throttle = Throttle::default();
        for key in keys {
            throttle.record(*key);
        }
        throttle
    }

    #[test]
    fn single_key_throttle_is_not_global_evidence() {
        let mut tracker = ThrottleTracker::new(3);
        let now = Instant::now();
        assert!(!tracker.observe(&throttle(&[0]), now));
    }

    #[test]
    fn repeated_throttle_across_keys_is_global_evidence() {
        let mut tracker = ThrottleTracker::new(3);
        let now = Instant::now();
        assert!(!tracker.observe(&throttle(&[0]), now));
        assert!(tracker.observe(&throttle(&[1]), now + Duration::from_secs(30)));
    }

    #[test]
    fn all_keys_throttled_in_one_call_is_not_global_evidence() {
        let mut tracker = ThrottleTracker::new(3);
        assert!(!tracker.observe(&throttle(&[0, 1, 2]), Instant::now()));
        assert!(tracker.observe(&throttle(&[0]), Instant::now() + Duration::from_secs(1)));
    }

    #[test]
    fn global_evidence_does_not_retrigger_inside_the_window() {
        let mut tracker = ThrottleTracker::new(2);
        let now = Instant::now();
        assert!(!tracker.observe(&throttle(&[0]), now));
        assert!(tracker.observe(&throttle(&[1]), now + Duration::from_secs(30)));
        assert!(!tracker.observe(&throttle(&[0, 1]), now + Duration::from_secs(60)));
        assert!(tracker.observe(
            &throttle(&[0, 1]),
            now + Duration::from_secs(31) + RATE_LIMIT_WINDOW
        ));
    }

    #[test]
    fn per_key_throttle_does_not_slow_the_governor_or_gate() {
        let mut governor = governor();
        let mut gate = RequestGate::new(governor.rate());
        let mut tracker = ThrottleTracker::new(3);
        let now = Instant::now();
        let before_rate = governor.rate();
        let before_cap = governor.rate_cap();
        let before_next = gate.next;

        let global = assess_throttle(
            &throttle(&[0]),
            true,
            &mut tracker,
            &mut governor,
            &mut gate,
            now,
        );

        assert!(!global);
        assert_eq!(governor.rate(), before_rate);
        assert_eq!(governor.rate_cap(), before_cap);
        assert_eq!(gate.next, before_next);
    }

    #[test]
    fn global_throttle_cuts_the_governor_and_defers_a_failed_call() {
        let mut governor = at_cap_governor();
        let mut gate = RequestGate::new(governor.rate());
        let mut tracker = ThrottleTracker::new(3);
        let now = Instant::now();

        assert!(!assess_throttle(
            &throttle(&[0]),
            true,
            &mut tracker,
            &mut governor,
            &mut gate,
            now,
        ));

        let before_rate = governor.rate();
        let before_cap = governor.rate_cap();
        let before_next = gate.next;
        let global = assess_throttle(
            &throttle(&[1]),
            false,
            &mut tracker,
            &mut governor,
            &mut gate,
            now + Duration::from_secs(1),
        );

        assert!(global);
        assert!(governor.rate() < before_rate);
        assert!(governor.rate_cap() < before_cap);
        assert!(gate.next > before_next);
    }

    #[test]
    fn error_backoff_increases_and_caps() {
        assert_eq!(error_backoff(0), Duration::from_secs(5));
        assert_eq!(error_backoff(1), Duration::from_secs(10));
        assert_eq!(error_backoff(2), Duration::from_secs(20));
        assert_eq!(error_backoff(3), Duration::from_secs(30));
        assert_eq!(error_backoff(4), Duration::from_secs(30));
        assert_eq!(error_backoff(10), Duration::from_secs(30));
    }
}
