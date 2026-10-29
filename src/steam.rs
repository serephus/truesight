use crate::config::SteamConfig;
use crate::model::Hero;
use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

const HISTORY_PATH: &str = "/IDOTA2Match_570/GetMatchHistoryBySequenceNum/v1/";
const LATEST_PATH: &str = "/IDOTA2Match_570/GetMatchHistory/v1/";
const HEROES_PATH: &str = "/IEconDOTA2_570/GetHeroes/v1/";
const TRANSIENT_KEY_COOLDOWN: Duration = Duration::from_secs(2);
const AUTH_KEY_COOLDOWN: Duration = Duration::from_secs(3600);

#[derive(Clone)]
pub struct SteamClient {
    http: Client,
    base_url: String,
    keys: Arc<Mutex<KeyPool>>,
}

#[derive(Debug, Clone, Copy)]
pub struct Head {
    pub seq: u64,
    pub start_time: u32,
}

/// Rate-limit evidence collected while serving one Steam call.
///
/// A single key returning 429 while another key completes the call is per-key
/// evidence and must not throttle the whole collector. Repeated 429s across
/// keys are a much stronger signal of a global limit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Throttle {
    events: u32,
    keys: Vec<usize>,
}

impl Throttle {
    pub fn is_limited(&self) -> bool {
        self.events > 0
    }

    pub fn keys(&self) -> &[usize] {
        &self.keys
    }

    pub(crate) fn record(&mut self, key: usize) {
        self.events = self.events.saturating_add(1);
        if !self.keys.contains(&key) {
            self.keys.push(key);
        }
    }
}

/// A Steam response paired with any rate-limit evidence seen while fetching it.
pub struct SteamResponse<T> {
    pub result: Result<T>,
    pub throttle: Throttle,
}

struct KeyPool {
    keys: Vec<KeyState>,
    cursor: usize,
}

struct KeyState {
    key: String,
    failures: u32,
    cooldown_until: Option<Instant>,
}

impl SteamClient {
    pub fn new(cfg: &SteamConfig) -> Result<Self> {
        if cfg.keys.is_empty() {
            bail!("SteamClient requires at least one API key");
        }
        let http = Client::builder()
            .timeout(cfg.timeout)
            .user_agent("truesight/0.1 (+https://github.com/serephus/truesight)")
            .build()
            .context("failed to build Steam HTTP client")?;
        Ok(Self {
            http,
            base_url: cfg.base_url.clone(),
            keys: Arc::new(Mutex::new(KeyPool {
                keys: cfg
                    .keys
                    .iter()
                    .map(|key| KeyState {
                        key: key.clone(),
                        failures: 0,
                        cooldown_until: None,
                    })
                    .collect(),
                cursor: 0,
            })),
        })
    }

    /// Fetch up to `matches_requested` full match objects starting at a sequence number.
    pub async fn fetch_by_sequence(
        &self,
        start_seq: u64,
        batch_size: u32,
    ) -> SteamResponse<Vec<Value>> {
        let params = vec![
            ("start_at_match_seq_num", start_seq.to_string()),
            ("matches_requested", batch_size.to_string()),
        ];
        let (value, throttle) = self.get_json(HISTORY_PATH, &params).await;
        let result = value.and_then(|value| {
            let result = api_result(&value)?;
            Ok(result
                .get("matches")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default())
        });
        debug!(
            start_seq,
            limited = throttle.is_limited(),
            "fetched sequence batch"
        );
        SteamResponse { result, throttle }
    }

    /// Fetch the newest public match from GetMatchHistory.
    pub async fn fetch_head(&self) -> SteamResponse<Head> {
        let (value, throttle) = self
            .get_json(LATEST_PATH, &[("matches_requested", "1".to_owned())])
            .await;
        let result = value.and_then(|value| {
            let result = api_result(&value)?;
            let head = result
                .get("matches")
                .and_then(Value::as_array)
                .and_then(|matches| matches.first())
                .context("Steam head response is missing matches")?;
            let seq = head
                .get("match_seq_num")
                .and_then(Value::as_u64)
                .context("Steam head match is missing match_seq_num")?;
            let start_time = head.get("start_time").and_then(Value::as_u64).unwrap_or(0) as u32;
            Ok(Head { seq, start_time })
        });
        SteamResponse { result, throttle }
    }

    /// Best-effort lookup of the newest public match sequence number.
    pub async fn fetch_latest_sequence(&self) -> Result<u64> {
        Ok(self.fetch_head().await.result?.seq)
    }

    pub async fn fetch_heroes(&self) -> Result<Vec<Hero>> {
        let (value, _) = self
            .get_json(HEROES_PATH, &[("language", "en".to_owned())])
            .await;
        let value = value?;
        let result = api_result_any(&value)?;
        let heroes = result
            .get("heroes")
            .and_then(Value::as_array)
            .context("Steam hero response is missing result.heroes")?;
        let mut parsed = Vec::with_capacity(heroes.len());
        for hero in heroes {
            if let Ok(hero) = Hero::from_value(hero) {
                parsed.push(hero);
            }
        }
        Ok(parsed)
    }

    async fn get_json(&self, path: &str, params: &[(&str, String)]) -> (Result<Value>, Throttle) {
        let url = format!("{}{}", self.base_url, path);
        let max_attempts = self.key_count().saturating_mul(2).max(2);
        let mut last_error = None;
        let mut throttle = Throttle::default();

        for attempt in 0..max_attempts {
            let (index, key) = match self.pick_key() {
                Ok(key) => key,
                Err(error) => {
                    last_error = Some(error);
                    break;
                }
            };
            let mut query: Vec<(&str, String)> = params.to_vec();
            query.push(("key", key));
            query.push(("format", "json".to_owned()));

            let response = self.http.get(&url).query(&query).send().await;
            match response {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        match response.text().await {
                            Ok(text) => match serde_json::from_str::<Value>(&text) {
                                Ok(value) => {
                                    if let Some(error) = api_error(&value) {
                                        if is_rate_limit_error(&error) {
                                            self.mark_rate_limited(index, Duration::from_secs(30));
                                            throttle.record(index);
                                        } else if is_auth_error(&error) {
                                            self.mark_auth_failure(index);
                                        } else {
                                            self.mark_transient(index);
                                        }
                                        last_error = Some(anyhow!("Steam API error: {error}"));
                                    } else {
                                        self.mark_success(index);
                                        return (Ok(value), throttle);
                                    }
                                }
                                Err(error) => {
                                    self.mark_transient(index);
                                    last_error = Some(anyhow!(
                                        "Steam returned invalid JSON: {error}: {}",
                                        truncate(&text, 300)
                                    ));
                                }
                            },
                            Err(error) => {
                                self.mark_transient(index);
                                last_error = Some(anyhow!(
                                    "failed to read Steam response body: {}",
                                    error.without_url()
                                ));
                                // Re-trying a stalled body read would multiply the
                                // request timeout; let the collector's error
                                // backoff decide when to try again.
                                break;
                            }
                        }
                    } else {
                        let retry_after = response
                            .headers()
                            .get(reqwest::header::RETRY_AFTER)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok());
                        match status {
                            StatusCode::TOO_MANY_REQUESTS => {
                                self.mark_rate_limited(
                                    index,
                                    Duration::from_secs(retry_after.unwrap_or(30).max(10)),
                                );
                                throttle.record(index);
                            }
                            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                                self.mark_auth_failure(index);
                            }
                            _ => self.mark_transient(index),
                        }
                        warn!(%status, attempt, "Steam request failed");
                        last_error = Some(anyhow!("Steam returned HTTP {status}"));
                    }
                }
                Err(error) => {
                    self.mark_transient(index);
                    last_error = Some(anyhow!("Steam request failed: {}", error.without_url()));
                }
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        (
            Err(last_error.unwrap_or_else(|| anyhow!("Steam request failed"))),
            throttle,
        )
    }

    pub fn key_count(&self) -> usize {
        self.lock().keys.len()
    }

    fn pick_key(&self) -> Result<(usize, String)> {
        let now = Instant::now();
        let mut pool = self.lock();
        let len = pool.keys.len();
        for offset in 0..len {
            let index = (pool.cursor + offset) % len;
            let available = pool.keys[index]
                .cooldown_until
                .map(|until| until <= now)
                .unwrap_or(true);
            if available {
                pool.cursor = (index + 1) % len;
                return Ok((index, pool.keys[index].key.clone()));
            }
        }
        let wait = pool
            .keys
            .iter()
            .filter_map(|key| key.cooldown_until)
            .min()
            .map(|until| until.saturating_duration_since(now))
            .unwrap_or(Duration::from_secs(5));
        bail!("all Steam API keys are cooling down for {wait:?}")
    }

    fn mark_success(&self, index: usize) {
        let mut pool = self.lock();
        if let Some(key) = pool.keys.get_mut(index) {
            key.failures = 0;
            key.cooldown_until = None;
        }
    }

    fn mark_failure(&self, index: usize, cooldown: Duration) {
        let mut pool = self.lock();
        if let Some(key) = pool.keys.get_mut(index) {
            key.failures = key.failures.saturating_add(1);
            let exponential = Duration::from_secs(2u64.saturating_pow(key.failures.min(6)));
            key.cooldown_until = Some(Instant::now() + cooldown.max(exponential));
        }
    }

    fn mark_rate_limited(&self, index: usize, cooldown: Duration) {
        self.mark_failure(index, cooldown);
    }

    fn mark_auth_failure(&self, index: usize) {
        let mut pool = self.lock();
        if let Some(key) = pool.keys.get_mut(index) {
            key.failures = key.failures.saturating_add(1);
            key.cooldown_until = Some(Instant::now() + AUTH_KEY_COOLDOWN);
        }
    }

    /// Network/body-read/invalid-JSON failures are usually transport issues, not
    /// bad keys. Cool the key briefly but do not build up an exponential
    /// penalty that outlives the incident.
    fn mark_transient(&self, index: usize) {
        let mut pool = self.lock();
        if let Some(key) = pool.keys.get_mut(index) {
            let until = Instant::now() + TRANSIENT_KEY_COOLDOWN;
            key.cooldown_until = Some(
                key.cooldown_until
                    .map_or(until, |existing| existing.max(until)),
            );
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, KeyPool> {
        self.keys
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn api_result(value: &Value) -> Result<&Value> {
    let result = value
        .get("result")
        .context("Steam response is missing result")?;
    if let Some(error) = api_error(value) {
        bail!("Steam API error: {error}");
    }
    if let Some(status) = result.get("status").and_then(Value::as_u64)
        && status != 1
    {
        bail!("Steam API returned status {status}");
    }
    Ok(result)
}

/// Some Steam endpoints (notably `GetHeroes`) report `status: 200` on success
/// instead of the match endpoints' `status: 1`.
fn api_result_any(value: &Value) -> Result<&Value> {
    let result = value
        .get("result")
        .context("Steam response is missing result")?;
    if let Some(error) = result
        .get("error")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
    {
        bail!("Steam API error: {error}");
    }
    if let Some(status) = result.get("status").and_then(Value::as_u64)
        && status != 1
        && status != 200
    {
        let detail = result
            .get("statusDetail")
            .and_then(Value::as_str)
            .unwrap_or("");
        if detail.is_empty() {
            bail!("Steam API returned status {status}");
        }
        bail!("Steam API returned status {status}: {detail}");
    }
    Ok(result)
}

fn api_error(value: &Value) -> Option<String> {
    let result = value.get("result")?;
    if let Some(error) = result
        .get("error")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
    {
        return Some(error.to_owned());
    }
    let failed = result
        .get("status")
        .and_then(Value::as_u64)
        .map(|status| status != 1)
        .unwrap_or(false);
    if failed {
        return result
            .get("statusDetail")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .map(ToOwned::to_owned);
    }
    None
}

fn is_rate_limit_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("too many") || message.contains("rate limit")
}

fn is_auth_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("invalid key")
        || message.contains("unauthorized")
        || message.contains("forbidden")
        || message.contains("access is denied")
}

fn truncate(value: &str, max: usize) -> &str {
    if value.len() <= max {
        return value;
    }
    let mut end = max;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn detects_api_errors() {
        let value = json!({"result": {"status": 0, "statusDetail": "invalid key"}});
        assert_eq!(api_error(&value).as_deref(), Some("invalid key"));
        assert!(api_result(&value).is_err());
    }

    #[test]
    fn parses_successful_result() {
        let value = json!({"result": {"status": 1, "matches": []}});
        assert!(api_result(&value).unwrap().get("matches").is_some());
    }

    #[test]
    fn hero_result_accepts_status_200() {
        let value = json!({"result": {"status": 200, "heroes": []}});
        assert!(api_result_any(&value).unwrap().get("heroes").is_some());
    }

    #[test]
    fn hero_result_rejects_failed_status() {
        let value = json!({"result": {"status": 0, "statusDetail": "invalid key"}});
        assert!(api_result_any(&value).is_err());
    }
}
