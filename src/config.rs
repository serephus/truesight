use anyhow::{Context, Result, bail};
use std::{
    env, fmt, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

const DEFAULT_LOG: &str = "info,truesight=debug";

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub log: String,
    pub clickhouse: ClickHouseConfig,
    pub steam: SteamConfig,
    pub collector: CollectorConfig,
    pub ranker: RankerConfig,
    pub raw_retention_days: u32,
    pub snapshot_retention_days: u32,
}

#[derive(Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
    pub timeout: Duration,
}

impl fmt::Debug for ClickHouseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseConfig")
            .field("url", &self.url)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Clone)]
pub struct SteamConfig {
    pub keys: Vec<String>,
    pub base_url: String,
    pub timeout: Duration,
}

impl fmt::Debug for SteamConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SteamConfig")
            .field("keys", &format!("<{} keys>", self.keys.len()))
            .field("base_url", &self.base_url)
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct CollectorConfig {
    pub enabled: bool,
    /// Initial polling interval; used to derive the initial MPM estimate.
    pub poll_interval: Duration,
    /// Safety clamps for the adaptive interval.
    pub min_poll_interval: Duration,
    pub max_poll_interval: Duration,
    pub batch_size: u32,
    pub start_seq: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RankerConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub patch: Option<Patch>,
    pub lambda: f64,
    pub min_games: u64,
}

#[derive(Debug, Clone)]
pub struct Patch {
    pub name: String,
    pub start: u32,
}

impl Config {
    pub fn load() -> Result<Self> {
        let bind = env_string("TRUESIGHT_BIND").unwrap_or_else(|| "127.0.0.1:8080".into());
        let bind: SocketAddr = bind
            .parse()
            .with_context(|| format!("invalid TRUESIGHT_BIND: {bind}"))?;

        let database =
            env_string("TRUESIGHT_CLICKHOUSE_DATABASE").unwrap_or_else(|| "truesight".into());
        validate_identifier(&database)?;

        let clickhouse = ClickHouseConfig {
            url: env_string("TRUESIGHT_CLICKHOUSE_URL")
                .unwrap_or_else(|| "http://127.0.0.1:8123".into())
                .trim_end_matches('/')
                .to_owned(),
            database,
            user: env_string("TRUESIGHT_CLICKHOUSE_USER").unwrap_or_else(|| "default".into()),
            password: read_secret(
                "TRUESIGHT_CLICKHOUSE_PASSWORD",
                "TRUESIGHT_CLICKHOUSE_PASSWORD_FILE",
                "clickhouse_password",
            )?
            .unwrap_or_default(),
            timeout: Duration::from_secs(env_u64("TRUESIGHT_CLICKHOUSE_TIMEOUT_SECS", 60)?),
        };

        let keys = read_keys()?;
        let collector_enabled = env_bool("TRUESIGHT_COLLECTOR_ENABLED", true)?;
        if collector_enabled && keys.is_empty() {
            bail!(
                "no Steam API keys configured; set TRUESIGHT_STEAM_API_KEYS \
                 (comma separated), TRUESIGHT_STEAM_API_KEYS_FILE, or run with \
                 TRUESIGHT_COLLECTOR_ENABLED=false"
            );
        }

        let steam = SteamConfig {
            keys,
            base_url: env_string("TRUESIGHT_STEAM_BASE_URL")
                .unwrap_or_else(|| "https://api.steampowered.com".into())
                .trim_end_matches('/')
                .to_owned(),
            timeout: Duration::from_secs(env_u64("TRUESIGHT_STEAM_TIMEOUT_SECS", 30)?),
        };

        let batch_size = env_u64("TRUESIGHT_BATCH_SIZE", 100)?.clamp(1, 100) as u32;
        let min_poll_interval =
            Duration::from_millis(env_u64("TRUESIGHT_MIN_POLL_INTERVAL_MS", 2000)?)
                .max(Duration::from_millis(1));
        let max_poll_interval =
            Duration::from_millis(env_u64("TRUESIGHT_MAX_POLL_INTERVAL_MS", 30_000)?)
                .max(Duration::from_millis(1));
        if min_poll_interval > max_poll_interval {
            bail!("TRUESIGHT_MIN_POLL_INTERVAL_MS must be <= TRUESIGHT_MAX_POLL_INTERVAL_MS");
        }
        let poll_interval = Duration::from_millis(env_u64("TRUESIGHT_POLL_INTERVAL_MS", 5000)?)
            .max(min_poll_interval)
            .min(max_poll_interval);
        let patch_start = env_string("TRUESIGHT_PATCH_START")
            .map(|v| {
                v.parse::<u32>()
                    .with_context(|| format!("invalid TRUESIGHT_PATCH_START: {v}"))
            })
            .transpose()?
            .filter(|v| *v > 0);
        let patch = patch_start.map(|start| Patch {
            name: env_string("TRUESIGHT_PATCH_NAME").unwrap_or_else(|| "current".into()),
            start,
        });
        let lambda = match env_string("TRUESIGHT_REGULARIZATION") {
            None => 1.0,
            Some(value) => {
                let value: f64 = value
                    .parse()
                    .with_context(|| format!("invalid TRUESIGHT_REGULARIZATION: {value}"))?;
                if !value.is_finite() || value <= 0.0 {
                    bail!("TRUESIGHT_REGULARIZATION must be a finite positive number");
                }
                value
            }
        };
        let raw_retention_days = env_u32("TRUESIGHT_RAW_RETENTION_DAYS", 0)?;
        let snapshot_retention_days = env_u32("TRUESIGHT_SNAPSHOT_RETENTION_DAYS", 180)?;

        Ok(Self {
            bind,
            log: env_string("TRUESIGHT_LOG")
                .or_else(|| env_string("RUST_LOG"))
                .unwrap_or_else(|| DEFAULT_LOG.into()),
            clickhouse,
            steam,
            collector: CollectorConfig {
                enabled: collector_enabled,
                poll_interval,
                min_poll_interval,
                max_poll_interval,
                batch_size,
                start_seq: env_string("TRUESIGHT_START_SEQ")
                    .map(|v| {
                        v.parse::<u64>()
                            .with_context(|| format!("invalid TRUESIGHT_START_SEQ: {v}"))
                    })
                    .transpose()?,
            },
            ranker: RankerConfig {
                enabled: env_bool("TRUESIGHT_RANKER_ENABLED", true)?,
                interval: Duration::from_secs(env_u64("TRUESIGHT_RANK_INTERVAL_SECS", 300)?),
                patch,
                lambda,
                min_games: env_u64("TRUESIGHT_MIN_RATED_GAMES", 1)?,
            },
            raw_retention_days,
            snapshot_retention_days,
        })
    }
}

pub fn validate_identifier(value: &str) -> Result<()> {
    let mut chars = value.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => bail!("invalid ClickHouse database name: {value:?}"),
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        bail!("invalid ClickHouse database name: {value:?}");
    }
    Ok(())
}

pub fn split_keys(raw: &str) -> Vec<String> {
    let mut keys: Vec<String> = raw
        .split(|c: char| c == ',' || c.is_whitespace())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

fn read_keys() -> Result<Vec<String>> {
    let raw = read_secret(
        "TRUESIGHT_STEAM_API_KEYS",
        "TRUESIGHT_STEAM_API_KEYS_FILE",
        "steam_api_keys",
    )?;
    Ok(raw.map(|v| split_keys(&v)).unwrap_or_default())
}

/// Reads a secret from `NAME`, then `NAME_FILE`, then systemd's
/// `$CREDENTIALS_DIRECTORY/<credential_name>`.
fn read_secret(name: &str, file_var: &str, credential_name: &str) -> Result<Option<String>> {
    if let Some(value) = env_string(name) {
        return Ok(Some(value));
    }
    if let Some(path) = env_string(file_var) {
        return read_file(Path::new(&path)).map(Some);
    }
    if let Some(dir) = env_string("CREDENTIALS_DIRECTORY") {
        let path = PathBuf::from(dir).join(credential_name);
        if path.exists() {
            return read_file(&path).map(Some);
        }
    }
    Ok(None)
}

fn read_file(path: &Path) -> Result<String> {
    let value = fs::read_to_string(path)
        .with_context(|| format!("failed to read secret file {}", path.display()))?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        bail!("secret file {} is empty", path.display());
    }
    Ok(value)
}

fn env_string(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    match env_string(name) {
        None => Ok(default),
        Some(value) => parse_bool(&value).with_context(|| {
            format!("invalid {name}: expected one of 1/0, true/false, yes/no, on/off")
        }),
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    match env_string(name) {
        None => Ok(default),
        Some(value) => value
            .parse()
            .with_context(|| format!("invalid {name}: expected a non-negative integer")),
    }
}

fn env_u32(name: &str, default: u32) -> Result<u32> {
    match env_string(name) {
        None => Ok(default),
        Some(value) => value
            .parse()
            .with_context(|| format!("invalid {name}: expected a non-negative integer")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_and_dedups_keys() {
        let keys = split_keys(" abc,def\nabc , ghi\n\n");
        assert_eq!(keys, vec!["abc", "def", "ghi"]);
    }

    #[test]
    fn validates_database_names() {
        assert!(validate_identifier("truesight").is_ok());
        assert!(validate_identifier("_x1").is_ok());
        assert!(validate_identifier("1x").is_err());
        assert!(validate_identifier("a-b").is_err());
        assert!(validate_identifier("a.b").is_err());
        assert!(validate_identifier("").is_err());
    }

    #[test]
    fn parses_boolean_values() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            assert_eq!(parse_bool(value), Some(true));
        }
        for value in ["0", "false", "NO", "off"] {
            assert_eq!(parse_bool(value), Some(false));
        }
        assert_eq!(parse_bool("maybe"), None);
    }

    #[test]
    fn debug_output_redacts_secrets() {
        let steam = SteamConfig {
            keys: vec!["super-secret-key".into()],
            base_url: "https://example.test".into(),
            timeout: Duration::from_secs(1),
        };
        let debug = format!("{steam:?}");
        assert!(!debug.contains("super-secret-key"));
        assert!(debug.contains("<1 keys>"));

        let clickhouse = ClickHouseConfig {
            url: "http://localhost:8123".into(),
            database: "truesight".into(),
            user: "default".into(),
            password: "hunter2".into(),
            timeout: Duration::from_secs(1),
        };
        let debug = format!("{clickhouse:?}");
        assert!(!debug.contains("hunter2"));
        assert!(debug.contains("[REDACTED]"));
    }
}
