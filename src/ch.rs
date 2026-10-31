use crate::config::validate_identifier;
use anyhow::{Context, Result};
use clickhouse::query::Query;
use clickhouse::{Client, RowOwned, RowRead, RowWrite};
use std::{future::Future, time::Duration};

#[derive(Clone)]
pub struct ClickHouse {
    bootstrap: Client,
    client: Client,
    database: String,
    timeout: Duration,
}

impl ClickHouse {
    pub fn new(
        url: &str,
        database: &str,
        user: &str,
        password: &str,
        timeout: Duration,
    ) -> Result<Self> {
        validate_identifier(database)?;
        let mut client = Client::default()
            .with_url(url.trim_end_matches('/'))
            .with_database("default")
            .with_setting("async_insert", "0")
            .with_setting("wait_end_of_query", "1")
            .with_setting("join_use_nulls", "0");
        if !user.is_empty() {
            client = client.with_user(user);
        }
        if !password.is_empty() {
            client = client.with_password(password);
        }
        let bootstrap = client;
        let client = bootstrap.clone().with_database(database);
        Ok(Self {
            bootstrap,
            client,
            database: database.to_owned(),
            timeout,
        })
    }

    pub fn table(&self, name: &str) -> String {
        name.to_owned()
    }

    pub async fn migrate(
        &self,
        raw_retention_days: u32,
        snapshot_retention_days: u32,
    ) -> Result<()> {
        self.execute(&format!("CREATE DATABASE IF NOT EXISTS {}", self.database))
            .await?;
        for statement in
            schema_statements(&self.database, raw_retention_days, snapshot_retention_days)
        {
            self.execute(&statement)
                .await
                .with_context(|| format!("failed to apply schema statement: {statement}"))?;
        }
        Ok(())
    }

    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.with_timeout(self.bootstrap.query_raw(sql).execute(), "query")
            .await
    }

    pub async fn query_with<T, F>(&self, sql: &str, apply: F) -> Result<Vec<T>>
    where
        T: RowOwned + RowRead,
        F: FnOnce(Query) -> Query,
    {
        let query = apply(self.client.query(sql));
        self.with_timeout(query.fetch_all::<T>(), "query").await
    }

    async fn with_timeout<T>(
        &self,
        future: impl Future<Output = clickhouse::error::Result<T>>,
        operation: &str,
    ) -> Result<T> {
        tokio::time::timeout(self.timeout, future)
            .await
            .with_context(|| format!("ClickHouse {operation} timed out after {:?}", self.timeout))?
            .with_context(|| format!("ClickHouse {operation} failed"))
    }

    pub async fn insert<T: RowOwned + RowWrite>(
        &self,
        table: &str,
        rows: &[T],
        dedup_token: Option<&str>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .with_timeout(self.client.insert::<T>(table), "insert metadata")
            .await?
            .with_timeouts(Some(self.timeout), Some(self.timeout));
        if let Some(token) = dedup_token {
            insert = insert.with_setting("insert_deduplication_token", token);
        }
        for row in rows {
            insert
                .write(row)
                .await
                .with_context(|| format!("failed to write row for {table}"))?;
        }
        insert
            .end()
            .await
            .with_context(|| format!("failed to finish insert into {table}"))
    }
}

pub fn schema_statements(
    database: &str,
    raw_retention_days: u32,
    snapshot_retention_days: u32,
) -> Vec<String> {
    let raw_ttl = if raw_retention_days > 0 {
        format!(" TTL toDateTime(start_time) + INTERVAL {raw_retention_days} DAY")
    } else {
        String::new()
    };
    let snapshot_ttl = if snapshot_retention_days > 0 {
        format!(" TTL toDateTime(computed_at) + INTERVAL {snapshot_retention_days} DAY DELETE")
    } else {
        String::new()
    };

    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.matches (
                match_id UInt64,
                match_seq_num UInt64,
                start_time UInt32,
                duration UInt32,
                lobby_type UInt8,
                game_mode UInt8,
                radiant_win Bool,
                radiant_score UInt16,
                dire_score UInt16,
                avg_rank_tier UInt16,
                cluster UInt32,
                radiant_team_id UInt32,
                dire_team_id UInt32,
                radiant_heroes Array(UInt16),
                dire_heroes Array(UInt16),
                radiant_account_ids Array(UInt64),
                dire_account_ids Array(UInt64),
                raw String CODEC(ZSTD(3)){raw_ttl},
                ingested_at UInt32 DEFAULT toUnixTimestamp(now())
            )
            ENGINE = ReplacingMergeTree(ingested_at)
            PARTITION BY toYYYYMM(toDateTime(start_time))
            ORDER BY (start_time, match_id)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.match_players (
                match_id UInt64,
                start_time UInt32,
                player_slot UInt8,
                is_radiant Bool,
                account_id UInt64,
                hero_id UInt16,
                player_name String,
                kills UInt16,
                deaths UInt16,
                assists UInt16,
                leaver_status UInt8,
                level UInt8,
                gold UInt32,
                net_worth UInt32,
                last_hits UInt16,
                denies UInt16,
                gold_per_min UInt16,
                xp_per_min UInt16,
                hero_damage UInt32,
                tower_damage UInt32,
                hero_healing UInt32,
                items Array(UInt32),
                backpack Array(UInt32),
                aghanims_scepter UInt8,
                aghanims_shard UInt8,
                ingested_at UInt32 DEFAULT toUnixTimestamp(now())
            )
            ENGINE = ReplacingMergeTree(ingested_at)
            PARTITION BY toYYYYMM(toDateTime(start_time))
            ORDER BY (match_id, player_slot)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.hero_matchups_hourly (
                hour UInt32,
                hero_lo UInt16,
                hero_hi UInt16,
                games UInt64,
                lo_wins UInt64,
                lo_radiant_games UInt64,
                lo_radiant_wins UInt64
            )
            ENGINE = SummingMergeTree
            ORDER BY (hour, hero_lo, hero_hi)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.hero_stats_hourly (
                hour UInt32,
                hero_id UInt16,
                games UInt64,
                wins UInt64,
                radiant_games UInt64,
                radiant_wins UInt64
            )
            ENGINE = SummingMergeTree
            ORDER BY (hour, hero_id)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.heroes (
                hero_id UInt16,
                name String,
                localized_name String,
                icon String,
                primary_attr String,
                attack_type String,
                roles Array(String),
                updated_at UInt32
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY hero_id"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.collector_state (
                key String,
                value String,
                updated_at UInt64
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY key"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.hero_ratings (
                computed_at UInt32,
                period LowCardinality(String),
                patch LowCardinality(String),
                window_start UInt32,
                hero_id UInt16,
                rating Float64,
                rating_se Float64,
                games UInt64,
                wins UInt64,
                win_rate Float64,
                prev_rating Float64,
                delta Float64,
                rank UInt16
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(toDateTime(computed_at))
            ORDER BY (period, computed_at, hero_id){snapshot_ttl}"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.rating_models (
                computed_at UInt32,
                period LowCardinality(String),
                patch LowCardinality(String),
                window_start UInt32,
                window_end UInt32,
                matches UInt64,
                heroes UInt32,
                radiant_advantage Float64,
                iterations UInt8,
                converged Bool,
                log_likelihood Float64,
                lambda Float64
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(toDateTime(computed_at))
            ORDER BY (period, computed_at){snapshot_ttl}"
        ),
        format!(
            "ALTER TABLE {database}.rating_models
             ADD COLUMN IF NOT EXISTS window_end UInt32 DEFAULT 0"
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_are_unqualified() {
        let ch = ClickHouse::new(
            "http://127.0.0.1:8123",
            "truesight",
            "",
            "",
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(ch.table("matches"), "matches");
    }

    #[test]
    fn schema_has_all_tables() {
        let statements = schema_statements("truesight", 0, 0);
        assert_eq!(statements.len(), 9);
        let joined = statements.join("\n");
        for table in [
            "matches",
            "match_players",
            "hero_matchups_hourly",
            "hero_stats_hourly",
            "heroes",
            "collector_state",
            "hero_ratings",
            "rating_models",
        ] {
            assert!(joined.contains(&format!("truesight.{table}")));
        }
        assert!(!joined.contains("TTL"));
    }

    #[test]
    fn schema_applies_raw_ttl() {
        let joined = schema_statements("truesight", 90, 0).join("\n");
        assert!(joined.contains("TTL toDateTime(start_time) + INTERVAL 90 DAY"));
    }

    #[test]
    fn schema_applies_snapshot_ttl() {
        let joined = schema_statements("truesight", 0, 180).join("\n");
        assert!(joined.contains("TTL toDateTime(computed_at) + INTERVAL 180 DAY DELETE"));
        assert!(
            !schema_statements("truesight", 0, 0)
                .join("\n")
                .contains("INTERVAL 180 DAY")
        );
    }
}
