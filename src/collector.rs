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

    let mut consecutive_errors = 0u32;
    loop {
        if ct.is_cancelled() {
            break;
        }

        match steam
            .fetch_by_sequence(cursor.saturating_add(1), cfg.batch_size)
            .await
        {
            Ok(matches) if matches.is_empty() => {
                debug!(cursor, "caught up with the steam sequence stream");
                if !sleep_or_cancel(&ct, cfg.poll_interval).await {
                    break;
                }
            }
            Ok(matches) => {
                consecutive_errors = 0;
                let batch_end = matches
                    .iter()
                    .filter_map(|m| m.get("match_seq_num").and_then(|v| v.as_u64()))
                    .max()
                    .unwrap_or(cursor);
                if batch_end <= cursor {
                    warn!(
                        cursor,
                        batch_end, "Steam returned a non-advancing sequence batch"
                    );
                    if !sleep_or_cancel(&ct, cfg.poll_interval).await {
                        break;
                    }
                    continue;
                }
                let token = format!("{}-{}", cursor.saturating_add(1), batch_end);
                match store_batch(&ch, &matches, &token).await {
                    Ok((stored, rated)) => {
                        cursor = batch_end.max(cursor);
                        save_cursor(&ch, cursor).await?;
                        debug!(
                            cursor,
                            fetched = matches.len(),
                            stored,
                            rated,
                            "stored sequence batch"
                        );
                    }
                    Err(error) => {
                        warn!(%error, token, "failed to store batch, will retry");
                        if !sleep_or_cancel(&ct, retry_delay(consecutive_errors)).await {
                            break;
                        }
                        consecutive_errors = consecutive_errors.saturating_add(1);
                        continue;
                    }
                }

                if matches.len() < cfg.batch_size as usize {
                    if !sleep_or_cancel(&ct, cfg.poll_interval).await {
                        break;
                    }
                } else {
                    tokio::task::yield_now().await;
                }
            }
            Err(error) => {
                warn!(%error, "steam fetch failed");
                if !sleep_or_cancel(&ct, retry_delay(consecutive_errors)).await {
                    break;
                }
                consecutive_errors = consecutive_errors.saturating_add(1);
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

fn retry_delay(consecutive_errors: u32) -> Duration {
    Duration::from_secs(2u64.saturating_pow(consecutive_errors.min(5)).min(30))
}

async fn sleep_or_cancel(ct: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = ct.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}
