use crate::ch::ClickHouse;
use crate::config::RankerConfig;
use crate::period::Period;
use crate::rating::{self, FitOptions, HeroCounts, PairCounts};
use anyhow::{Context, Result};
use clickhouse::Row;
use clickhouse::sql::Identifier;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const LN_2: f64 = std::f64::consts::LN_2;

#[derive(Deserialize, Row)]
struct PairAggregate {
    hero_lo: u16,
    hero_hi: u16,
    w_games: f64,
    w_lo_wins: f64,
    w_lo_radiant_games: f64,
    w_lo_radiant_wins: f64,
}

#[derive(Deserialize, Row)]
struct HeroAggregate {
    hero_id: u16,
    games: u64,
    wins: u64,
}

#[derive(Deserialize, Row)]
struct PreviousRating {
    hero_id: u16,
    rating: f64,
}

#[derive(Deserialize, Row)]
struct MaxComputedAt {
    computed_at: u32,
}

#[derive(Deserialize, Row)]
struct LatestMatch {
    start_time: u32,
}

#[derive(Deserialize, Row)]
struct LatestIngest {
    ingested_at: u32,
}

#[derive(Deserialize, Row)]
struct LatestModel {
    computed_at: u32,
    patch: String,
    window_start: u32,
    window_end: u32,
}

#[derive(Serialize, Row)]
struct RatingRow {
    computed_at: u32,
    period: String,
    patch: String,
    window_start: u32,
    hero_id: u16,
    rating: f64,
    rating_se: f64,
    games: u64,
    wins: u64,
    win_rate: f64,
    prev_rating: f64,
    delta: f64,
    rank: u16,
}

#[derive(Serialize, Row)]
struct ModelRow {
    computed_at: u32,
    period: String,
    patch: String,
    window_start: u32,
    window_end: u32,
    matches: u64,
    heroes: u32,
    radiant_advantage: f64,
    iterations: u8,
    converged: bool,
    log_likelihood: f64,
    lambda: f64,
}

pub async fn run(ch: ClickHouse, cfg: RankerConfig, ct: CancellationToken) -> Result<()> {
    let mut interval = tokio::time::interval(cfg.interval.max(Duration::from_secs(5)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ct.cancelled() => break,
            _ = interval.tick() => {
                match latest_match_time(&ch).await {
                    Ok(Some(anchor)) => {
                        for period in Period::ALL {
                            if let Err(error) = rank_period(&ch, &cfg, period, anchor).await {
                                warn!(error = %format!("{error:#}"), period = period.as_str(), "ranking failed");
                            }
                        }
                    }
                    Ok(None) => info!("no collected matches yet, skipping ranking"),
                    Err(error) => warn!(error = %format!("{error:#}"), "failed to determine ranking anchor"),
                }
            }
        }
    }
    info!("ranker stopped");
    Ok(())
}

async fn rank_period(
    ch: &ClickHouse,
    cfg: &RankerConfig,
    period: Period,
    anchor: u32,
) -> Result<()> {
    let now = now().max(anchor);
    let Some((window_start, window_end)) =
        window_bounds(period, anchor, cfg.patch.as_ref().map(|patch| patch.start))
    else {
        info!(
            period = period.as_str(),
            anchor, "no complete window yet, skipping ranking"
        );
        return Ok(());
    };

    let patch = cfg
        .patch
        .as_ref()
        .map(|p| p.name.clone())
        .unwrap_or_default();
    let previous_model = latest_model(ch, period).await?;
    let latest_ingest = latest_ingest_time(ch).await?;
    if let Some(previous) = previous_model.as_ref()
        && snapshot_is_current(previous, &patch, window_start, window_end, latest_ingest)
    {
        info!(
            period = period.as_str(),
            computed_at = previous.computed_at,
            window_start,
            window_end,
            "ratings are already current, skipping fit"
        );
        return Ok(());
    }

    let since_hour = window_start / 3600 * 3600;
    let end_hour = window_end / 3600 * 3600;
    let half_life_hours = (period.seconds() as f64 / 2.0) / 3600.0;
    let matchup_table = ch.table("hero_matchups_hourly");
    let hero_table = ch.table("hero_stats_hourly");
    let pair_sql = "
        SELECT hero_lo, hero_hi,
            sum(games * weight) AS w_games,
            sum(lo_wins * weight) AS w_lo_wins,
            sum(lo_radiant_games * weight) AS w_lo_radiant_games,
            sum(lo_radiant_wins * weight) AS w_lo_radiant_wins
        FROM (
            SELECT *,
                exp(-? * (toInt64(?) - toInt64(hour)) / 3600.0 / ?) AS weight
            FROM ?
            WHERE hour >= ? AND hour <= ?
        )
        GROUP BY hero_lo, hero_hi";
    let hero_sql = "
        SELECT hero_id,
            sum(games) AS games,
            sum(wins) AS wins
        FROM ?
        WHERE hour >= ? AND hour <= ?
        GROUP BY hero_id";

    let pairs: Vec<PairAggregate> = ch
        .query_with(pair_sql, |query| {
            query
                .bind(LN_2)
                .bind(window_end)
                .bind(half_life_hours)
                .bind(Identifier(&matchup_table))
                .bind(since_hour)
                .bind(end_hour)
        })
        .await?;
    let heroes: Vec<HeroAggregate> = ch
        .query_with(hero_sql, |query| {
            query
                .bind(Identifier(&hero_table))
                .bind(since_hour)
                .bind(end_hour)
        })
        .await?;
    if pairs.is_empty() || heroes.is_empty() {
        info!(period = period.as_str(), "not enough data for ranking yet");
        return Ok(());
    }

    let pair_counts: Vec<PairCounts> = pairs
        .iter()
        .filter(|p| p.w_games > 0.0)
        .map(|p| PairCounts {
            lo: p.hero_lo,
            hi: p.hero_hi,
            games: p.w_games,
            lo_wins: p.w_lo_wins,
            lo_radiant_games: p.w_lo_radiant_games,
            lo_radiant_wins: p.w_lo_radiant_wins,
        })
        .collect();
    let hero_counts: Vec<HeroCounts> = heroes
        .iter()
        .map(|h| HeroCounts {
            hero_id: h.hero_id,
            games: h.games,
            wins: h.wins.min(h.games),
        })
        .collect();

    let options = FitOptions {
        lambda: cfg.lambda,
        gamma_prior: 1.0,
        max_iterations: 50,
    };
    let fit = rating::fit(&pair_counts, &hero_counts, options);
    if fit.ratings.is_empty() {
        info!(period = period.as_str(), "fit produced no ratings");
        return Ok(());
    }

    let computed_at = next_computed_at(ch, period, now).await?;
    let previous = previous_ratings(ch, period, &patch, computed_at).await?;

    let mut rated: Vec<_> = fit
        .ratings
        .into_iter()
        .filter(|rating| rating.games >= cfg.min_games)
        .collect();
    rated.sort_by(|a, b| b.rating.partial_cmp(&a.rating).unwrap_or(Ordering::Equal));

    let rows: Vec<RatingRow> = rated
        .iter()
        .enumerate()
        .map(|(index, rating)| {
            let previous = previous
                .iter()
                .find(|p| p.hero_id == rating.hero_id)
                .map(|p| p.rating)
                .unwrap_or(rating.rating);
            RatingRow {
                computed_at,
                period: period.as_str().to_owned(),
                patch: patch.clone(),
                window_start,
                hero_id: rating.hero_id,
                rating: rating.rating,
                rating_se: rating.rating_se,
                games: rating.games,
                wins: rating.wins,
                win_rate: rating.win_rate,
                prev_rating: previous,
                delta: rating.rating - previous,
                rank: (index + 1).min(u16::MAX as usize) as u16,
            }
        })
        .collect();

    let total_matches = (heroes.iter().map(|h| h.games).sum::<u64>() as f64 / 10.0).round() as u64;

    let token = format!("{}-{}-{computed_at}", period.as_str(), patch);
    ch.insert("hero_ratings", &rows, Some(&format!("{token}-hr")))
        .await
        .context("failed to insert hero ratings")?;
    let model = ModelRow {
        computed_at,
        period: period.as_str().to_owned(),
        patch: patch.clone(),
        window_start,
        window_end,
        matches: total_matches,
        heroes: rows.len() as u32,
        radiant_advantage: fit.radiant_advantage,
        iterations: fit.iterations.min(u8::MAX as usize) as u8,
        converged: fit.converged,
        log_likelihood: fit.log_likelihood,
        lambda: options.lambda,
    };
    ch.insert("rating_models", &[model], Some(&format!("{token}-rm")))
        .await
        .context("failed to insert rating model metadata")?;

    if !fit.converged {
        warn!(
            period = period.as_str(),
            iterations = fit.iterations,
            "rating fit did not fully converge"
        );
    }
    info!(
        period = period.as_str(),
        computed_at,
        window_start,
        window_end,
        matches = total_matches,
        heroes = rows.len(),
        radiant_advantage = fit.radiant_advantage,
        iterations = fit.iterations,
        converged = fit.converged,
        "computed hero ratings"
    );
    Ok(())
}

fn window_bounds(period: Period, anchor: u32, patch_start: Option<u32>) -> Option<(u32, u32)> {
    if anchor == 0 {
        return None;
    }
    let mut start = anchor.saturating_sub(period.seconds());
    if let Some(patch_start) = patch_start {
        start = start.max(patch_start);
    }
    if start >= anchor {
        None
    } else {
        Some((start, anchor))
    }
}

async fn latest_match_time(ch: &ClickHouse) -> Result<Option<u32>> {
    let table = ch.table("matches");
    let rows: Vec<LatestMatch> = ch
        .query_with("SELECT max(start_time) AS start_time FROM ?", |query| {
            query.bind(Identifier(&table))
        })
        .await?;
    Ok(rows
        .first()
        .map(|row| row.start_time)
        .filter(|start_time| *start_time > 0))
}

async fn latest_ingest_time(ch: &ClickHouse) -> Result<Option<u32>> {
    let table = ch.table("matches");
    let rows: Vec<LatestIngest> = ch
        .query_with("SELECT max(ingested_at) AS ingested_at FROM ?", |query| {
            query.bind(Identifier(&table))
        })
        .await?;
    Ok(rows
        .first()
        .map(|row| row.ingested_at)
        .filter(|ingested_at| *ingested_at > 0))
}

fn snapshot_is_current(
    previous: &LatestModel,
    patch: &str,
    window_start: u32,
    window_end: u32,
    latest_ingest: Option<u32>,
) -> bool {
    previous.patch == patch
        && previous.window_start == window_start
        && previous.window_end == window_end
        && latest_ingest.is_some_and(|ingested_at| ingested_at < previous.computed_at)
}

async fn latest_model(ch: &ClickHouse, period: Period) -> Result<Option<LatestModel>> {
    let table = ch.table("rating_models");
    let rows: Vec<LatestModel> = ch
        .query_with(
            "SELECT computed_at, patch, window_start, window_end
             FROM ?
             WHERE period = ?
             ORDER BY computed_at DESC
             LIMIT 1",
            |query| query.bind(Identifier(&table)).bind(period.as_str()),
        )
        .await?;
    Ok(rows.into_iter().next())
}

async fn next_computed_at(ch: &ClickHouse, period: Period, now: u32) -> Result<u32> {
    let table = ch.table("rating_models");
    let rows: Vec<MaxComputedAt> = ch
        .query_with(
            "SELECT max(computed_at) AS computed_at FROM ? WHERE period = ?",
            |query| query.bind(Identifier(&table)).bind(period.as_str()),
        )
        .await?;
    let last = rows.first().map(|row| row.computed_at).unwrap_or(0);
    Ok(now.max(last.saturating_add(1)))
}

async fn previous_ratings(
    ch: &ClickHouse,
    period: Period,
    patch: &str,
    computed_at: u32,
) -> Result<Vec<PreviousRating>> {
    let table = ch.table("hero_ratings");
    let period = period.as_str();
    ch.query_with(
        "SELECT hero_id, rating
         FROM ?
         WHERE period = ? AND patch = ? AND computed_at = (
             SELECT max(computed_at) FROM ?
             WHERE period = ? AND patch = ? AND computed_at < ?
         )",
        |query| {
            query
                .bind(Identifier(&table))
                .bind(period)
                .bind(patch)
                .bind(Identifier(&table))
                .bind(period)
                .bind(patch)
                .bind(computed_at)
        },
    )
    .await
}

fn now() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn day_window_ends_at_the_anchor() {
        let anchor = 1_800_000_000;
        assert_eq!(
            window_bounds(Period::Day, anchor, None),
            Some((anchor - 86_400, anchor))
        );
    }

    #[test]
    fn patch_clips_the_window_start() {
        let anchor = 1_800_000_000;
        let patch_start = anchor - 3_600;
        assert_eq!(
            window_bounds(Period::Day, anchor, Some(patch_start)),
            Some((patch_start, anchor))
        );
    }

    #[test]
    fn patch_at_or_after_the_anchor_skips_ranking() {
        let anchor = 1_800_000_000;
        assert_eq!(window_bounds(Period::Day, anchor, Some(anchor)), None);
        assert_eq!(window_bounds(Period::Day, anchor, Some(anchor + 1)), None);
    }

    #[test]
    fn empty_anchor_skips_ranking() {
        assert_eq!(window_bounds(Period::Day, 0, None), None);
    }

    #[test]
    fn skips_only_unchanged_snapshots_without_new_data() {
        let previous = LatestModel {
            computed_at: 2_000,
            patch: "7.40".into(),
            window_start: 1_000,
            window_end: 2_000,
        };
        assert!(snapshot_is_current(
            &previous,
            "7.40",
            1_000,
            2_000,
            Some(1_999)
        ));
        assert!(!snapshot_is_current(
            &previous,
            "7.40",
            1_000,
            2_000,
            Some(2_000)
        ));
        assert!(!snapshot_is_current(
            &previous,
            "7.41",
            1_000,
            2_000,
            Some(1_999)
        ));
        assert!(!snapshot_is_current(
            &previous,
            "7.40",
            1_001,
            2_000,
            Some(1_999)
        ));
        assert!(!snapshot_is_current(&previous, "7.40", 1_000, 2_000, None));
    }
}
