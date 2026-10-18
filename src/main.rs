mod ch;
mod collector;
mod config;
mod hero_data;
mod heroes;
mod model;
mod period;
mod ranker;
mod rating;
mod steam;
mod web;

use anyhow::{Context, Result};
use ch::ClickHouse;
use config::Config;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::load()?;
    init_tracing(&cfg.log);

    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        warn!("rustls crypto provider was already installed");
    }

    info!(
        version = env!("CARGO_PKG_VERSION"),
        bind = %cfg.bind,
        database = %cfg.clickhouse.database,
        patch = cfg.ranker.patch.as_ref().map(|p| p.name.as_str()).unwrap_or(""),
        keys = cfg.steam.keys.len(),
        "starting truesight"
    );

    let ch = ClickHouse::new(
        &cfg.clickhouse.url,
        &cfg.clickhouse.database,
        &cfg.clickhouse.user,
        &cfg.clickhouse.password,
        cfg.clickhouse.timeout,
    )?;
    migrate_with_retry(&ch, cfg.raw_retention_days).await?;
    if let Err(error) = heroes::seed_fallback(&ch).await {
        warn!(%error, "failed to seed hardcoded hero metadata; web will use the built-in fallback");
    }

    let steam = if cfg.steam.keys.is_empty() {
        None
    } else {
        Some(steam::SteamClient::new(&cfg.steam)?)
    };

    let ct = CancellationToken::new();
    let mut tasks = JoinSet::new();

    if let Some(steam) = steam.clone() {
        tasks.spawn(heroes::run(ch.clone(), steam.clone(), ct.clone()));
    } else {
        warn!("no Steam API keys; hero metadata will not be loaded");
    }

    if cfg.collector.enabled {
        let steam = steam
            .clone()
            .context("collector is enabled but no Steam client could be built")?;
        tasks.spawn(collector::run(
            ch.clone(),
            steam,
            cfg.collector.clone(),
            ct.clone(),
        ));
    } else {
        warn!("collector disabled; no new matches will be ingested");
    }

    if cfg.ranker.enabled {
        tasks.spawn(ranker::run(ch.clone(), cfg.ranker.clone(), ct.clone()));
    } else {
        warn!("ranker disabled; ratings will not be refreshed");
    }

    let state = web::AppState::new(ch, cfg.ranker.patch.as_ref().map(|p| p.start));
    tasks.spawn(web::serve(state, cfg.bind, ct.clone()));

    tokio::select! {
        _ = shutdown_signal() => info!("shutdown signal received"),
        joined = tasks.join_next() => {
            match joined {
                Some(Ok(Err(error))) => error!(%error, "service task failed"),
                Some(Err(error)) => error!(%error, "service task panicked"),
                Some(Ok(Ok(()))) => warn!("service task exited unexpectedly"),
                None => {}
            }
        }
    }

    ct.cancel();
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined {
            warn!(%error, "task failed while shutting down");
        }
    }
    info!("truesight stopped");
    Ok(())
}

async fn migrate_with_retry(ch: &ClickHouse, raw_retention_days: u32) -> Result<()> {
    let mut last_error = None;
    for attempt in 1..=30 {
        match ch.migrate(raw_retention_days).await {
            Ok(()) => {
                info!("ClickHouse schema is ready");
                return Ok(());
            }
            Err(error) => {
                warn!(attempt, %error, "ClickHouse not ready, retrying");
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("failed to initialize ClickHouse")))
}

fn init_tracing(filter: &str) {
    let filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to listen for Ctrl-C");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                warn!(%error, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
