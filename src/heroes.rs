use crate::ch::ClickHouse;
use crate::steam::SteamClient;
use anyhow::Result;
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const REFRESH_INTERVAL: Duration = Duration::from_secs(300);
const MAX_AGE: u32 = 24 * 3600;

#[derive(Deserialize)]
struct LastUpdate {
    updated_at: u32,
}

pub async fn run(ch: ClickHouse, steam: SteamClient, ct: CancellationToken) -> Result<()> {
    let mut interval = tokio::time::interval(REFRESH_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ct.cancelled() => break,
            _ = interval.tick() => {
                if let Err(error) = refresh(&ch, &steam).await {
                    warn!(%error, "failed to refresh hero metadata");
                }
            }
        }
    }
    info!("hero refresh stopped");
    Ok(())
}

async fn refresh(ch: &ClickHouse, steam: &SteamClient) -> Result<()> {
    let now = now();
    let rows: Vec<LastUpdate> = ch
        .query(&format!(
            "SELECT max(updated_at) AS updated_at FROM {}",
            ch.table("heroes")
        ))
        .await?;
    let last = rows.first().map(|row| row.updated_at).unwrap_or(0);
    if last > 0 && now.saturating_sub(last) < MAX_AGE {
        return Ok(());
    }

    let heroes = steam.fetch_heroes().await?;
    if heroes.is_empty() {
        warn!("steam returned an empty hero list");
        return Ok(());
    }
    let rows: Vec<_> = heroes.iter().map(|hero| hero.row(now)).collect();
    ch.insert("heroes", &rows, None).await?;
    info!(count = rows.len(), "refreshed hero metadata");
    Ok(())
}

fn now() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

#[cfg(test)]
mod tests {
    use crate::model::Hero;
    use serde_json::json;

    #[test]
    fn hero_rows_use_cdn_slug() {
        let hero = Hero::from_value(&json!({
            "id": 1,
            "name": "npc_dota_hero_antimage",
            "localized_name": "Anti-Mage",
            "primary_attr": "agi",
            "attack_type": "Melee",
            "roles": ["Carry", "Escape", "Nuker"]
        }))
        .unwrap();
        let row = hero.row(42);
        assert_eq!(row.icon, "antimage");
        assert_eq!(row.roles, vec!["Carry", "Escape", "Nuker"]);
        assert_eq!(row.updated_at, 42);
    }
}
