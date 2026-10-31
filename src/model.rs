use anyhow::{Context, Result, bail};
use clickhouse::Row;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

pub const RANKED_LOBBY: u8 = 7;
pub const RANKED_ALL_PICK: u8 = 22;
pub const MIN_RATED_DURATION: u32 = 600;

#[derive(Debug, Clone)]
pub struct Match {
    pub match_id: u64,
    pub match_seq_num: u64,
    pub start_time: u32,
    pub duration: u32,
    pub lobby_type: u8,
    pub game_mode: u8,
    pub radiant_win: bool,
    pub radiant_score: u16,
    pub dire_score: u16,
    pub avg_rank_tier: u16,
    pub cluster: u32,
    pub radiant_team_id: u32,
    pub dire_team_id: u32,
    pub players: Vec<Player>,
    pub raw: String,
}

#[derive(Debug, Clone)]
pub struct Player {
    pub account_id: u64,
    pub player_slot: u8,
    pub hero_id: u16,
    pub player_name: String,
    pub kills: u16,
    pub deaths: u16,
    pub assists: u16,
    pub leaver_status: u8,
    pub level: u8,
    pub gold: u32,
    pub net_worth: u32,
    pub last_hits: u16,
    pub denies: u16,
    pub gold_per_min: u16,
    pub xp_per_min: u16,
    pub hero_damage: u32,
    pub tower_damage: u32,
    pub hero_healing: u32,
    pub items: Vec<u32>,
    pub backpack: Vec<u32>,
    pub aghanims_scepter: u8,
    pub aghanims_shard: u8,
}

impl Player {
    pub fn is_radiant(&self) -> bool {
        self.player_slot < 128
    }
}

impl Match {
    pub fn from_value(value: &Value) -> Result<Self> {
        let match_id = value
            .get("match_id")
            .and_then(Value::as_u64)
            .context("match is missing match_id")?;
        let match_seq_num = value
            .get("match_seq_num")
            .and_then(Value::as_u64)
            .context("match is missing match_seq_num")?;
        let start_time = uint(value, "start_time") as u32;
        if start_time == 0 {
            bail!("match {match_id} is missing start_time");
        }
        let radiant_win = value
            .get("radiant_win")
            .and_then(Value::as_bool)
            .with_context(|| format!("match {match_id} is missing radiant_win"))?;

        let players = value
            .get("players")
            .and_then(Value::as_array)
            .map(|players| players.iter().filter_map(Player::from_value).collect())
            .unwrap_or_default();

        Ok(Self {
            match_id,
            match_seq_num,
            start_time,
            duration: uint(value, "duration") as u32,
            lobby_type: uint(value, "lobby_type") as u8,
            game_mode: uint(value, "game_mode") as u8,
            radiant_win,
            radiant_score: uint(value, "radiant_score") as u16,
            dire_score: uint(value, "dire_score") as u16,
            avg_rank_tier: uint(value, "avg_rank_tier") as u16,
            cluster: uint(value, "cluster") as u32,
            radiant_team_id: uint(value, "radiant_team_id") as u32,
            dire_team_id: uint(value, "dire_team_id") as u32,
            players,
            raw: serde_json::to_string(value).context("failed to serialize match")?,
        })
    }

    pub fn is_ranked(&self) -> bool {
        self.lobby_type == RANKED_LOBBY && self.game_mode == RANKED_ALL_PICK
    }

    pub fn hour(&self) -> u32 {
        self.start_time / 3600 * 3600
    }

    pub fn radiant_heroes(&self) -> Vec<u16> {
        self.players
            .iter()
            .filter(|p| p.is_radiant() && p.hero_id != 0)
            .map(|p| p.hero_id)
            .collect()
    }

    pub fn dire_heroes(&self) -> Vec<u16> {
        self.players
            .iter()
            .filter(|p| !p.is_radiant() && p.hero_id != 0)
            .map(|p| p.hero_id)
            .collect()
    }

    /// Matches suitable for the rating model. Everything is still stored.
    pub fn rateable(&self) -> bool {
        self.is_ranked()
            && self.duration >= MIN_RATED_DURATION
            && self.players.len() >= 10
            && self.players.iter().all(|p| p.leaver_status < 2)
            && self.radiant_heroes().len() == 5
            && self.dire_heroes().len() == 5
    }

    pub fn match_row(&self) -> MatchRow {
        MatchRow {
            match_id: self.match_id,
            match_seq_num: self.match_seq_num,
            start_time: self.start_time,
            duration: self.duration,
            lobby_type: self.lobby_type,
            game_mode: self.game_mode,
            radiant_win: self.radiant_win,
            radiant_score: self.radiant_score,
            dire_score: self.dire_score,
            avg_rank_tier: self.avg_rank_tier,
            cluster: self.cluster,
            radiant_team_id: self.radiant_team_id,
            dire_team_id: self.dire_team_id,
            radiant_heroes: self.radiant_heroes(),
            dire_heroes: self.dire_heroes(),
            radiant_account_ids: self
                .players
                .iter()
                .filter(|p| p.is_radiant())
                .map(|p| p.account_id)
                .collect(),
            dire_account_ids: self
                .players
                .iter()
                .filter(|p| !p.is_radiant())
                .map(|p| p.account_id)
                .collect(),
            raw: self.raw.clone(),
        }
    }

    pub fn player_rows(&self) -> Vec<PlayerRow> {
        self.players
            .iter()
            .map(|p| PlayerRow {
                match_id: self.match_id,
                start_time: self.start_time,
                player_slot: p.player_slot,
                is_radiant: p.is_radiant(),
                account_id: p.account_id,
                hero_id: p.hero_id,
                player_name: p.player_name.clone(),
                kills: p.kills,
                deaths: p.deaths,
                assists: p.assists,
                leaver_status: p.leaver_status,
                level: p.level,
                gold: p.gold,
                net_worth: p.net_worth,
                last_hits: p.last_hits,
                denies: p.denies,
                gold_per_min: p.gold_per_min,
                xp_per_min: p.xp_per_min,
                hero_damage: p.hero_damage,
                tower_damage: p.tower_damage,
                hero_healing: p.hero_healing,
                items: p.items.clone(),
                backpack: p.backpack.clone(),
                aghanims_scepter: p.aghanims_scepter,
                aghanims_shard: p.aghanims_shard,
            })
            .collect()
    }
}

impl Player {
    fn from_value(value: &Value) -> Option<Self> {
        let hero_id = uint(value, "hero_id") as u16;
        if hero_id == 0 {
            return None;
        }
        let mut items = Vec::with_capacity(6);
        for i in 0..6 {
            items.push(uint(value, &format!("item_{i}")) as u32);
        }
        let mut backpack = Vec::with_capacity(3);
        for i in 0..3 {
            backpack.push(uint(value, &format!("backpack_{i}")) as u32);
        }
        Some(Self {
            account_id: uint(value, "account_id"),
            player_slot: uint(value, "player_slot") as u8,
            hero_id,
            player_name: value
                .get("player_name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            kills: uint(value, "kills") as u16,
            deaths: uint(value, "deaths") as u16,
            assists: uint(value, "assists") as u16,
            leaver_status: uint(value, "leaver_status") as u8,
            level: uint(value, "level") as u8,
            gold: uint(value, "gold") as u32,
            net_worth: uint(value, "net_worth") as u32,
            last_hits: uint(value, "last_hits") as u16,
            denies: uint(value, "denies") as u16,
            gold_per_min: uint(value, "gold_per_min") as u16,
            xp_per_min: uint(value, "xp_per_min") as u16,
            hero_damage: uint(value, "hero_damage") as u32,
            tower_damage: uint(value, "tower_damage") as u32,
            hero_healing: uint(value, "hero_healing") as u32,
            items,
            backpack,
            aghanims_scepter: uint(value, "aghanims_scepter") as u8,
            aghanims_shard: uint(value, "aghanims_shard") as u8,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Hero {
    pub id: u16,
    pub name: String,
    pub localized_name: String,
    pub primary_attr: String,
    pub attack_type: String,
    pub roles: Vec<String>,
}

impl Hero {
    pub fn from_value(value: &Value) -> Result<Self> {
        let id = value
            .get("id")
            .and_then(Value::as_u64)
            .context("hero is missing id")? as u16;
        if id == 0 {
            bail!("hero has id 0");
        }
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let localized_name = value
            .get("localized_name")
            .and_then(Value::as_str)
            .unwrap_or(&name)
            .to_owned();
        let roles = value
            .get("roles")
            .and_then(Value::as_array)
            .map(|roles| {
                roles
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            id,
            name,
            localized_name,
            primary_attr: value
                .get("primary_attr")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            attack_type: value
                .get("attack_type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            roles,
        })
    }

    pub fn icon(&self) -> String {
        self.name
            .strip_prefix("npc_dota_hero_")
            .unwrap_or(&self.name)
            .to_owned()
    }

    pub fn row(&self, updated_at: u32) -> HeroRow {
        HeroRow {
            hero_id: self.id,
            name: self.name.clone(),
            localized_name: self.localized_name.clone(),
            icon: self.icon(),
            primary_attr: self.primary_attr.clone(),
            attack_type: self.attack_type.clone(),
            roles: self.roles.clone(),
            updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Row)]
pub struct MatchRow {
    pub match_id: u64,
    pub match_seq_num: u64,
    pub start_time: u32,
    pub duration: u32,
    pub lobby_type: u8,
    pub game_mode: u8,
    pub radiant_win: bool,
    pub radiant_score: u16,
    pub dire_score: u16,
    pub avg_rank_tier: u16,
    pub cluster: u32,
    pub radiant_team_id: u32,
    pub dire_team_id: u32,
    pub radiant_heroes: Vec<u16>,
    pub dire_heroes: Vec<u16>,
    pub radiant_account_ids: Vec<u64>,
    pub dire_account_ids: Vec<u64>,
    pub raw: String,
}

#[derive(Debug, Clone, Serialize, Row)]
pub struct PlayerRow {
    pub match_id: u64,
    pub start_time: u32,
    pub player_slot: u8,
    pub is_radiant: bool,
    pub account_id: u64,
    pub hero_id: u16,
    pub player_name: String,
    pub kills: u16,
    pub deaths: u16,
    pub assists: u16,
    pub leaver_status: u8,
    pub level: u8,
    pub gold: u32,
    pub net_worth: u32,
    pub last_hits: u16,
    pub denies: u16,
    pub gold_per_min: u16,
    pub xp_per_min: u16,
    pub hero_damage: u32,
    pub tower_damage: u32,
    pub hero_healing: u32,
    pub items: Vec<u32>,
    pub backpack: Vec<u32>,
    pub aghanims_scepter: u8,
    pub aghanims_shard: u8,
}

#[derive(Debug, Clone, Serialize, Row)]
pub struct MatchupRow {
    pub hour: u32,
    pub hero_lo: u16,
    pub hero_hi: u16,
    pub games: u64,
    pub lo_wins: u64,
    pub lo_radiant_games: u64,
    pub lo_radiant_wins: u64,
}

#[derive(Debug, Clone, Serialize, Row)]
pub struct HeroStatRow {
    pub hour: u32,
    pub hero_id: u16,
    pub games: u64,
    pub wins: u64,
    pub radiant_games: u64,
    pub radiant_wins: u64,
}

#[derive(Debug, Clone, Serialize, Row)]
pub struct HeroRow {
    pub hero_id: u16,
    pub name: String,
    pub localized_name: String,
    pub icon: String,
    pub primary_attr: String,
    pub attack_type: String,
    pub roles: Vec<String>,
    pub updated_at: u32,
}

#[derive(Debug, Clone, Serialize, Row)]
pub struct StateRow {
    pub key: String,
    pub value: String,
    pub updated_at: u64,
}

#[derive(Default)]
pub struct Accumulator {
    matchups: HashMap<(u32, u16, u16), MatchupCounts>,
    heroes: HashMap<(u32, u16), HeroCounts>,
    pub matches: u64,
}

#[derive(Default, Clone, Copy)]
struct MatchupCounts {
    games: u64,
    lo_wins: u64,
    lo_radiant_games: u64,
    lo_radiant_wins: u64,
}

#[derive(Default, Clone, Copy)]
struct HeroCounts {
    games: u64,
    wins: u64,
    radiant_games: u64,
    radiant_wins: u64,
}

impl Accumulator {
    pub fn add(&mut self, game: &Match) {
        if !game.rateable() {
            return;
        }
        self.matches += 1;
        let hour = game.hour();
        let radiant = game.radiant_heroes();
        let dire = game.dire_heroes();

        for &r in &radiant {
            for &d in &dire {
                if r == d {
                    continue;
                }
                let lo = r.min(d);
                let hi = r.max(d);
                let entry = self.matchups.entry((hour, lo, hi)).or_default();
                entry.games += 1;
                if lo == r {
                    entry.lo_radiant_games += 1;
                    if game.radiant_win {
                        entry.lo_radiant_wins += 1;
                        entry.lo_wins += 1;
                    }
                } else if !game.radiant_win {
                    entry.lo_wins += 1;
                }
            }
        }

        for (hero, is_radiant) in radiant
            .iter()
            .map(|h| (*h, true))
            .chain(dire.iter().map(|h| (*h, false)))
        {
            let entry = self.heroes.entry((hour, hero)).or_default();
            entry.games += 1;
            let won = (is_radiant && game.radiant_win) || (!is_radiant && !game.radiant_win);
            if won {
                entry.wins += 1;
            }
            if is_radiant {
                entry.radiant_games += 1;
                if game.radiant_win {
                    entry.radiant_wins += 1;
                }
            }
        }
    }

    pub fn into_rows(self) -> (Vec<MatchupRow>, Vec<HeroStatRow>) {
        let mut matchups: Vec<MatchupRow> = self
            .matchups
            .into_iter()
            .map(|((hour, hero_lo, hero_hi), c)| MatchupRow {
                hour,
                hero_lo,
                hero_hi,
                games: c.games,
                lo_wins: c.lo_wins,
                lo_radiant_games: c.lo_radiant_games,
                lo_radiant_wins: c.lo_radiant_wins,
            })
            .collect();
        matchups.sort_by_key(|r| (r.hour, r.hero_lo, r.hero_hi));

        let mut heroes: Vec<HeroStatRow> = self
            .heroes
            .into_iter()
            .map(|((hour, hero_id), c)| HeroStatRow {
                hour,
                hero_id,
                games: c.games,
                wins: c.wins,
                radiant_games: c.radiant_games,
                radiant_wins: c.radiant_wins,
            })
            .collect();
        heroes.sort_by_key(|r| (r.hour, r.hero_id));

        (matchups, heroes)
    }
}

fn uint(value: &Value, field: &str) -> u64 {
    value
        .get(field)
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub fn fixture_match(match_id: u64, radiant_win: bool) -> Value {
        let mut players = Vec::new();
        for slot in 0..5u8 {
            players.push(json!({
                "account_id": 1000 + slot as u64,
                "player_slot": slot,
                "hero_id": slot + 1,
                "player_name": format!("radiant-{slot}"),
                "kills": 5, "deaths": 2, "assists": 9, "leaver_status": 0,
                "level": 20, "gold": 1000, "net_worth": 20000,
                "last_hits": 200, "denies": 10, "gold_per_min": 500, "xp_per_min": 600,
                "hero_damage": 20000, "tower_damage": 5000, "hero_healing": 0,
                "item_0": 1, "item_1": 2, "item_2": 3, "item_3": 4, "item_4": 5, "item_5": 6,
                "backpack_0": 7, "backpack_1": 8, "backpack_2": 9,
                "aghanims_scepter": 0, "aghanims_shard": 1
            }));
        }
        for slot in 0..5u8 {
            players.push(json!({
                "account_id": 2000 + slot as u64,
                "player_slot": 128 + slot,
                "hero_id": slot + 6,
                "player_name": format!("dire-{slot}"),
                "kills": 2, "deaths": 5, "assists": 4, "leaver_status": 0,
                "level": 18, "gold": 900, "net_worth": 18000,
                "last_hits": 150, "denies": 5, "gold_per_min": 450, "xp_per_min": 550,
                "hero_damage": 15000, "tower_damage": 2000, "hero_healing": 100,
                "item_0": 10, "item_1": 11, "item_2": 12, "item_3": 13, "item_4": 14, "item_5": 15,
                "backpack_0": 16, "backpack_1": 17, "backpack_2": 18,
                "aghanims_scepter": 1, "aghanims_shard": 0
            }));
        }
        json!({
            "match_id": match_id,
            "match_seq_num": match_id * 2,
            "start_time": 1_700_000_000u32,
            "duration": 2400,
            "lobby_type": 7,
            "game_mode": 22,
            "radiant_win": radiant_win,
            "radiant_score": 30,
            "dire_score": 20,
            "avg_rank_tier": 75,
            "cluster": 111,
            "radiant_team_id": 0,
            "dire_team_id": 0,
            "players": players,
            "objectives": [{"time": 100, "type": "CHAT_MESSAGE_FIRSTBLOOD"}]
        })
    }

    #[test]
    fn parses_full_match() {
        let value = fixture_match(123, true);
        let game = Match::from_value(&value).unwrap();
        assert_eq!(game.match_id, 123);
        assert!(game.is_ranked());
        assert!(game.rateable());
        assert_eq!(game.radiant_heroes(), vec![1, 2, 3, 4, 5]);
        assert_eq!(game.dire_heroes(), vec![6, 7, 8, 9, 10]);
        assert!(game.raw.contains("objectives"));
        assert_eq!(game.player_rows()[0].items, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn rejects_short_or_unranked_games() {
        let mut value = fixture_match(1, true);
        value["duration"] = json!(300);
        assert!(!Match::from_value(&value).unwrap().rateable());

        let mut value = fixture_match(2, true);
        value["lobby_type"] = json!(0);
        assert!(!Match::from_value(&value).unwrap().rateable());
    }

    #[test]
    fn rejects_match_without_result() {
        let mut value = fixture_match(1, true);
        value.as_object_mut().unwrap().remove("radiant_win");
        assert!(Match::from_value(&value).is_err());
    }

    #[test]
    fn accumulates_matchups_and_heroes() {
        let value = fixture_match(1, true);
        let game = Match::from_value(&value).unwrap();
        let mut acc = Accumulator::default();
        acc.add(&game);
        assert_eq!(acc.matches, 1);
        let (matchups, heroes) = acc.into_rows();
        assert_eq!(matchups.len(), 25);
        assert_eq!(heroes.len(), 10);
        assert!(matchups.iter().all(|m| m.games == 1));
        // Hero 1 is radiant and won all five cross-team pairs.
        let hero1 = matchups
            .iter()
            .find(|m| m.hero_lo == 1 && m.hero_hi == 6)
            .unwrap();
        assert_eq!(hero1.lo_wins, 1);
        assert_eq!(hero1.lo_radiant_games, 1);
        assert_eq!(hero1.lo_radiant_wins, 1);
        let stat1 = heroes.iter().find(|h| h.hero_id == 1).unwrap();
        assert_eq!(stat1.wins, 1);
        let stat6 = heroes.iter().find(|h| h.hero_id == 6).unwrap();
        assert_eq!(stat6.wins, 0);
    }
}
