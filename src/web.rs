use crate::ch::{ClickHouse, quote_string};
use crate::period::Period;
use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

const INDEX_HTML: &str = include_str!("../assets/index.html");
const APP_JS: &str = include_str!("../assets/app.js");
const STYLE_CSS: &str = include_str!("../assets/style.css");
const PAIR_CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct AppState {
    pub ch: ClickHouse,
    pub patch_start: Option<u32>,
    pub started_at: u32,
    cache: Arc<RwLock<HashMap<Period, CachedPairs>>>,
}

struct CachedPairs {
    at: Instant,
    pairs: Vec<PairRow>,
}

impl AppState {
    pub fn new(ch: ClickHouse, patch_start: Option<u32>) -> Self {
        Self {
            ch,
            patch_start,
            started_at: now(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/static/app.js", get(app_js))
        .route("/static/style.css", get(style_css))
        .route("/api/health", get(health))
        .route("/api/heroes", get(heroes))
        .route("/api/heroes/{hero_id}", get(hero_detail))
        .route("/api/matchups", get(matchups))
        .route("/api/matches", get(recent_matches))
        .with_state(state)
}

pub async fn serve(state: AppState, bind: SocketAddr, ct: CancellationToken) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!(%bind, "web server listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { ct.cancelled().await })
        .await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn app_js() -> impl IntoResponse {
    (
        [
            (
                header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        APP_JS,
    )
}

async fn style_css() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        STYLE_CSS,
    )
}

#[derive(Deserialize)]
struct PeriodQuery {
    period: Option<String>,
}

#[derive(Deserialize)]
struct HeroQuery {
    period: Option<String>,
    min_games: Option<u64>,
}

#[derive(Deserialize)]
struct MatchesQuery {
    limit: Option<u32>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    uptime_secs: u32,
    last_seq: Option<u64>,
    latest_match_time: u32,
    matches_stored: u64,
    rankings: HashMap<String, u32>,
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, ApiError> {
    let matches: Vec<CountRow> = state
        .ch
        .query(&format!(
            "SELECT count() AS count, max(start_time) AS latest, max(match_seq_num) AS seq FROM {}",
            state.ch.table("matches")
        ))
        .await
        .map_err(ApiError::internal)?;
    let row = matches.into_iter().next().unwrap_or_default();

    let seq: Vec<StateValue> = state
        .ch
        .query(&format!(
            "SELECT value FROM {} WHERE key = 'last_seq' ORDER BY updated_at DESC LIMIT 1",
            state.ch.table("collector_state")
        ))
        .await
        .map_err(ApiError::internal)?;
    let last_seq = seq
        .first()
        .and_then(|v| v.value.parse::<u64>().ok())
        .or(row.seq);

    let models: Vec<ModelTime> = state
        .ch
        .query(&format!(
            "SELECT period, max(computed_at) AS computed_at FROM {} GROUP BY period",
            state.ch.table("rating_models")
        ))
        .await
        .map_err(ApiError::internal)?;
    let rankings = models
        .into_iter()
        .map(|m| (m.period, m.computed_at))
        .collect();

    Ok(Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        uptime_secs: now().saturating_sub(state.started_at),
        last_seq,
        latest_match_time: row.latest,
        matches_stored: row.count,
        rankings,
    }))
}

#[derive(Deserialize, Default)]
struct CountRow {
    count: u64,
    latest: u32,
    seq: Option<u64>,
}

#[derive(Deserialize)]
struct StateValue {
    value: String,
}

#[derive(Deserialize)]
struct ModelTime {
    period: String,
    computed_at: u32,
}

#[derive(Serialize)]
struct HeroesResponse {
    period: &'static str,
    computed_at: u32,
    patch: String,
    window_start: u32,
    matches: u64,
    converged: bool,
    radiant_advantage: f64,
    heroes: Vec<HeroDto>,
}

#[derive(Serialize, Clone)]
struct HeroDto {
    hero_id: u16,
    name: String,
    localized_name: String,
    icon: String,
    primary_attr: String,
    attack_type: String,
    roles: Vec<String>,
    rating: f64,
    rating_se: f64,
    games: u64,
    wins: u64,
    win_rate: f64,
    prev_rating: f64,
    delta: f64,
    rank: u16,
}

#[derive(Deserialize)]
struct RatingRow {
    computed_at: u32,
    hero_id: u16,
    rating: f64,
    rating_se: f64,
    games: u64,
    wins: u64,
    win_rate: f64,
    prev_rating: f64,
    delta: f64,
    rank: u16,
    #[serde(default)]
    name: String,
    #[serde(default)]
    localized_name: String,
    #[serde(default)]
    icon: String,
    #[serde(default)]
    primary_attr: String,
    #[serde(default)]
    attack_type: String,
    #[serde(default)]
    roles: Vec<String>,
}

impl From<RatingRow> for HeroDto {
    fn from(row: RatingRow) -> Self {
        Self {
            hero_id: row.hero_id,
            name: row.name,
            localized_name: row.localized_name,
            icon: row.icon,
            primary_attr: row.primary_attr,
            attack_type: row.attack_type,
            roles: row.roles,
            rating: row.rating,
            rating_se: row.rating_se,
            games: row.games,
            wins: row.wins,
            win_rate: row.win_rate,
            prev_rating: row.prev_rating,
            delta: row.delta,
            rank: row.rank,
        }
    }
}

#[derive(Deserialize)]
struct ModelRow {
    computed_at: u32,
    patch: String,
    window_start: u32,
    matches: u64,
    converged: bool,
    radiant_advantage: f64,
}

async fn heroes(
    State(state): State<AppState>,
    Query(query): Query<PeriodQuery>,
) -> Result<Json<HeroesResponse>, ApiError> {
    let period = parse_period(query.period.as_deref())?;
    let rows = latest_ratings(&state.ch, period, None)
        .await
        .map_err(ApiError::internal)?;
    let model = latest_model(&state.ch, period)
        .await
        .map_err(ApiError::internal)?;
    let computed_at = rows
        .first()
        .map(|r| r.computed_at)
        .or_else(|| model.as_ref().map(|m| m.computed_at))
        .unwrap_or(0);
    let heroes = rows.into_iter().map(HeroDto::from).collect();
    Ok(Json(HeroesResponse {
        period: period.as_str(),
        computed_at,
        patch: model.as_ref().map(|m| m.patch.clone()).unwrap_or_default(),
        window_start: model.as_ref().map(|m| m.window_start).unwrap_or(0),
        matches: model.as_ref().map(|m| m.matches).unwrap_or(0),
        converged: model.as_ref().map(|m| m.converged).unwrap_or(false),
        radiant_advantage: model.as_ref().map(|m| m.radiant_advantage).unwrap_or(0.0),
        heroes,
    }))
}

#[derive(Serialize)]
struct HeroDetailResponse {
    period: &'static str,
    computed_at: u32,
    hero: HeroDto,
    history: Vec<HistoryPoint>,
    beats: Vec<Counter>,
    loses_to: Vec<Counter>,
}

#[derive(Serialize)]
struct HistoryPoint {
    computed_at: u32,
    rating: f64,
    rating_se: f64,
    games: u64,
    wins: u64,
}

#[derive(Deserialize)]
struct HistoryRow {
    computed_at: u32,
    rating: f64,
    rating_se: f64,
    games: u64,
    wins: u64,
}

#[derive(Serialize, Clone)]
struct Counter {
    hero_id: u16,
    localized_name: String,
    icon: String,
    games: u64,
    wins: u64,
    win_rate: f64,
}

#[derive(Deserialize, Clone)]
struct PairRow {
    hero_lo: u16,
    hero_hi: u16,
    games: u64,
    lo_wins: u64,
}

#[derive(Deserialize)]
struct HeroMeta {
    hero_id: u16,
    localized_name: String,
    icon: String,
}

async fn hero_detail(
    State(state): State<AppState>,
    Path(hero_id): Path<u16>,
    Query(query): Query<HeroQuery>,
) -> Result<Json<HeroDetailResponse>, ApiError> {
    let period = parse_period(query.period.as_deref())?;
    let rows = latest_ratings(&state.ch, period, Some(hero_id))
        .await
        .map_err(ApiError::internal)?;
    let hero = rows
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::not_found("hero has no rating for this period"))?;
    let computed_at = hero.computed_at;
    let hero_dto = HeroDto::from(hero);

    let history: Vec<HistoryPoint> = state
        .ch
        .query::<HistoryRow>(&format!(
            "SELECT computed_at, rating, rating_se, games, wins
             FROM {}
             WHERE period = {} AND hero_id = {hero_id}
             ORDER BY computed_at DESC LIMIT 168",
            state.ch.table("hero_ratings"),
            quote_string(period.as_str())
        ))
        .await
        .map_err(ApiError::internal)?
        .into_iter()
        .map(|row| HistoryPoint {
            computed_at: row.computed_at,
            rating: row.rating,
            rating_se: row.rating_se,
            games: row.games,
            wins: row.wins,
        })
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    let min_games = query.min_games.unwrap_or(20).max(1);
    let pairs = pair_rows(&state, period).await?;
    let meta: HashMap<u16, HeroMeta> = hero_meta(&state.ch)
        .await
        .map_err(ApiError::internal)?
        .into_iter()
        .map(|meta| (meta.hero_id, meta))
        .collect();
    let mut counters = Vec::new();
    for pair in pairs.iter().filter(|p| p.games >= min_games) {
        let other = if pair.hero_lo == hero_id {
            pair.hero_hi
        } else if pair.hero_hi == hero_id {
            pair.hero_lo
        } else {
            continue;
        };
        let wins = if pair.hero_lo == hero_id {
            pair.lo_wins
        } else {
            pair.games.saturating_sub(pair.lo_wins)
        };
        let meta = meta.get(&other);
        counters.push(Counter {
            hero_id: other,
            localized_name: meta
                .map(|m| m.localized_name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("#{other}")),
            icon: meta.map(|m| m.icon.clone()).unwrap_or_default(),
            games: pair.games,
            wins,
            win_rate: (wins as f64 + 1.0) / (pair.games as f64 + 2.0),
        });
    }

    counters.sort_by(|a, b| {
        b.win_rate
            .partial_cmp(&a.win_rate)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let beats: Vec<Counter> = counters
        .iter()
        .filter(|counter| counter.win_rate > 0.5)
        .take(10)
        .cloned()
        .collect();
    let mut losing: Vec<Counter> = counters
        .iter()
        .filter(|counter| counter.win_rate < 0.5)
        .cloned()
        .collect();
    losing.sort_by(|a, b| {
        a.win_rate
            .partial_cmp(&b.win_rate)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let loses_to: Vec<Counter> = losing.into_iter().take(10).collect();

    Ok(Json(HeroDetailResponse {
        period: period.as_str(),
        computed_at,
        hero: hero_dto,
        history,
        beats,
        loses_to,
    }))
}

async fn latest_ratings(
    ch: &ClickHouse,
    period: Period,
    hero_id: Option<u16>,
) -> Result<Vec<RatingRow>> {
    let filter = hero_id
        .map(|id| format!(" AND r.hero_id = {id}"))
        .unwrap_or_default();
    let sql = format!(
        "SELECT r.computed_at, r.hero_id, r.rating, r.rating_se, r.games, r.wins,
                r.win_rate, r.prev_rating, r.delta, r.rank,
                h.name AS name, h.localized_name AS localized_name, h.icon AS icon,
                h.primary_attr AS primary_attr, h.attack_type AS attack_type,
                h.roles AS roles
         FROM {} AS r
         LEFT JOIN (
             SELECT hero_id,
                    argMax(name, updated_at) AS name,
                    argMax(localized_name, updated_at) AS localized_name,
                    argMax(icon, updated_at) AS icon,
                    argMax(primary_attr, updated_at) AS primary_attr,
                    argMax(attack_type, updated_at) AS attack_type,
                    argMax(roles, updated_at) AS roles
             FROM {} GROUP BY hero_id
         ) AS h ON h.hero_id = r.hero_id
         WHERE r.period = {} AND r.computed_at = (
             SELECT max(computed_at) FROM {} WHERE period = {}
         ){filter}
         ORDER BY r.rank ASC",
        ch.table("hero_ratings"),
        ch.table("heroes"),
        quote_string(period.as_str()),
        ch.table("hero_ratings"),
        quote_string(period.as_str())
    );
    ch.query(&sql).await
}

async fn latest_model(ch: &ClickHouse, period: Period) -> Result<Option<ModelRow>> {
    let rows: Vec<ModelRow> = ch
        .query(&format!(
            "SELECT computed_at, patch, window_start, matches, converged, radiant_advantage
             FROM {} WHERE period = {} ORDER BY computed_at DESC LIMIT 1",
            ch.table("rating_models"),
            quote_string(period.as_str())
        ))
        .await?;
    Ok(rows.into_iter().next())
}

#[derive(Serialize)]
struct MatchupsResponse {
    period: &'static str,
    pairs: Vec<MatchupDto>,
}

#[derive(Serialize)]
struct MatchupDto {
    a: u16,
    b: u16,
    games: u64,
    a_wins: u64,
    a_win_rate: f64,
}

async fn matchups(
    State(state): State<AppState>,
    Query(query): Query<HeroQuery>,
) -> Result<Json<MatchupsResponse>, ApiError> {
    let period = parse_period(query.period.as_deref())?;
    let min_games = query.min_games.unwrap_or(20).max(1);
    let mut pairs: Vec<MatchupDto> = pair_rows(&state, period)
        .await?
        .into_iter()
        .filter(|p| p.games >= min_games)
        .map(|p| MatchupDto {
            a: p.hero_lo,
            b: p.hero_hi,
            games: p.games,
            a_wins: p.lo_wins,
            a_win_rate: (p.lo_wins as f64 + 1.0) / (p.games as f64 + 2.0),
        })
        .collect();
    pairs.sort_by_key(|p| std::cmp::Reverse(p.games));
    pairs.truncate(5_000);
    Ok(Json(MatchupsResponse {
        period: period.as_str(),
        pairs,
    }))
}

async fn pair_rows(state: &AppState, period: Period) -> Result<Vec<PairRow>, ApiError> {
    if let Some(cached) = state.cache.read().await.get(&period)
        && cached.at.elapsed() < PAIR_CACHE_TTL
    {
        return Ok(cached.pairs.clone());
    }

    let now = now();
    let mut since = now.saturating_sub(period.seconds());
    if let Some(patch_start) = state.patch_start {
        since = since.max(patch_start);
    }
    let since_hour = since / 3600 * 3600;
    let rows: Vec<PairRow> = state
        .ch
        .query(&format!(
            "SELECT hero_lo, hero_hi,
                    sum(games) AS games,
                    sum(lo_wins) AS lo_wins
             FROM {}
             WHERE hour >= {since_hour}
             GROUP BY hero_lo, hero_hi",
            state.ch.table("hero_matchups_hourly")
        ))
        .await
        .map_err(ApiError::internal)?;
    state.cache.write().await.insert(
        period,
        CachedPairs {
            at: Instant::now(),
            pairs: rows.clone(),
        },
    );
    Ok(rows)
}

async fn hero_meta(ch: &ClickHouse) -> Result<Vec<HeroMeta>> {
    ch.query(&format!(
        "SELECT hero_id,
                argMax(localized_name, updated_at) AS localized_name,
                argMax(icon, updated_at) AS icon
         FROM {} GROUP BY hero_id",
        ch.table("heroes")
    ))
    .await
}

#[derive(Serialize)]
struct MatchesResponse {
    matches: Vec<MatchDto>,
}

#[derive(Serialize)]
struct MatchDto {
    match_id: String,
    start_time: u32,
    duration: u32,
    radiant_win: bool,
    radiant_score: u16,
    dire_score: u16,
    avg_rank_tier: u16,
    radiant_heroes: Vec<MatchHero>,
    dire_heroes: Vec<MatchHero>,
}

#[derive(Serialize)]
struct MatchHero {
    hero_id: u16,
    localized_name: String,
    icon: String,
}

#[derive(Deserialize)]
struct MatchRow {
    match_id: u64,
    start_time: u32,
    duration: u32,
    radiant_win: bool,
    radiant_score: u16,
    dire_score: u16,
    avg_rank_tier: u16,
    radiant_heroes: Vec<u16>,
    dire_heroes: Vec<u16>,
}

async fn recent_matches(
    State(state): State<AppState>,
    Query(query): Query<MatchesQuery>,
) -> Result<Json<MatchesResponse>, ApiError> {
    let limit = query.limit.unwrap_or(30).clamp(1, 200);
    let rows: Vec<MatchRow> = state
        .ch
        .query(&format!(
            "SELECT match_id, start_time, duration, radiant_win, radiant_score,
                    dire_score, avg_rank_tier, radiant_heroes, dire_heroes
             FROM {}
             ORDER BY start_time DESC LIMIT {limit}",
            state.ch.table("matches")
        ))
        .await
        .map_err(ApiError::internal)?;
    let meta: HashMap<u16, HeroMeta> = hero_meta(&state.ch)
        .await
        .map_err(ApiError::internal)?
        .into_iter()
        .map(|meta| (meta.hero_id, meta))
        .collect();
    let to_hero = |id: u16| {
        let meta = meta.get(&id);
        MatchHero {
            hero_id: id,
            localized_name: meta
                .map(|m| m.localized_name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("#{id}")),
            icon: meta.map(|m| m.icon.clone()).unwrap_or_default(),
        }
    };
    let matches = rows
        .into_iter()
        .map(|row| MatchDto {
            match_id: row.match_id.to_string(),
            start_time: row.start_time,
            duration: row.duration,
            radiant_win: row.radiant_win,
            radiant_score: row.radiant_score,
            dire_score: row.dire_score,
            avg_rank_tier: row.avg_rank_tier,
            radiant_heroes: row.radiant_heroes.into_iter().map(to_hero).collect(),
            dire_heroes: row.dire_heroes.into_iter().map(to_hero).collect(),
        })
        .collect();
    Ok(Json(MatchesResponse { matches }))
}

fn parse_period(value: Option<&str>) -> Result<Period, ApiError> {
    let value = value.unwrap_or("day");
    Period::parse(value).ok_or_else(|| ApiError::bad_request("period must be day, week, or month"))
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
    source: Option<anyhow::Error>,
}

impl ApiError {
    fn internal(error: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "internal server error".into(),
            source: Some(error),
        }
    }

    fn bad_request(message: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            source: None,
        }
    }

    fn not_found(message: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
            source: None,
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let Some(source) = &self.source {
            error!(error = %source, "request failed");
        }
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

fn now() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}
