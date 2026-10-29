use crate::ch::{ClickHouse, quote_string};
use crate::hero_data;
use crate::period::Period;
use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderName, HeaderValue, StatusCode, header},
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
use tower_http::{
    compression::CompressionLayer, set_header::SetResponseHeaderLayer, timeout::TimeoutLayer,
    trace::TraceLayer,
};
use tracing::{error, info, warn};

const INDEX_HTML: &str = include_str!("../assets/index.html");
const APP_JS: &str = include_str!("../assets/app.js");
const STYLE_CSS: &str = include_str!("../assets/style.css");
const PAIR_CACHE_TTL: Duration = Duration::from_secs(60);
const HERO_META_CACHE_TTL: Duration = Duration::from_secs(300);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct AppState {
    pub ch: ClickHouse,
    pub started_at: u32,
    cache: Arc<RwLock<HashMap<Period, CachedPairs>>>,
    meta_cache: Arc<RwLock<Option<CachedHeroMeta>>>,
}

struct CachedPairs {
    at: Instant,
    window_start: u32,
    window_end: u32,
    pairs: Vec<PairRow>,
}

struct CachedHeroMeta {
    at: Instant,
    meta: Arc<HashMap<u16, HeroMeta>>,
}

impl AppState {
    pub fn new(ch: ClickHouse) -> Self {
        Self {
            ch,
            started_at: now(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            meta_cache: Arc::new(RwLock::new(None)),
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
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("x-frame-options"),
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("referrer-policy"),
            HeaderValue::from_static("no-referrer"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static(
                "default-src 'self'; img-src 'self' data: https://cdn.cloudflare.steamstatic.com; \
                 style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'; \
                 frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
            ),
        ))
        .with_state(state)
}

pub async fn serve(state: AppState, bind: SocketAddr, ct: CancellationToken) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!(%bind, "web server listening");
    let server = axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { ct.cancelled().await });
    match tokio::time::timeout(SHUTDOWN_TIMEOUT, server).await {
        Ok(result) => result?,
        Err(_) => warn!("web server shutdown timed out; forcing exit"),
    }
    Ok(())
}

async fn index() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(INDEX_HTML))
}

async fn app_js() -> impl IntoResponse {
    (
        [
            (
                header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        APP_JS,
    )
}

async fn style_css() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
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

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    uptime_secs: u32,
    last_seq: Option<u64>,
    latest_match_time: u32,
    matches_stored: u64,
    heroes_loaded: u32,
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

    let db_heroes = state
        .ch
        .query::<HeroCount>(&format!(
            "SELECT uniqExact(hero_id) AS heroes FROM {}",
            state.ch.table("heroes")
        ))
        .await
        .map(|rows| rows.first().map(|row| row.heroes).unwrap_or(0))
        .unwrap_or_else(|error| {
            warn!(%error, "failed to count ClickHouse hero metadata; using hardcoded fallback");
            0
        });
    let heroes_loaded = db_heroes.max(hero_data::HEROES.len() as u32);

    Ok(Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        uptime_secs: now().saturating_sub(state.started_at),
        last_seq,
        latest_match_time: row.latest,
        matches_stored: row.count,
        heroes_loaded,
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

#[derive(Deserialize)]
struct HeroCount {
    heroes: u32,
}

#[derive(Serialize)]
struct HeroesResponse {
    period: &'static str,
    computed_at: u32,
    patch: String,
    window_start: u32,
    window_end: u32,
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
    icon_url: String,
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

fn hero_dto(row: RatingRow, meta: Option<&HeroMeta>) -> HeroDto {
    let icon = meta.map(|m| m.icon.clone()).unwrap_or_default();
    let localized_name = meta
        .map(|m| m.localized_name.clone())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| format!("#{}", row.hero_id));
    HeroDto {
        hero_id: row.hero_id,
        name: meta.map(|m| m.name.clone()).unwrap_or_default(),
        localized_name,
        icon_url: icon_url(&icon),
        icon,
        primary_attr: meta.map(|m| m.primary_attr.clone()).unwrap_or_default(),
        attack_type: meta.map(|m| m.attack_type.clone()).unwrap_or_default(),
        roles: meta.map(|m| m.roles.clone()).unwrap_or_default(),
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

#[derive(Deserialize)]
struct ModelRow {
    computed_at: u32,
    patch: String,
    window_start: u32,
    window_end: u32,
    matches: u64,
    converged: bool,
    radiant_advantage: f64,
}

async fn heroes(
    State(state): State<AppState>,
    Query(query): Query<PeriodQuery>,
) -> Result<Json<HeroesResponse>, ApiError> {
    let period = parse_period(query.period.as_deref())?;
    let Some(model) = latest_model(&state.ch, period)
        .await
        .map_err(ApiError::internal)?
    else {
        return Ok(Json(HeroesResponse {
            period: period.as_str(),
            computed_at: 0,
            patch: String::new(),
            window_start: 0,
            window_end: 0,
            matches: 0,
            converged: false,
            radiant_advantage: 0.0,
            heroes: Vec::new(),
        }));
    };
    let rows = latest_ratings(&state.ch, period, model.computed_at, &model.patch, None)
        .await
        .map_err(ApiError::internal)?;
    let meta = hero_meta(&state).await;
    let heroes = rows
        .into_iter()
        .map(|row| {
            let hero_id = row.hero_id;
            hero_dto(row, meta.get(&hero_id))
        })
        .collect();
    Ok(Json(HeroesResponse {
        period: period.as_str(),
        computed_at: model.computed_at,
        patch: model.patch.clone(),
        window_start: model.window_start,
        window_end: effective_window_end(&model),
        matches: model.matches,
        converged: model.converged,
        radiant_advantage: model.radiant_advantage,
        heroes,
    }))
}

#[derive(Serialize)]
struct HeroDetailResponse {
    period: &'static str,
    computed_at: u32,
    window_start: u32,
    window_end: u32,
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
    icon_url: String,
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

#[derive(Deserialize, Clone)]
struct HeroMeta {
    hero_id: u16,
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

async fn hero_detail(
    State(state): State<AppState>,
    Path(hero_id): Path<u16>,
    Query(query): Query<HeroQuery>,
) -> Result<Json<HeroDetailResponse>, ApiError> {
    let period = parse_period(query.period.as_deref())?;
    let model = latest_model(&state.ch, period)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("hero has no rating for this period"))?;
    let rows = latest_ratings(
        &state.ch,
        period,
        model.computed_at,
        &model.patch,
        Some(hero_id),
    )
    .await
    .map_err(ApiError::internal)?;
    let meta = hero_meta(&state).await;
    let hero = rows
        .into_iter()
        .next()
        .ok_or_else(|| ApiError::not_found("hero has no rating for this period"))?;
    let computed_at = model.computed_at;
    let window_end = effective_window_end(&model);
    let hero_dto = hero_dto(hero, meta.get(&hero_id));

    let history: Vec<HistoryPoint> = state
        .ch
        .query::<HistoryRow>(&format!(
            "SELECT computed_at, rating, rating_se, games, wins
             FROM {}
             WHERE period = {} AND patch = {} AND hero_id = {hero_id}
               AND computed_at <= {computed_at}
             ORDER BY computed_at DESC LIMIT 168",
            state.ch.table("hero_ratings"),
            quote_string(period.as_str()),
            quote_string(&model.patch)
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
    let pairs = pair_rows(&state, period, model.window_start, window_end).await?;
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
        let icon = meta.map(|m| m.icon.clone()).unwrap_or_default();
        counters.push(Counter {
            hero_id: other,
            localized_name: meta
                .map(|m| m.localized_name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("#{other}")),
            icon_url: icon_url(&icon),
            icon,
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
        window_start: model.window_start,
        window_end,
        hero: hero_dto,
        history,
        beats,
        loses_to,
    }))
}

fn effective_window_end(model: &ModelRow) -> u32 {
    if model.window_end == 0 {
        model.computed_at
    } else {
        model.window_end
    }
}

async fn latest_ratings(
    ch: &ClickHouse,
    period: Period,
    computed_at: u32,
    patch: &str,
    hero_id: Option<u16>,
) -> Result<Vec<RatingRow>> {
    let filter = hero_id
        .map(|id| format!(" AND hero_id = {id}"))
        .unwrap_or_default();
    let sql = format!(
        "SELECT hero_id, rating, rating_se, games, wins,
                win_rate, prev_rating, delta, rank
         FROM {}
         WHERE period = {} AND patch = {} AND computed_at = {computed_at}{filter}
         ORDER BY rank ASC",
        ch.table("hero_ratings"),
        quote_string(period.as_str()),
        quote_string(patch)
    );
    ch.query(&sql).await
}

async fn latest_model(ch: &ClickHouse, period: Period) -> Result<Option<ModelRow>> {
    let rows: Vec<ModelRow> = ch
        .query(&format!(
            "SELECT computed_at, patch, window_start, window_end, matches, converged, radiant_advantage
             FROM {} WHERE period = {} ORDER BY computed_at DESC LIMIT 1",
            ch.table("rating_models"),
            quote_string(period.as_str())
        ))
        .await?;
    Ok(rows.into_iter().next())
}

async fn pair_rows(
    state: &AppState,
    period: Period,
    window_start: u32,
    window_end: u32,
) -> Result<Vec<PairRow>, ApiError> {
    if let Some(cached) = state.cache.read().await.get(&period)
        && cached.at.elapsed() < PAIR_CACHE_TTL
        && cached.window_start == window_start
        && cached.window_end == window_end
    {
        return Ok(cached.pairs.clone());
    }

    let since_hour = window_start / 3600 * 3600;
    let end_hour = window_end / 3600 * 3600;
    let rows: Vec<PairRow> = state
        .ch
        .query(&format!(
            "SELECT hero_lo, hero_hi,
                    sum(games) AS games,
                    sum(lo_wins) AS lo_wins
             FROM {}
             WHERE hour >= {since_hour} AND hour <= {end_hour}
             GROUP BY hero_lo, hero_hi",
            state.ch.table("hero_matchups_hourly")
        ))
        .await
        .map_err(ApiError::internal)?;
    state.cache.write().await.insert(
        period,
        CachedPairs {
            at: Instant::now(),
            window_start,
            window_end,
            pairs: rows.clone(),
        },
    );
    Ok(rows)
}

fn icon_url(icon: &str) -> String {
    let slug: String = icon
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .collect();
    if slug.is_empty() {
        String::new()
    } else {
        format!(
            "https://cdn.cloudflare.steamstatic.com/apps/dota2/images/dota_react/heroes/{slug}.png"
        )
    }
}

fn fallback_hero_meta() -> HashMap<u16, HeroMeta> {
    hero_data::HEROES
        .iter()
        .map(|hero| {
            (
                hero.id,
                HeroMeta {
                    hero_id: hero.id,
                    name: hero.name.to_owned(),
                    localized_name: hero.localized_name.to_owned(),
                    icon: hero.icon.to_owned(),
                    primary_attr: hero.primary_attr.to_owned(),
                    attack_type: hero.attack_type.to_owned(),
                    roles: hero.roles.iter().map(|role| (*role).to_owned()).collect(),
                },
            )
        })
        .collect()
}

fn merge_hero_meta(map: &mut HashMap<u16, HeroMeta>, row: HeroMeta) {
    if let Some(existing) = map.get_mut(&row.hero_id) {
        if !row.name.is_empty() {
            existing.name = row.name;
        }
        if !row.localized_name.is_empty() {
            existing.localized_name = row.localized_name;
        }
        if !row.icon.is_empty() {
            existing.icon = row.icon;
        }
        if !row.primary_attr.is_empty() {
            existing.primary_attr = row.primary_attr;
        }
        if !row.attack_type.is_empty() {
            existing.attack_type = row.attack_type;
        }
        if !row.roles.is_empty() {
            existing.roles = row.roles;
        }
    } else {
        map.insert(row.hero_id, row);
    }
}

async fn hero_meta(state: &AppState) -> Arc<HashMap<u16, HeroMeta>> {
    if let Some(cached) = state.meta_cache.read().await.as_ref()
        && cached.at.elapsed() < HERO_META_CACHE_TTL
    {
        return cached.meta.clone();
    }

    let mut heroes = fallback_hero_meta();
    match state
        .ch
        .query::<HeroMeta>(&format!(
            "SELECT hero_id, name, localized_name, icon, primary_attr, attack_type, roles
             FROM {} FINAL",
            state.ch.table("heroes")
        ))
        .await
    {
        Ok(rows) => {
            for row in rows {
                merge_hero_meta(&mut heroes, row);
            }
        }
        Err(error) => {
            warn!(%error, "failed to load hero metadata from ClickHouse; using hardcoded fallback");
        }
    }
    let meta = Arc::new(heroes);
    *state.meta_cache.write().await = Some(CachedHeroMeta {
        at: Instant::now(),
        meta: meta.clone(),
    });
    meta
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_icon_slugs() {
        assert_eq!(
            icon_url("antimage"),
            "https://cdn.cloudflare.steamstatic.com/apps/dota2/images/dota_react/heroes/antimage.png"
        );
        assert!(icon_url("bad\"slug").ends_with("/badslug.png"));
        assert!(!icon_url("bad\"slug").contains('"'));
        assert_eq!(icon_url(""), "");
    }

    #[test]
    fn old_models_fall_back_to_computed_at_as_window_end() {
        let model = ModelRow {
            computed_at: 2_000,
            patch: "7.40".into(),
            window_start: 1_000,
            window_end: 0,
            matches: 10,
            converged: true,
            radiant_advantage: 0.5,
        };
        assert_eq!(effective_window_end(&model), 2_000);
    }
}
