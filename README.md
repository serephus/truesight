# Truesight

Dota 2 hero strength ratings from the public ranked match stream.

One process does everything: it tails Steam's public match sequence, stores full
match details in ClickHouse, fits day/week/month Bradley–Terry ratings, and
serves a JSON API plus an embedded frontend. There are no subcommands; all
loops run concurrently and shut down together on SIGTERM/Ctrl-C.

```
Steam Web API ──► collector ──► ClickHouse ──► ranker ──► hero_ratings
                     │              │                         │
                     └── checkpoint └── hourly aggregates     └──► axum API / UI
```

## What it collects

- Public **ranked All Pick** matches (`lobby_type = 7`, `game_mode = 22`), no
  skill-bracket filter.
- No historical backfill. On first boot the collector starts at the newest
  public match sequence. After a restart it resumes from its persisted checkpoint,
  which fills the downtime gap.
- Every ranked match is stored in full (`raw` JSON, ZSTD-compressed), plus
  extracted columns and a `match_players` row per player for future features.
- Rating uses games with 10 players, at least 10 minutes of duration, and no
  abandons. All matches are still stored.
- Multiple Steam keys are rotated automatically with per-key cooldowns on
  429/5xx/invalid-key responses.

The collector tracks the live stream with `GetMatchHistory` and adapts its
request rate to Valve's rate limit.

- Every ~30 s it fetches the newest public match sequence and estimates:
  - `lag_matches = head_seq - cursor`
  - the stream rate in matches/second from consecutive head samples
  - `lag_seconds = lag_matches / stream_rate`
- The request governor works in requests/second:
  - initial rate = `1 / TRUESIGHT_POLL_INTERVAL_MS`
  - floor = `1 / TRUESIGHT_MAX_POLL_INTERVAL_MS`
  - ceiling = `1 / TRUESIGHT_MIN_POLL_INTERVAL_MS`
- While behind (`head_seq > cursor`), it runs at the current rate ceiling.
- When caught up, it slows to the observed stream rate divided by batch size.
- On HTTP 429, it halves the current rate, lowers a durable ceiling to 80% of
  the rate that triggered the 429, and enforces a 30 s cooldown.
- After the cooldown, while still behind, the current rate probes upward by 5%
  per successful batch, but only up to that durable ceiling.
- After 5 minutes with no 429, the durable ceiling itself recovers by 1%; this
  repeats every quiet 5-minute period, up to the interval-derived maximum.
  Recovery is intentionally much slower than the backoff.
- If no 429 has occurred, the ceiling is the interval-derived maximum.
- It never issues requests faster than the current ceiling, and all Steam
  requests (head + history) share the same gate.
- Non-429 failures use a 5/10/20/40/60 s backoff; per-key cooldowns still apply.

The effective match rate is `request_rate * batch_size * 60` matches/minute.
With the defaults (`5000ms`, batch `100`), the initial rate is 0.2 req/s
(1,200 matches/min); the ceiling is 0.5 req/s (3,000 matches/min).

## Rating model

For each window (24h, 7d, 30d), Truesight fits a Bradley–Terry model over the 25
cross-team hero pairs in every match. Windows are anchored at the newest
collected match's `start_time`, not wall-clock `now`:

- the window is `[anchor - period, anchor]`
- patch clipping is applied to the start
- time decay is relative to `anchor`, so collector lag does not age the window
- `computed_at` remains the time the snapshot was fitted

This keeps a full day/week/month of collected data even while the collector is
catching up, instead of leaving a partially empty window at the live edge.

```
P(hero_a beats hero_b) = sigmoid(r_a - r_b + gamma * radiant_side)
```

- `r_h` is the latent hero strength; `gamma` is the radiant advantage.
- L2 regularization shrinks low-sample heroes towards the mean and makes the
  model identifiable.
- Matches are weighted with an exponential time decay (half-life = window / 2).
- Standard errors come from the diagonal of the inverse Hessian (Laplace
  approximation).
- Each snapshot stores the previous rating and the delta, so the UI can show
  rising/falling heroes.
- If `TRUESIGHT_PATCH_START` is set, every window is clipped to that timestamp,
  so ratings never mix patches. Snapshots are labelled with
  `TRUESIGHT_PATCH_NAME`.

The hero detail modal shows raw matchup counters and the "beats / loses to"
lists, derived from the same hourly aggregate table with Beta(1,1) smoothing.

## Quick start (Nix)

```bash
nix develop
mkdir -p .clickhouse

# Terminal 1: local ClickHouse
clickhouse server -- \
  --path="$PWD/.clickhouse/" \
  --tmp_path="$PWD/.clickhouse/tmp/" \
  --http_port=8123 --tcp_port=9000

# Terminal 2: run Truesight
export TRUESIGHT_STEAM_API_KEYS="key1,key2,key3"
cargo run
```

Open <http://127.0.0.1:8080>.

To develop the API/UI without Steam keys:

```bash
TRUESIGHT_COLLECTOR_ENABLED=false TRUESIGHT_RANKER_ENABLED=false cargo run
```

The schema is created automatically at startup.

## Configuration

Everything is environment-driven (no CLI flags).

| Variable | Default | Meaning |
|---|---|---|
| `TRUESIGHT_BIND` | `127.0.0.1:8080` | HTTP listen address |
| `TRUESIGHT_LOG` / `RUST_LOG` | `info,truesight=debug` | tracing filter |
| `TRUESIGHT_STEAM_API_KEYS` | — | Comma/whitespace-separated keys |
| `TRUESIGHT_STEAM_API_KEYS_FILE` | — | File containing keys |
| `TRUESIGHT_STEAM_BASE_URL` | `https://api.steampowered.com` | Override for tests |
| `TRUESIGHT_COLLECTOR_ENABLED` | `true` | Toggle ingestion |
| `TRUESIGHT_POLL_INTERVAL_MS` | `5000` | Initial request interval; derives the initial rate |
| `TRUESIGHT_MIN_POLL_INTERVAL_MS` | `2000` | Fastest interval; becomes the rate ceiling |
| `TRUESIGHT_MAX_POLL_INTERVAL_MS` | `30000` | Slowest interval; becomes the rate floor |
| `TRUESIGHT_BATCH_SIZE` | `100` | Matches per Steam call (max 100) |
| `TRUESIGHT_START_SEQ` | latest | First-boot sequence override |
| `TRUESIGHT_CLICKHOUSE_URL` | `http://127.0.0.1:8123` | ClickHouse HTTP endpoint |
| `TRUESIGHT_CLICKHOUSE_DATABASE` | `truesight` | Database name |
| `TRUESIGHT_CLICKHOUSE_USER` | `default` | ClickHouse user |
| `TRUESIGHT_CLICKHOUSE_PASSWORD` | empty | ClickHouse password |
| `TRUESIGHT_RANKER_ENABLED` | `true` | Toggle rating fits |
| `TRUESIGHT_RANK_INTERVAL_SECS` | `300` | Seconds between fits |
| `TRUESIGHT_REGULARIZATION` | `1.0` | L2 lambda |
| `TRUESIGHT_MIN_RATED_GAMES` | `1` | Minimum games published |
| `TRUESIGHT_PATCH_START` | unset | Unix timestamp of current patch |
| `TRUESIGHT_PATCH_NAME` | `current` | Patch label |
| `TRUESIGHT_RAW_RETENTION_DAYS` | `0` | Drop raw JSON column after N days; 0 = keep |

Secrets can also be supplied through systemd credentials: if
`$CREDENTIALS_DIRECTORY` exists, Truesight reads `steam_api_keys` and
`clickhouse_password` from it.

### Log redaction

Tracing output is wrapped in a redacting writer:

- every configured Steam API key and the ClickHouse password are replaced with
  `[REDACTED]`;
- values of sensitive query parameters (`key`, `password`, `token`, `secret`,
  etc.) are masked even if the value was not registered up front;
- internal `reqwest` URLs are stripped from request errors.

This applies to dependency logs as well. Raw match data is **not** redacted:
`matches.raw` and `match_players` contain `account_id` when present, but not
player names — `GetMatchHistoryBySequenceNum` does not return names. If you
need account IDs dropped or hashed at rest, that is a separate data change.

## NixOS module

The flake exports `nixosModules.default`:

```nix
{
  inputs.truesight.url = "github:serephus/truesight";

  outputs = { self, nixpkgs, truesight, ... }: {
    nixosConfigurations.host = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        truesight.nixosModules.default
        {
          services.truesight = {
            enable = true;
            bind = "0.0.0.0";          # or keep localhost and enable nginx
            port = 8080;
            openFirewall = true;
            steamApiKeysFile = "/run/secrets/steam_api_keys";

            patch = {
              name = "7.40";
              start = 1760000000;       # unix timestamp
            };

            # Local managed ClickHouse (default). Set enable = false to use an
            # external instance and configure `clickhouse.url`.
            clickhouse.enable = true;

            # Optional reverse proxy with TLS:
            # nginx.enable = true;
            # nginx.serverName = "dota.example.com";
            # nginx.enableACME = true;
            # nginx.forceSSL = true;
          };
        }
      ];
    };
  };
}
```

The module creates a `truesight` system user, starts a local ClickHouse server
(listening on 127.0.0.1), wires dependencies/restarts, loads secrets through
systemd credentials, and applies systemd hardening. The ClickHouse data lives in
`/var/lib/clickhouse`.

## HTTP API

| Endpoint | Description |
|---|---|
| `GET /api/health` | Uptime, checkpoint, stored match count, snapshot times |
| `GET /api/heroes?period=day\|week\|month` | Latest hero leaderboard |
| `GET /api/heroes/{hero_id}?period=…&min_games=20` | Hero detail, history, counters |

Hero responses include the anchored `window_start` and `window_end` timestamps.

The frontend is embedded in the binary (`assets/`), so no Node build step or
static file deployment is required. The UI is a single leaderboard with search,
day/week/month switching, and a hero detail modal. Hero objects include `localized_name`,
`icon`, `icon_url`, `primary_attr`, `attack_type`, and `roles`, loaded from
Steam's `IEconDOTA2_570/GetHeroes` endpoint. Hero metadata is refreshed even
when the match collector is disabled, as long as Steam keys are configured.

A compiled-in snapshot of every hero is also used as a fallback: missing rows
are seeded into the `heroes` table at startup, and the API merges the fallback
with ClickHouse metadata on read. A Steam refresh always takes precedence.

## ClickHouse tables

| Table | Engine | Purpose |
|---|---|---|
| `matches` | `ReplacingMergeTree` | Full match rows + raw JSON |
| `match_players` | `ReplacingMergeTree` | Per-player detail (items, GPM/XPM, damage, …) |
| `hero_matchups_hourly` | `SummingMergeTree` | Cross-team pair counts per hour |
| `hero_stats_hourly` | `SummingMergeTree` | Hero wins/games per hour |
| `heroes` | `ReplacingMergeTree` | Hero metadata from Steam |
| `collector_state` | `ReplacingMergeTree` | Sequence checkpoint |
| `hero_ratings` | `MergeTree` | Rating snapshots per period |
| `rating_models` | `MergeTree` | Fit metadata (radiant edge, iterations, …) |

Inserts are idempotent for crash replay: each batch uses a deterministic
`insert_deduplication_token` derived from its sequence range.

## Operational notes

- A full ranked stream is roughly 1M matches/day. Hourly pre-aggregation keeps
  rating fits cheap: a month fit scans a few million aggregate rows, not the raw
  player table.
- Raw JSON is the dominant storage cost (roughly 10–20 GB/day before
  compression). Set `TRUESIGHT_RAW_RETENTION_DAYS` if you only need recent
  payloads; structured rows and hourly aggregates are kept.
- Windows are clipped to the current patch when `TRUESIGHT_PATCH_START` is set.
  Patch boundaries are approximate to the hour.
- The API is read-only and unauthenticated. Keep the default localhost bind and
  put nginx in front for public deployments.
- Steam's sequence endpoint is global; unranked matches are skipped but still
  advance the checkpoint, so the collector never gets stuck on them.

## Model limitations

- Hero strength is an average effect. Same-team synergy and player skill are not
  modelled yet; the pair pseudo-likelihood also makes the displayed standard
  errors indicative rather than exact match-level confidence intervals.
- Patch boundaries are applied at the hourly aggregate granularity.
- The schema is created idempotently at boot, but there is no migration
  framework yet. Recreate the database after incompatible schema changes.
- `TRUESIGHT_RAW_RETENTION_DAYS` is applied when the `matches` table is created;
  changing it later does not retrofit a column TTL.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
nix build
```

## License

MIT.
