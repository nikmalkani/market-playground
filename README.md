# Market Playground: SPX Strategy Analyzer

Market Playground is a Python project for collecting SPX spot and options snapshot data from tastytrade, storing them in SQL, and exploring historical strategy behavior through local backtest and analysis UIs.

The repo has three main pieces:

- a collector that pulls SPX market and options data on a schedule
- local HTTP apps for development and staging analysis
- a production-oriented HTTP app that can be served behind a reverse proxy

## What It Does

On each collection run, the app:

- inserts one market snapshot row for `SPX` into `spx_market_snapshots`
- inserts SPX option contract snapshot rows into `spx_option_snapshots`

The project is designed for historical snapshot collection and strategy analysis rather than live trading.

## Quick Start

Requirements:

- Python `3.11+`
- tastytrade credentials with a refresh token

Setup:

```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
```

Then fill in `.env` with at least:

- `TASTYTRADE_CLIENT_SECRET`
- `TASTYTRADE_REFRESH_TOKEN`

Common runtime settings:

- `DB_URL` default: `sqlite:///spx_options.db`
- `UNDERLYING_SYMBOL` default: `SPX`
- `OPTION_EXPIRIES_PER_RUN` default: `2`
- `OPTION_STRIKES_COUNT` default: `140`
- `OPTIONS_STREAM_TIMEOUT_SECONDS` default: `20`
- `COLLECTOR_LOG_LEVEL` default: `INFO`

## Running The Collector

Run one collection pass:

```bash
spx-collector run-once
```

Run the scheduler daemon:

```bash
spx-collector daemon
```

Run auth and spot diagnostics:

```bash
spx-collector diagnose-spot
```

Run options-only collection:

```bash
spx-collector run-options-only
```

## Scheduler Window

The scheduler only runs during this window:

- Monday through Friday
- every 15 minutes
- `06:00 <= Pacific time < 14:00`

`run-once` uses the same time-window check. For an off-hours test, use the forced snapshot command below.

## Running The UIs

App roles:

- `src/spx_collector/backtest_dev.py`: local dev UI, default port `8787`
- `src/spx_collector/backtest_staging.py`: local staging UI, default port `8788`
- `src/spx_collector/backtest_prod.py`: production-oriented UI, default port `8789`

Start them with:

```bash
PYTHONPATH=src python -m spx_collector.backtest_dev
PYTHONPATH=src python -m spx_collector.backtest_staging
PYTHONPATH=src python -m spx_collector.backtest_prod
```

All three apps read through Python HTTP handlers. The browser does not connect to SQLite directly.

## Data Model

`spx_market_snapshots` stores:

- `snapshot_ts`, `symbol`
- `spot_price`, `bid_price`, `ask_price`, `last_price`
- `market_data_updated_at`, `metrics_updated_at`
- `implied_volatility_index`, `implied_volatility_30_day`, `historical_volatility_30_day`

`spx_option_snapshots` stores:

- `snapshot_ts`, `symbol`, `streamer_symbol`
- `expiration_date`, `strike_price`, `option_type`
- `dte`
- `time_in_day_est`
- `bid_price`, `ask_price`, `mid_price`
- `volatility`, `delta`, `gamma`, `theta`, `vega`

Notes:

- `dte` is computed as `max(0, expiration_date - date(snapshot_ts))`
- `time_in_day_est` is stored in US Eastern time for easier intraday grouping
- SQLite startup migration auto-adds and backfills these columns for existing DBs

## SQL Checks

List tables:

```sql
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
```

Count rows:

```sql
SELECT COUNT(*) FROM spx_market_snapshots;
SELECT COUNT(*) FROM spx_option_snapshots;
```

View recent market rows:

```sql
SELECT snapshot_ts, symbol, spot_price
FROM spx_market_snapshots
ORDER BY snapshot_ts DESC
LIMIT 10;
```

View recent option rows:

```sql
SELECT snapshot_ts, expiration_date, strike_price, option_type, bid_price, ask_price, delta
FROM spx_option_snapshots
ORDER BY snapshot_ts DESC, expiration_date, strike_price, option_type
LIMIT 50;
```

Export options to CSV:

```bash
sqlite3 -header -csv spx_options.db "SELECT * FROM spx_option_snapshots ORDER BY snapshot_ts DESC, expiration_date, strike_price, option_type;" > spx_option_snapshots.csv
```

## Forced Snapshot Test

This bypasses the scheduler window and runs a full collection pass once:

```bash
python -c "import asyncio; from spx_collector.config import Settings; from spx_collector.db import build_session_factory; from spx_collector.collector import SPXCollector; s=Settings(); sf=build_session_factory(s.db_url); db=sf(); n=asyncio.run(SPXCollector(s).run_snapshot(db)); db.close(); print('FORCED_INSERTED=', n)"
```

## SQLite Backup

The backup timer maintains one file at `backups/sqlite/spx_options.db.backup`. The first run creates a consistent SQLite backup; later runs append rows with IDs newer than the backup's high-water mark in each snapshot table. It does not create a full dated copy every day. The snapshot tables are append-only; schema mismatches fail loudly rather than producing a misleading backup.

Run it manually with:

```bash
./scripts/backup_sqlite.sh
```

Keep the backup file on a separate disk or object store for disaster recovery. This local single-file mirror is not a substitute for an off-host backup.

## Deployment Shape

The public deployment path looks like this:

`Browser -> Reverse proxy -> backtest_prod.py on 127.0.0.1:8789 -> SQLite`

Included in the repo:

- sanitized systemd examples in `deploy/systemd/`
- a sanitized Caddy example in `deploy/caddy/public-site.example.Caddyfile`
- deployment notes in `docs/lightsail_prod_setup.md`
- architecture notes in `docs/architecture.md`

Treat the local repo as the source of truth and your server as a deploy target.

## Security Notes

- Keep `.env`, database files, keys, and backups out of Git
- Use `.env.example` as the template for local setup
- Keep the public app on loopback behind a reverse proxy
- Keep production-only host details out of the public repo
- Review `docs/public_repo_checklist.md` before publishing changes that affect deployment or secrets

## Project Notes

- OAuth auth path only; no username/password login path
- Logs include `snapshot_id` and stage names for debugging
- Collector appends rows and does not automatically clean up old data
