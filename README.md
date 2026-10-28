# Zleek Score

Ingests live Polymarket market/orderbook/trade data into Postgres (TimescaleDB) and
Redis, computes rolling signals (features, "tremor" score), and serves them through a
Next.js web app.

## Components

- `apps/worker` -- Python asyncio ingestion service (Gamma REST + CLOB websocket +
  signals engine). This is the part that actually does the work.
- `apps/web` -- Next.js/React frontend. Currently unmodified `create-next-app`
  scaffolding; the intended pages (`AlertsPage.tsx`, `JournalPage.tsx`, `MarketsPage.tsx`,
  `MarketDetailPage.tsx`) and guards (`AuthGuard.ts`, `PlanGate.ts`) exist as empty files
  and are not yet implemented or wired into routing.
- `infra/sql` -- `schema.sql` (base tables) and `migrations/domain_model.sql` (signals
  tables). Both are mounted into the Postgres container's init scripts by
  `docker-compose.yml`.
- `docker-compose.yml` -- runs Postgres (TimescaleDB) and Redis only. It does not
  containerize the worker or web app themselves.

There is no hosted/production deployment path set up yet -- this is all local. The worker
does run as a persistent launchd service on this machine (see "Persistent worker service"
below) so it survives reboots and auto-restarts on crash; everything else (web app,
Docker) is still started manually as described below.

## Prerequisites

- Docker Desktop (for Postgres + Redis)
- Python 3.11+ (pandas/numpy pinned versions require it; 3.9/3.10 will fail to install)
- Node.js 20+ -- installing via [nvm](https://github.com/nvm-sh/nvm) is recommended if
  you don't already have Node:
  ```
  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.6/install.sh | bash
  nvm install --lts
  ```

## 1. Start Postgres + Redis

```
cd /path/to/Zleek_Score-main
docker compose up -d
```

This creates two containers, `zleek_db` (Postgres on host port `5435`) and
`zleek_redis` (Redis on `6379`), and applies `infra/sql/schema.sql` and
`infra/sql/migrations/domain_model.sql` on first run (only on first run -- if you need to
re-apply schema changes, `docker compose down && rm -rf postgres_data && docker compose
up -d`, which wipes local data).

Check they're healthy:
```
docker compose ps
docker compose logs db --tail 20
```

## 2. Configure environment

The worker reads its config from `apps/worker/ingestion/.env.local` (not the repo-root
`.env.example` -- that file is just a template). Create it:

```
cp .env.example apps/worker/ingestion/.env.local
```

Then edit `apps/worker/ingestion/.env.local` and fill in `POLYMARKET_WALLET_KEY` and
`FUNDER_ADDRESS` if you want authenticated trade-history backfill (see comments in
`.env.example` for what these are). The `DATABASE_URL`/`REDIS_URL` defaults already match
the docker-compose services above. This file is gitignored -- it will never be committed.

## 3. Set up and run the worker

```
cd apps/worker
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd ingestion
python main.py
```

To run it detached in the background instead of tying up a terminal:
```
nohup python main.py > /tmp/zleek_worker.log 2>&1 &
disown
```

### Persistent worker service (launchd)

For a setup that survives reboots and restarts automatically on crash, the worker also
runs as a macOS launchd agent, `~/Library/LaunchAgents/com.zleek.worker.plist`. It runs
the same `.venv` interpreter against `apps/worker/ingestion/main.py`, with
`RunAtLoad`/`KeepAlive` both true and a 15s restart throttle, logging to
`apps/worker/ingestion/worker.log`.

```
# load / start at login
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zleek.worker.plist

# check status (PID + last exit code)
launchctl list | grep zleek

# stop and unload
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.zleek.worker.plist
```

Since the worker needs Postgres/Redis, Docker Desktop is also set as a macOS Login Item
(System Settings > General > Login Items) so it starts automatically too. If the worker
starts before Docker is ready, launchd's restart throttle will keep retrying every 15s
until the database pool can connect -- no manual intervention needed. Don't run both the
launchd-managed worker and a manually-started one at the same time; check
`launchctl list | grep zleek` before starting a manual copy.

## 4. Set up and run the web app

```
cd apps/web
npm install
npm run dev
```

Open `http://localhost:3000`. Note this currently shows the default `create-next-app`
placeholder page, not real application UI (see Components above).

## Logs

The worker has no built-in file logging of its own -- it logs to stdout only (Python's
`logging` module, configured with a `StreamHandler`). Logs only exist wherever that
stdout gets redirected:
- If running under launchd (see above): `tail -f apps/worker/ingestion/worker.log`.
- Foreground: logs print directly to your terminal.
- Manually backgrounded (`nohup ... &`): wherever you redirected it, e.g.
  `tail -f /tmp/zleek_worker.log`.

Nothing rotates these files automatically -- a long-lived worker log will grow
indefinitely (it logs every websocket event stat). Truncate periodically if needed:
`: > apps/worker/ingestion/worker.log` (safe to do on the live file without restarting
the process, unlike renaming it).

The web app (Next.js dev server) behaves the same way -- no log file unless you redirect
`npm run dev`'s stdout yourself.

Postgres and Redis logs are captured by Docker and are always available regardless of
how you started them:
```
docker compose logs -f db
docker compose logs -f redis
```

## Checking the database

Quick one-off query:
```
docker exec zleek_db psql -U postgres -c "SELECT count(*) FROM trades;"
```

Interactive shell (`\dt` to list tables, `\d trades` to see a table's columns, `\q` to
quit):
```
docker exec -it zleek_db psql -U postgres
```

GUI client (TablePlus, Postico, DBeaver, pgAdmin, etc.) -- connect with:
```
Host: localhost
Port: 5435
User: postgres
Password: (POSTGRES_PASSWORD from your local .env — never commit it)
Database: postgres
```
(same as `DATABASE_URL` in `.env.local`)

Tables: `markets`, `trades`, `ob_snapshots`, `bars_1m`, `bars_state`, `features_5m`,
`signals_history`, `signals_latest`, `alert_rules`. A few queries worth keeping handy:
```sql
SELECT market_id, asset_id, price, side, ts FROM trades ORDER BY ts DESC LIMIT 10;
SELECT id, title, liquidity_usd FROM markets ORDER BY liquidity_usd DESC LIMIT 10;
SELECT * FROM signals_history ORDER BY ts DESC LIMIT 10;
```

## Stopping everything

```
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.zleek.worker.plist  # worker (if launchd-managed)
kill $(pgrep -f "ingestion/main.py")   # worker (if manually started instead)
kill $(pgrep -f "next dev")            # web dev server
docker compose down                    # Postgres + Redis (add -v to also wipe data)
```

## Known limitations

- Web app pages are unimplemented placeholders (see Components above).
- Gamma market fetch pagination hits a hard API ceiling around ~2100 markets (Polymarket
  returns `422 offset too large` beyond that and directs you to an undocumented
  `/markets/keyset` cursor endpoint). Not worth chasing today: the app only ever acts on
  the top ~20-200 markets by liquidity, well within that ceiling.
- No CI, tests, or production deployment configuration exist yet.
