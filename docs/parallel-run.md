# 48-hour parallel run: Mac → Oracle Cloud (zleekin.com)

Goal: before the Oracle server becomes the system of record, show for 48 hours
that it ingests, builds bars and scores at least as completely as the Mac's
launchd worker, and that it can be monitored, backed up and reached from
outside. Everything here is **read-only** against both databases; nothing
touches either running worker.

Both workers keep running for the whole 48 hours. Don't switch the Mac off
until [cutover](#5-cutover-criteria) passes.

This document replaces the earlier `docs/vps-migration.md` plan (systemd
units on a fresh VPS), which was never merged: production runs as a docker
compose stack (db, redis, worker, web, caddy) on Oracle Cloud instead. Its
still-accurate parts are in sections [6](#6-security-baseline-and-constraints)
to [9](#9-backups).

## 0. Variables used below

The production compose file isn't in the repo, so check these against
`docker-compose.prod.yml` on the server and adjust once:

```sh
# on the Oracle server, in the directory with docker-compose.prod.yml
PROD="docker compose -f docker-compose.prod.yml"
DB=db                 # the Postgres service name (you listed: db, redis, worker, web, caddy)
PGUSER=postgres       # check POSTGRES_USER in docker-compose.prod.yml (default: postgres)
PGDATABASE=postgres   # check POSTGRES_DB (default: same as the user)
REPO=~/Zleek_Score    # path of the repo checkout on the server (for the .sql files)

# read-only psql into the db container: every statement runs in a read-only transaction
psql_ro() { $PROD exec -T -e PGOPTIONS='-c default_transaction_read_only=on' "$DB" \
              psql -U "$PGUSER" -d "$PGDATABASE" -X -v ON_ERROR_STOP=1 "$@"; }
```

On the Mac, the same function with `PROD="docker compose"` in the repo root,
`DB=db`, `PGUSER=postgres`, `PGDATABASE=postgres`.

`PGOPTIONS=default_transaction_read_only=on` makes Postgres reject any write in
that session, so even the superuser can't change data by accident. (`psql` runs
inside the db container, so nothing needs installing on the host.)

## 1. Acceptance queries A-E on production

`apps/worker/scripts/acceptance_price_orientation.sql` runs unchanged: it's
SELECT-only, and its fixed cutoff (E: snapshots from 2026-10-30 17:24:17) is
before the production database existed, so E simply checks all of production's
snapshots.

```sh
psql_ro < $REPO/apps/worker/scripts/acceptance_price_orientation.sql
```

Five result sets come back, in order A, B, C, D, E. Expected on production:

| Query | Expect | Note |
|---|---|---|
| A: bars jumping > 0.5 in 24h | a handful at most (real moves) | needs ~24h of data first |
| B: dashboard scores | `score_mismatch` 0, `ineligible_on_dashboard` 0, `stale_scores` 0 | `over_50` is a tracking number |
| C: wrong token labels | `labels_wrong` 0 | |
| D: trade orientation | `price_wrong` 0, `side_wrong` 0, `no_raw_price` 0 | |
| E: book snapshots | `crossed` 0, `best_bid_mismatch` 0, `mid_far_from_nearby_trades` 0 | `one_sided_comparable` / `skipped_no_nearby_trades` are informational |

Run it after ~24h and again at 48h. B can briefly show 1 in
`ineligible_on_dashboard` right after a market ages out; rerun 5 minutes
later if so.

## 2. Funnel comparison: Mac vs production

Same window, same queries on both databases, then compare offline.

**Pick the window:** a fixed UTC range that both machines covered fully and
that ended **at least 70 minutes ago** (the backfill re-fetches the last hour,
the bars job rebuilds the last 65 minutes). E.g. at hour 30 of the run, use
the previous complete UTC day, or a 24h range starting 2h after the server's
first start.

```sh
T0='2026-11-03 00:00:00+00'; T1='2026-11-04 00:00:00+00'
S=$REPO/apps/worker/scripts
```

**On each machine** (prod shown; on the Mac use `mac_` file names):

```sh
psql_ro -v t0="$T0" -v t1="$T1" < $S/funnel_compare.sql                                  # summary, read on screen
psql_ro -v t0="$T0" -v t1="$T1" -q < $S/funnel_export_trades.sql  > prod_trades.csv
psql_ro -v t0="$T0" -v t1="$T1" -q < $S/funnel_export_markets.sql > prod_markets.csv
```

Copy the production CSVs to the Mac (`scp server:prod_*.csv .`), then:

```sh
python3 apps/worker/scripts/compare_funnels.py mac_trades.csv prod_trades.csv mac_markets.csv prod_markets.csv
```

Reading the result:

- **MISSED** = trades the Mac has, in markets production *was* tracking in that
  window, that production lacks. This is the "silently dropping data" signal.
  Target: 0. The script prints the markets and UTC hours they fall in:
  clustered in one hour points at a production outage (check
  `$PROD logs worker --since ...`); spread out points at a systematic gap.
- **NOT TRACKED** = Mac-only trades in markets production had no trades for
  at all. Some is expected: each worker ranks markets from its own `markets`
  table (websocket: top 20 by 24h volume; backfill: top 200 by liquidity), and
  production's started fresh. Large numbers in big markets are worth a look.
- **Prod-only** in markets the Mac tracked = trades the Mac missed (the Mac
  sleeping or restarting shows up here).
- Per-market section: markets where production has bars/features/scores and
  the Mac doesn't, or the reverse, and markets where production has fewer bars.
- Verdict: PASS when MISSED is at most 0.1% of the Mac's trades in markets
  production tracked (`--max-missed-pct`, default 0.1); exit code 1 otherwise.

Tested on 2026-11-02 against the Mac database, read-only: identical exports →
0 missed, PASS; a copy with 30 trades and one whole market deleted → exactly
30 MISSED and that market as NOT TRACKED.

## 3. Heartbeat on the server (cron, with external alerting)

Nobody watches the server's terminal, so alerts have to leave the machine.
`apps/worker/scripts/heartbeat.py` runs the same two checks as on the Mac
(newest signal and newest live websocket trade, each within 15 min) and is
configured by environment variables; on the Mac, with none set, it behaves
exactly as before (macOS notification, `apps/worker/ingestion/heartbeat.log`).

| Variable | Value on the server | Effect |
|---|---|---|
| `DATABASE_URL` | read-only role (see [6](#6-security-baseline-and-constraints)), host `db` | the two checks |
| `NTFY_URL` | `https://ntfy.sh/<long-random-topic>` | each alert pushed to your phone (ntfy app subscribed to the topic) |
| `NTFY_TOKEN` | optional | bearer token, if the topic is access-controlled |
| `HEALTHCHECK_URL` | `https://hc-ping.com/<check-uuid>` | every run pings it: `<url>` on OK, `<url>/fail` on FAIL, log line as the body |
| `HEARTBEAT_LOG` | `-` | log line to stdout (cron or docker logs keep it) |

What each part catches:
- **ntfy**: the worker crashed or went silent while the server is up. The
  checks fail (no fresh signals, or no live trades), and you get a push
  within one 15-min run.
- **healthchecks.io**: the failures the heartbeat can't report itself: the
  server is down, cron stopped, the heartbeat container can't start, the
  network is broken. healthchecks.io alerts when no ping arrives within
  period + grace. It also alerts on every `/fail` ping, so a FAIL reaches you
  by both routes.

Setup: one healthchecks.io check, period 15 min, grace 10 min, notifications
to email and/or its ntfy integration. Use a long random ntfy topic name
(anyone who knows it can subscribe), or an access-controlled topic with
`NTFY_TOKEN`. Alert delivery problems go to stderr and never stop a run;
`apps/worker/tests/test_heartbeat.py` covers OK, stale, database down, ntfy
down, and the unchanged Mac path.

### 3c. How cron runs it

Run the heartbeat in a **one-off container** from the worker image, so it has
Python, asyncpg and network access to `db` without touching the running worker
container. Add a service to `docker-compose.prod.yml` that only starts on
demand:

```yaml
  heartbeat:
    image: <same image as the worker service>   # or build: same as worker
    profiles: ["cron"]            # never started by `up`; only by `run`
    env_file: .env.heartbeat      # gitignored: DATABASE_URL (read-only role), NTFY_URL, HEALTHCHECK_URL, HEARTBEAT_LOG=-
    command: ["python", "<path in image>/apps/worker/scripts/heartbeat.py"]
    depends_on: [db]
```

Crontab for the deploy user (`crontab -e`):

```cron
*/15 * * * * cd /path/to/compose && flock -n /tmp/zleek-heartbeat.lock docker compose -f docker-compose.prod.yml run --rm --no-deps heartbeat >> $HOME/zleek-heartbeat.log 2>&1
```

- `flock -n` stops overlapping runs if one hangs.
- `--no-deps` avoids (re)starting `db`; `--rm` removes the container afterwards.
- `DATABASE_URL` should use a **read-only role** ([section 6](#6-security-baseline-and-constraints)). The heartbeat only reads.
- Rotate `~/zleek-heartbeat.log` with logrotate, or drop the redirect and rely on healthchecks.io's history.

**Verify:** run the cron line by hand (expect one `OK signals=… trades=…`
line and a green check on healthchecks.io); stop the cron job for longer than
the grace period (expect the "down" notification); test the alert path by
running the heartbeat once by hand with `NTFY_URL` pointed at a test topic and
`HEALTHCHECK_URL` at a second, test check, rather than stopping the worker.

## 4. Manual checks

- [ ] **Memory:** `docker stats --no-stream` and `free -h`. Note each service's
      memory; total should leave ≥ 20% free. Compare `TS_TUNE_MEMORY` (if set in
      the prod compose) with the instance's RAM: on a 1 GB "micro" shape a
      2 GB setting is wrong; on an Ampere A1 shape it's fine.
- [ ] **Disk:** `df -h /` and `docker system df`; database size:
      `psql_ro -c "SELECT pg_size_pretty(pg_database_size(current_database()))"`.
      Run again after 24h to get growth per day; leave room for ≥ 30 days of
      growth (more once phase 4 raises coverage).
- [ ] **Backup restore test** (uses a throwaway container; doesn't touch the
      running ones):
      ```sh
      $PROD exec -T "$DB" pg_dump -U "$PGUSER" -Fc "$PGDATABASE" > /tmp/zleek-test.dump
      docker run -d --name zleek-restore-test -e POSTGRES_PASSWORD=restoretest \
             timescale/timescaledb:<same tag as prod>     # same TimescaleDB version, or restore fails
      docker exec -i zleek-restore-test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();"
      docker exec -i zleek-restore-test pg_restore -U postgres -d postgres --no-owner < /tmp/zleek-test.dump
      docker exec -i zleek-restore-test psql -U postgres -c "SELECT timescaledb_post_restore();"
      docker exec -i zleek-restore-test psql -U postgres -c "SELECT count(*) FROM trades; SELECT count(*) FROM bars_1m;"
      docker exec -i zleek-restore-test psql -U postgres < $REPO/apps/worker/scripts/acceptance_price_orientation.sql
      docker rm -f zleek-restore-test && rm /tmp/zleek-test.dump
      ```
      Pass: the restore completes, counts are close to production's at dump
      time, and the acceptance file gives the same results. TimescaleDB needs
      `timescaledb_pre_restore()` / `post_restore()` around `pg_restore`.
      If there is no scheduled backup yet, add one (nightly `pg_dump`, copied
      off the server) before cutover.
- [ ] **Outside the home network:** on a phone with Wi-Fi **off** (mobile
      data), open `https://zleekin.com`: padlock shows a valid certificate
      (issuer Let's Encrypt), the dashboard loads, a market page renders its
      chart. Mobile carriers often prefer IPv6, so also check there's no stale
      AAAA record: `dig +short AAAA zleekin.com @1.1.1.1` should be empty or
      the server's IPv6. And `dig +short A zleekin.com @1.1.1.1` / `@8.8.8.8`
      should return only the server's IP.
- [ ] **Security List / firewall** (from the deploy gotcha): from outside,
      `nmap -Pn -p 22,80,443,5432,5435,6379 zleekin.com` shows only 22, 80
      and 443 (or 22 and 443). Postgres and Redis must not be reachable.

## 5. Cutover criteria

Switch production to system of record (and stop the Mac worker,
[section 7](#7-switching-off-the-mac-worker)) only when all hold at 48h:

- [ ] Acceptance A-E as in section 1, at ~24h and at 48h.
- [ ] Funnel comparison over a full 24h window: verdict PASS at the default
      0.1% MISSED limit, no cluster of missing hours, scored-market sets similar.
      Any MISSED trade is explained before cutover.
- [ ] Heartbeat: 48h of OK runs on the server, healthchecks.io green, one
      deliberate "down" alert received on the phone.
- [ ] Memory/disk headroom recorded; restore test passed; a scheduled backup exists ([section 9](#9-backups)).
- [ ] The worker and the heartbeat use least-privilege roles, not `postgres` ([section 6](#6-security-baseline-and-constraints)).
- [ ] Site loads over mobile data with a valid certificate; only expected ports open.

## 6. Security baseline and constraints

Carried over from the earlier plan, adapted to the docker compose deploy.

**Network**
- Cloud firewall (VCN Security List) **and** ufw on the instance: ingress
  22, 80, 443 only. Both layers are needed (CLAUDE.md, 2026-11-02 gotcha).
- Postgres and Redis must not be reachable from outside. Best: no `ports:`
  for `db` and `redis` at all (internal docker network only). If a port is
  ever published for maintenance, bind it to `127.0.0.1`: **Docker-published
  ports bypass ufw**.
- Redis with a password (`requirepass`) even on the internal network, and
  `--appendonly yes` so queued items survive a Redis restart (the flush only
  removes items after the Postgres commit, so a persisted queue loses nothing).
- SSH: keys only, no root login, no password authentication.

**Secrets**
- Credentials live only in gitignored env files on the server (`.env`,
  `.env.prod`, `.env.heartbeat`); compose files, Caddyfile and Dockerfiles in
  the repo hold none.
- Hex-only passwords (`openssl rand -hex 32`): base64 characters break
  connection URLs (CLAUDE.md gotcha).
- Not needed anywhere: `POLYMARKET_WALLET_KEY`, `FUNDER_ADDRESS`.
  `ingestion/clob_client.py` and `py-clob-client` are unused since the Data API
  backfill (nothing imports them) and can be removed.

**Least-privilege roles** (the Mac's worker also runs as `postgres`; don't
carry that over). Run once as `postgres` with the passwords passed in, not
committed:
```sql
CREATE ROLE zleek_worker LOGIN PASSWORD :'worker_pw';
GRANT USAGE ON SCHEMA public TO zleek_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO zleek_worker;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO zleek_worker;

CREATE ROLE zleek_web LOGIN PASSWORD :'web_pw';          -- web app and heartbeat
GRANT USAGE ON SCHEMA public TO zleek_web;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO zleek_web;  -- incl. bars_state
ALTER ROLE zleek_web SET default_transaction_read_only = on;
```
```sh
$PROD exec -T "$DB" psql -U postgres -v worker_pw=... -v web_pw=... < roles.sql
```
Grants on a hypertable also apply to its chunks. Switching the running worker
to `zleek_worker` is a restart with a new `DATABASE_URL`, so do it as a planned
step, not during the parallel run.

**Logs.** Docker's default `json-file` logging is unbounded. Cap it per service
(`logging: {driver: json-file, options: {max-size: "50m", max-file: "5"}}`),
or the worker's per-trade log lines fill the disk the way `worker.log` grew on
the Mac. `scripts/check_multifill_collisions.py` Check 1 reads a log file: feed
it `$PROD logs --no-color --since 168h worker > /tmp/worker.log` and pass
`--log /tmp/worker.log`.

**Constraints that still hold**
- The schema comes from `infra/sql/schema.sql` + `infra/sql/migrations/domain_model.sql`
  (TimescaleDB image; they reproduce the Mac's schema exactly, checked 2026-10-31).
  Init scripts run only on an empty volume.
- The startup gap backfill refills at most 48h after any worker restart; longer
  gaps are lost.
- Each worker picks its markets from its own database, so Mac and production
  coverage differ somewhat in the tail (section 2).
- Stray repo-root file `nly headline, recent-window z, … display"` (accidental
  `git diff` output in `a04e4fc`, no secrets): delete it.

## 7. Switching off the Mac worker

Only after [cutover criteria](#5-cutover-criteria) pass:
```sh
launchctl bootout gui/$(id -u)/com.zleek.worker
launchctl bootout gui/$(id -u)/com.zleek.heartbeat
```
- Keep both plists in `~/Library/LaunchAgents/` and the Mac's Postgres data
  (`postgres_data/`) for a 2-week rollback window. `docker compose stop` frees
  the Mac; don't `docker compose down -v`.
- Optional: restore Mac sleep (`sudo pmset -c sleep 10`).
- Then close the Mac-only `infra/TODO.md` items (Mac sleep; `worker.log`
  rotation becomes the docker log cap above).

**Verify:** `launchctl print gui/$(id -u)/com.zleek.worker` reports the
service isn't found; no new lines in the Mac's `worker.log`; production keeps
going (heartbeat green).

## 8. Rollback

| Situation | Rollback |
|---|---|
| Parallel run shows a problem | Nothing to roll back: the Mac never stopped. Fix production, or `$PROD stop worker` while investigating. |
| After cutover, within the 2-week window | On the Mac: `docker compose start`, then `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zleek.worker.plist` (and the heartbeat). The startup gap backfill refills up to 48h of trades in the Mac's database; anything older than that is missing there. |
| After the window | The Mac's data is stale; recover production from its backups (section 9). |

Data never moves between the two databases; each is complete for the time
its own worker ran (plus up to 48h of backfill after a restart).

## 9. Backups

Production's database is the only copy once the Mac is off.
- Nightly `pg_dump -Fc` from the `db` container (cron on the host, like the
  heartbeat), keep 7 days locally, and copy each dump off the server (Oracle
  Object Storage, or any S3-compatible bucket via `rclone`). A dump that only
  exists on the same disk doesn't survive losing the instance.
- Restore test as in [section 4](#4-manual-checks) once now, then after any
  TimescaleDB image upgrade (restores need the same TimescaleDB version).
- Ping a second healthchecks.io check from the backup job, so a backup that
  silently stops running raises an alert too.

