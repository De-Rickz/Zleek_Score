# 48-hour parallel run: Mac → Oracle Cloud (zleekin.com)

Goal: before the Oracle server becomes the system of record, show for 48 hours
that it ingests, builds bars and scores at least as completely as the Mac's
launchd worker, and that it can be monitored, backed up and reached from
outside. Everything here is **read-only** against both databases; nothing
touches either running worker.

Both workers keep running for the whole 48 hours. Don't switch the Mac off
until [cutover](#5-cutover-criteria) passes.

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

Tested on 2026-11-02 against the Mac database, read-only: identical exports →
0 missed, PASS; a copy with 30 trades and one whole market deleted → exactly
30 MISSED and that market as NOT TRACKED.

## 3. Heartbeat on the server (cron, with external alerting)

The Mac's heartbeat (`apps/worker/scripts/heartbeat.py`) can't run on the
server as it is: it alerts through `osascript` (macOS notifications) and reads
`DATABASE_URL` from `apps/web/.env.local`. Nobody watches the server's
terminal, so alerts have to leave the machine.

### 3a. Code change (small diff, for review before deploying)

In `heartbeat.py`:
1. `database_url()`: use `os.environ["DATABASE_URL"]` when set; keep the
   `.env.local` file as the Mac fallback.
2. `notify(message)`: when `ALERT_WEBHOOK_URL` is set, POST the message there
   (push to phone); otherwise `osascript` as today (Mac unchanged).
3. After each run, GET `HEALTHCHECK_PING_URL` on OK or `…/fail` on FAIL, so an
   external dead-man's switch alerts when **no** ping arrives. That covers the
   cases the heartbeat can't report itself: server down, cron stopped,
   network broken.
4. Log line to stdout (cron captures it) instead of a file inside the repo.

Same two checks as on the Mac (signals within 15 min; live websocket trades
within 15 min), same thresholds.

### 3b. Alerting services (decision needed, both have free tiers)

| Purpose | Suggested | Alternatives |
|---|---|---|
| Push the alert text to your phone | [ntfy](https://ntfy.sh): POST to `https://ntfy.sh/<long-random-topic>`, subscribe in the ntfy app | Telegram bot, email via SMTP |
| Alert when the heartbeat stops arriving | [healthchecks.io](https://healthchecks.io): one check, period 15 min, grace 10 min, notifies by email/push | Better Stack, Cronitor |

Use a long random ntfy topic name (anyone who knows it can read it), or a
self-hosted/authenticated topic.

### 3c. How cron runs it

Run the heartbeat in a **one-off container** from the worker image, so it has
Python, asyncpg and network access to `db` without touching the running worker
container. Add a service to `docker-compose.prod.yml` that only starts on
demand:

```yaml
  heartbeat:
    image: <same image as the worker service>   # or build: same as worker
    profiles: ["cron"]            # never started by `up`; only by `run`
    env_file: /etc/zleek/heartbeat.env   # DATABASE_URL (read-only role), ALERT_WEBHOOK_URL, HEALTHCHECK_PING_URL
    command: ["python", "<path in image>/apps/worker/scripts/heartbeat.py"]
    depends_on: [db]
```

Crontab for the deploy user (`crontab -e`):

```cron
*/15 * * * * cd /path/to/compose && flock -n /tmp/zleek-heartbeat.lock docker compose -f docker-compose.prod.yml run --rm --no-deps heartbeat >> $HOME/zleek-heartbeat.log 2>&1
```

- `flock -n` stops overlapping runs if one hangs.
- `--no-deps` avoids (re)starting `db`; `--rm` removes the container afterwards.
- `DATABASE_URL` should use a **read-only role** (e.g. `zleek_web`, created as in `docs/vps-migration.md` step 3). The heartbeat only reads.
- Rotate `~/zleek-heartbeat.log` with logrotate, or drop the redirect and rely on healthchecks.io's history.

**Verify:** run the cron line by hand (expect one `OK signals=… trades=…`
line and a green check on healthchecks.io); stop the cron job for longer than
the grace period (expect the "down" notification); test the failure path with
a temporary `HEALTHCHECK_PING_URL`/`ALERT_WEBHOOK_URL` against a *copy* of the
check rather than stopping the worker.

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
`docs/vps-migration.md` step 8) only when all hold at 48h:

- [ ] Acceptance A-E as in section 1, at ~24h and at 48h.
- [ ] Funnel comparison over a full 24h window: MISSED within the agreed limit
      (see decisions), no cluster of missing hours, scored-market sets similar.
- [ ] Heartbeat: 48h of OK runs on the server, healthchecks.io green, one
      deliberate "down" alert received on the phone.
- [ ] Memory/disk headroom recorded; restore test passed; a scheduled backup exists.
- [ ] Site loads over mobile data with a valid certificate; only expected ports open.
