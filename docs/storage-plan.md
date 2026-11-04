# Storage plan (production, before phase 4)

Written 2026-11-04 during the soak. Nothing here changes the worker or the
database settings until phase 4 starts (ROADMAP: no earlier than 2026-11-06).
The scripts below are host-level and read-only towards the database.

## Where we are (2026-11-04)

- Database 592 MB after ~2.5 days on production; disk 12 GB used of 45 GB
  (after pruning 5.7 GB of docker build cache).
- Nightly dump 23 MB (11-02) to 54 MB (11-04): about 9% of the database.
- **Hypertables:** only `bars_1m` and `features_5m`. `trades`, `ob_snapshots`
  and `signals_history` are plain tables, so TimescaleDB compression and
  retention policies don't apply to them yet.
- `trades` can't simply be converted: a hypertable needs every unique
  constraint to include the time column, and `tx_hash UNIQUE` is what the
  worker's `ON CONFLICT (tx_hash)` dedupe relies on. A unique `(tx_hash, ts)`
  would not dedupe either, because the same fill arrives with a millisecond
  timestamp from the websocket and a whole-second one from the Data API.
- `ob_snapshots` (primary key `(market_id, ts)`) and `signals_history`
  (`(market_id, ts)`) can become hypertables as they are.
- Order books are stored in full (every level, both sides), at most one
  snapshot per market every 5 s.

Measured bytes per row (Mac database, same schema and code, 2026-11-04,
`storage-report.sql` section 3; on disk = total size including indexes / rows):

| table | on disk / row | row itself | of which |
|---|---|---|---|
| trades | 1,182 B | 663 B | raw JSON 400 B (60%) |
| ob_snapshots | 1,974 B | 1,714 B | bids + asks JSON 1,568 B (91%) |
| bars_1m | 299 B | 193 B | |
| features_5m | 142 B | 45 B | |
| signals_history | 114 B | 46 B | |

## Daily storage report

`apps/worker/scripts/storage-report.sql` (read-only): size of each table
(hypertables with all their chunks) and of the database; rows per day for the
last 3 full days plus today for trades, bars_1m, features_5m,
signals_history and ob_snapshots; bytes per row on disk and per stored row,
with the raw JSON share of a trade and the levels share of a book snapshot.
Run daily until Friday 2026-11-06, then after every phase 4 step:

```sh
cd ~/zleek && docker compose -f docker-compose.prod.yml exec -T \
  -e PGOPTIONS='-c default_transaction_read_only=on' \
  db psql -U zleek_web -d postgres -X < apps/worker/scripts/storage-report.sql
```

## Growth and runway (provisional, replace with Friday's report)

Inputs: production's rows per day from the 2026-11-03 funnel window (trades
54,035; ob_snapshots 27,788; features_5m 72,928; signals_history 72,909;
bars_1m 12,440) times the bytes per row above. Scaling with the websocket cap:
by 24h volume the top 100 markets trade 2.8x the top 20 and the top 200 3.95x;
trades, book snapshots and bars are scaled by that; features and
signals_history depend on the scored markets, not the cap, so they stay flat.

| per day | cap 20 (now) | cap 100 | cap 200 |
|---|---|---|---|
| trades | 64 MB | 179 MB | 253 MB |
| ob_snapshots | 55 MB | 154 MB | 217 MB |
| features_5m | 10 MB | 10 MB | 10 MB |
| signals_history | 8 MB | 8 MB | 8 MB |
| bars_1m | 4 MB | 11 MB | 15 MB |
| **sum of tables** | **141 MB** | **362 MB** | **503 MB** |

The measured total (592 MB / 2.5 days = 237 MB/day) is higher than the 141 MB
table sum: the first load of `markets` (Gamma payloads, rewritten on every
refresh) and index growth. The daily report shows how much of that repeats.

**With the phase 4 storage step** (assumptions: TimescaleDB compression 4x,
conservative for this kind of data; books at top 10 levels a side, about 1.8x
smaller per snapshot; retention below): long-run growth is mostly compressed
trades and bars, plus fixed-size plateaus for the tables with retention
(~0.9 GB at cap 100, ~1.2 GB at cap 200).

Runway to the 75% disk warning (33.75 GB of 45 GB; 21.75 GB free above today's
12 GB, assuming the rest of the disk stays flat):

| | growth / day | days to 75% |
|---|---|---|
| cap 20, as now (measured 237 MB/day) | 237 MB | ~92 |
| cap 100, no compression or retention | 362 MB | ~60 |
| cap 100, with the storage step | ~48 MB + 0.9 GB plateau | ~430 (14 months) |
| cap 200, no compression or retention | 503 MB | ~43 |
| cap 200, with the storage step | ~67 MB + 1.2 GB plateau | ~310 (10 months) |

Backups grow with the database: dumps are ~9% of it, 7 are kept on the same
disk and 30 in the bucket (20 GB free tier). At cap 200 without compression,
the database would pass ~20 GB within ~40 days: dumps of ~1.8 GB, 12.6 GB of
local dumps on the same disk, and the bucket over its free tier. Compressed
chunks dump compressed, so the storage step keeps backups small too.

## Phase 4 storage step (before the websocket cap raise)

See ROADMAP.md, phase 4 step 4. Details:

**Compression** for chunks older than the backfill's reach: compress after
3 days (the startup gap backfill reaches back up to 48 h, plus a day of
margin; repairs can still UPDATE compressed rows, more slowly). Use 1-day
chunks for new hypertables so the policy acts within a day of the threshold
(the default 7-day chunks would wait up to 10 days).

**Hypertables:** convert `ob_snapshots` and `signals_history` (their keys
already include the time). `trades` needs a design first, because of the
`tx_hash` dedupe: the likely shape is a compressed hypertable
`trades_archive` that rows move into once they're past the backfill's reach
(dedupe only matters inside that window), with the dashboard and repair
queries reading both. A smaller, independent win: store `raw` with lz4 and a
lower `toast_tuple_target` so the 400 B JSON is compressed in place.

**Retention**, one reason each (adjusted from the starting suggestion where
noted):

| table | keep | reason |
|---|---|---|
| trades | forever, compressed | the source of truth; repairs recompute from `raw` (CLAUDE.md), so raw must stay |
| bars_1m | forever, compressed | small (~4-15 MB/day), drives the charts; rebuildable from trades but slow to rebuild |
| features_5m | **30 days** (suggested 90) | fully recomputable from bars (the z uses only the last 240 min); the imbalance part is display-only |
| signals_history | 90 days | the record of what the dashboard showed; can't be recomputed exactly later |
| ob_snapshots | **30 days** (upper end of 14-30) | not recomputable (no raw), and the imbalance-near-price calibration (phase 4 leftover) needs weeks of books to compare; cheap once top-10 and compressed |
| markets | no retention | one row per market; watch its size in the report (Gamma rewrites `raw` on every refresh) |

**Order books (step 6):** store only the top 10 levels a side, and a snapshot
when the top of book changes or at a fixed interval (e.g. every 30 s),
whichever comes first; today it's every book event, throttled to 5 s.

**Measure after every phase 4 step:** `storage-report.sql` alongside
`dashboard-classes.sql` / `stale-classify.sql` (coverage).

## Host scripts (install below)

| script | schedule | what it does |
|---|---|---|
| `infra/ops/zleek-disk-alert.sh` | every 15 min | ntfy push (heartbeat topic) when disk use on / crosses 75% (high) or 90% (urgent), once per crossing; one "back to normal" note below 75%; state in `~/.zleek/` |
| `infra/ops/zleek-docker-prune.sh` | monthly, 1st, 04:30 UTC | `docker builder prune -f` and `docker image prune -f` (dangling only; tagged rollback images kept); log line with disk before/after |
| `infra/backup/zleek-restore-test.sh` | monthly, 2nd, 04:30 UTC | restores the newest nightly dump into a throwaway, network-less container from the same image as `zleek_db`; checks rows, counts against production (read-only) and acceptance C; logs OK/FAIL; FAIL pushed to ntfy |
| `infra/ops/zleek-ntfy.sh` | (helper) | sourced by the others; reads only `NTFY_URL` / `NTFY_TOKEN` from `apps/worker/.env.heartbeat`, never the database URL |

Times avoid the nightly backup (03:20) and the heartbeat (:00/:15/:30/:45).

### Install on the server

```sh
cd ~/zleek && git status --short && git pull
install -m 700 infra/ops/zleek-ntfy.sh infra/ops/zleek-disk-alert.sh \
               infra/ops/zleek-docker-prune.sh infra/backup/zleek-restore-test.sh ~/bin/
# the helper is sourced, not run; keep it next to the scripts in ~/bin
~/bin/zleek-disk-alert.sh && cat ~/.zleek/disk-alert.state     # expect "ok" (no push at 27%)
~/bin/zleek-docker-prune.sh && tail -1 ~/.zleek/docker-prune.log
~/bin/zleek-restore-test.sh; echo "exit=$?"; tail -1 ~/backups/zleek-db/restore-test.log
crontab -e   # add:
#   */15 * * * * /home/ubuntu/bin/zleek-disk-alert.sh >> /home/ubuntu/.zleek/cron.log 2>&1
#   30 4 1 * *  /home/ubuntu/bin/zleek-docker-prune.sh >> /home/ubuntu/.zleek/cron.log 2>&1
#   30 4 2 * *  /home/ubuntu/bin/zleek-restore-test.sh >> /home/ubuntu/.zleek/cron.log 2>&1
crontab -l
```

To test the disk alert's push without filling the disk, run it once with a
low threshold, then reset its state:

```sh
ZLEEK_DISK_WARN_PCT=10 ~/bin/zleek-disk-alert.sh   # push "Warning: disk / 27% used ..."
rm ~/.zleek/disk-alert.state                       # next normal run starts from "ok", no push
```

The first restore-test run is the real check: if `pg_restore` reports
TimescaleDB errors that are harmless, the script fails on them by design;
paste the log line and we'll decide whether to tolerate that specific error.
