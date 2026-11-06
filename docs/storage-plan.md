# Storage plan (production, before phase 4)

Written 2026-11-04 during the soak. Nothing here changes the worker or the
database settings until phase 4 starts (ROADMAP: no earlier than 2026-11-06).
The scripts below are host-level and read-only towards the database.

## Where we were (2026-11-04)

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

## Growth and runway (measured 2026-11-06)

From production's storage report on 2026-11-06 14:55 UTC (database 790 MB,
disk 13 GB of 45 GB, 30%): rows per day averaged over the complete days
Tue 11-03 to Thu 11-05, times production's own bytes per row on disk.

| table | rows/day | on disk / row | per day at cap 20 |
|---|---|---|---|
| trades | 74,300 (63k, 68k, 92k: rising) | 992 B | 74 MB |
| ob_snapshots | 29,500 | 2,014 B | 59 MB |
| features_5m | 58,500 (falling as markets end) | 111 B | 6.5 MB |
| signals_history | 58,400 | 108 B | 6.3 MB |
| bars_1m | 10,400 | 288 B | 3 MB |
| **sum** | | | **~149 MB** |

Cross-check: the tables other than `markets` hold 652 MB after 4.7 days on
production, ~138 MB/day. Raw JSON is 60% of a trade row and the book levels
91% of a snapshot, as on the Mac. `markets` is 138 MB for ~8,500 markets
(~16 KB each: full Gamma payloads, plus dead row versions from rewriting them
on every refresh); whether it keeps growing or plateaus shows in the next
reports. The week includes the US midterms (Tue 11-03), so trades are likely
busier than usual: a safe basis for planning.

Scaling with the websocket cap: by 24h volume the top 100 markets trade 2.8x
the top 20 and the top 200 3.95x; trades, book snapshots and bars scale by
that; features and signals_history depend on the scored markets, not the cap.

| per day | cap 20 (now) | cap 100 | cap 200 |
|---|---|---|---|
| trades | 74 MB | 207 MB | 292 MB |
| ob_snapshots | 59 MB | 166 MB | 233 MB |
| features_5m | 6.5 MB | 6.5 MB | 6.5 MB |
| signals_history | 6.3 MB | 6.3 MB | 6.3 MB |
| bars_1m | 3 MB | 8.4 MB | 12 MB |
| **sum** | **~149 MB** | **~394 MB** | **~550 MB** |

**With the phase 4 storage step** (assumptions: TimescaleDB compression 4x,
conservative for this kind of data; books at top 10 levels a side, about 1.8x
smaller per snapshot; retention below): long-run growth is mostly compressed
trades and bars, plus fixed-size plateaus for the tables with retention
(~0.9 GB at cap 100, ~1.2 GB at cap 200).

Runway to the 75% disk warning (33.75 GB of 45 GB; 20.75 GB free above the
13 GB used on 2026-11-06, assuming the rest of the disk stays flat):

| | growth / day | days to 75% |
|---|---|---|
| cap 20, as now | ~149 MB | ~139 |
| cap 100, no compression or retention | ~394 MB | ~53 |
| cap 100, with the storage step | ~54 MB + 0.9 GB plateau | ~370 (12 months) |
| cap 200, no compression or retention | ~550 MB | ~38 |
| cap 200, with the storage step | ~76 MB + 1.2 GB plateau | ~260 (8.5 months) |

Backups grow with the database: the nightly dump went 54 -> 73 -> 92 MB on
11-04 to 11-06 (~19 MB/day, ~12% of the database), 7 kept on the same disk and
30 in the bucket (20 GB free tier). At cap 200 without compression the
database would pass ~20 GB within ~40 days: dumps of ~2.5 GB, ~17 GB of local
dumps on the same disk (so the real runway is shorter than the table says),
and the bucket far over its free tier. Compressed chunks dump compressed, so
the storage step keeps backups small too.

## Phase 4 storage step (before the websocket cap raise)

See ROADMAP.md, phase 4 step 3 (after steps 2a and 2, which multiply stored trades). Details:

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
