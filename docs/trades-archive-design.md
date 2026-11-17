# trades_archive: compressing trades (ROADMAP phase 4 step 4, storage)

Status: built 2026-11-16 on branch phase4/storage-trades-archive (infra/sql/migrations/trades_archive.sql, trades_archive_enable.sql; tests test_trades_archive.sh; rehearsal rehearse_trades_archive.sh). Not yet rehearsed on a production dump, not deployed.

## Requirements (set by the human, 2026-11-16)

1. **Dedupe across both tables** for any write older than the cutoff: a
   trade already in `trades` or `trades_archive` is never stored again.
   (Writes older than the cutoff, below.)
2. **A `trades_all` view** that every history reader uses. (Tables; Read paths.)
3. **A single-transaction nightly move**: no moment where a row is in both
   tables or neither. (Nightly move.)
4. **A rehearsal on a restored production dump** before the production run,
   timing each step and measuring peak disk. (Rehearsal.)
5. Carried over from the option A requirements: the production run starts
   from a **fresh backup verified off-site** and an **updated restore test**
   (the database is ~5.8 GB), and **inserts into compressed chunks must work
   and dedupe correctly on TimescaleDB 2.31** (production's version), for the
   recovery script and any repair. (Initial migration; Rehearsal.)

## Why this shape (option B)

TimescaleDB compression cuts trades ~4.2x (tested 2026-11-16: 162 MB -> 39 MB
for 200,000 rows), but only on a hypertable, and a hypertable's unique keys
must include the time column. Converting `trades` itself (option A) would turn
`UNIQUE (tx_hash)` into `UNIQUE (tx_hash, ts)`, and the same trade does not
have the same ts on both paths: on production (2026-11-15, --check-ts) every
websocket-stored trade is 1-5 s away from the Data API's timestamp (the
websocket's is earlier, by up to ~4 s; 0% identical), while Data API-stored
trades match it 100%. Every re-fetch of a websocket trade (step 2c's 24 hours,
the gap backfill, the recovery script) would then be stored a second time.

So `trades` stays exactly as it is for recent data, and rows move to a
compressed hypertable once nothing re-fetches them any more.

## Tables

- **`trades`** (hot, unchanged): plain table, `UNIQUE (tx_hash)`, every write
  path as today. Holds the last `CUTOFF` = **4 full UTC days**: further back
  than anything the worker re-fetches (step 2c 24 h, gap backfill capped at
  48 h) plus a day of margin.
- **`trades_archive`** (cold): same columns and types (including `id`, so ids
  stay unique across both), a hypertable on `ts` with **1-day chunks**,
  compressed with `segmentby = market_id`, `orderby = ts DESC` (the dashboard
  reads a market's latest trades), compression policy `compress_after = 5 days`
  (a chunk is complete once the day has moved). Indexes: `(market_id, ts)`
  (TimescaleDB's segmentby/orderby cover it on compressed chunks), and
  `(tx_hash)` non-unique for the uncompressed chunks. No unique constraint. FK
  `market_id -> markets(id)` as in trades. No retention: kept forever.
- **`trades_all`** (view): `SELECT ... FROM trades UNION ALL SELECT ... FROM
  trades_archive`. A row is in exactly one table at any moment (the move is
  one transaction, below), so `UNION ALL` never double counts. Filters on
  `market_id` and `ts` are pushed into both sides; chunk exclusion skips
  archive chunks outside a `ts` range.

## Nightly move (one transaction)

A procedure `archive_trades()` run daily by TimescaleDB's job scheduler
(`add_job`, 04:00 UTC, after the 03:20 backup):

```sql
WITH moved AS (
  DELETE FROM trades WHERE ts < date_trunc('day', now()) - interval '4 days'
  RETURNING *)
INSERT INTO trades_archive SELECT * FROM moved;
```

One statement, one transaction: readers of `trades_all` see each row in
exactly one table before and after, never both and never neither. ~360,000
rows (~440 MB) a night at today's rate. The deleted space in `trades` is
reused by new rows after autovacuum, so `trades` levels off at ~4 days
(~1.8 GB at today's rate) instead of growing. The job's success is visible in
`timescaledb_information.job_stats`.

## Writes older than the cutoff (dedupe across both tables)

Normal operation never writes a trade older than 4 days, so `REST_QUERY`'s
`ON CONFLICT (tx_hash)` against `trades` stays the dedupe. A write older than
the cutoff (only the recovery script's `--apply` over an old window, or a
future repair) must not create a second copy of a row that has been archived,
so every REST write goes through one database function, `zleek_store_trade`
(the writer's `REST_QUERY` is now `SELECT zleek_store_trade($1..$13)`). It
compares the trade's ts with `archive_cutoff()` (4 UTC days, the database's
clock, the same function the move uses): newer, exactly the old upsert into
`trades`; older, into `trades_archive` only if neither table holds the tx_hash:

```sql
IF EXISTS (SELECT 1 FROM trades WHERE tx_hash = p_tx_hash)
   OR EXISTS (SELECT 1 FROM trades_archive a
              WHERE a.market_id IS NOT DISTINCT FROM p_market_id AND a.tx_hash = p_tx_hash
                AND a.ts BETWEEN p_ts - interval '1 minute' AND p_ts + interval '1 minute')
THEN RETURN 'duplicate';
```

The `market_id` + `ts` window lets TimescaleDB decompress only that market's
segment for one chunk (it can't use a tx_hash index on compressed data); the
minute covers the websocket's 1-5 s offset. A transaction-level advisory
lock on the tx_hash serializes two writers of the same old trade. The
recovery script's own "already stored" lookup reads both tables the same way.
The price-only update that `REST_QUERY` does on conflict is not done for
archived rows (they're past the backfill's reach; a repair updates them
directly).

## Read paths to switch to `trades_all`

Anything that may need trades older than 4 days:

| code | today | change |
|---|---|---|
| `apps/web/lib/queries.ts` latest trade per market (`max(ts)`) | trades | trades_all: a market quiet for 4+ days still shows its last trade |
| `apps/web/lib/queries.ts` recent trades of a market | trades | trades_all |
| `bars_jobs.rebuild_bars` (`[start, end)`) | trades | trades_all: the recovery script and repairs rebuild old windows |
| `writer.newest_stored_trade` (gap backfill start) | trades | trades_all: after an outage of 4+ days the move would empty `trades` and the gap backfill would think the database is fresh |
| `scripts/recover_backfill_gap_2026-11.py` lookups | trades | both tables, as above |
| `scripts/dashboard-classes.sql`, `stale-classify.sql`, `storage-report.sql` | trades | trades_all; the report also lists trades_archive |
| `infra/backup/zleek-restore-test.sh` counts | trades | trades_all |

Unchanged (recent data only): `features.py` (trades in the last interval),
`heartbeat.py` (latest websocket / backfill trade), the periodic bars job
(last 65 minutes). Historic one-off repair scripts (`*_2026-10-*.sql`) stay as
they were, with a note that they predate the archive.

## Checks

- Heartbeat (`archive` check, only once trades_archive exists): `trades` holds
  no row older than 5 UTC days (the move ran; this replaces a job_stats check,
  which the read-only role may not see); no `tx_hash` in both tables within an
  hour either side of the cutoff (where the websocket/Data API offset could
  put one trade on both sides); no duplicate `tx_hash` in the newest archived
  day (the permanent duplicate guard). Each query is indexed and bounded.
- `storage-report.sql`: trades and trades_archive sizes, compressed chunks.

## Initial migration (production, once)

Order matters: the readers switch to `trades_all` BEFORE any row moves, so the
dashboard never loses history in between.

1. Fresh manual backup, confirmed off-site (`OK … offsite=ok` in backup.log
   and `rclone check --size-only --one-way` against the bucket: 0
   differences), and the restore test, updated to count `trades_all` once the
   archive exists, passes on that dump (database ~5.8 GB: check the
   throwaway container has the disk for it).
2. `trades_archive.sql`: creates the archive, the view and the functions;
   moves nothing, schedules nothing.
3. Deploy the worker, web and heartbeat changes (they read through
   `trades_all` and write through `zleek_store_trade`), with the usual
   rollback tags.
4. `CALL archive_trades_backlog()`: the backlog **one day per transaction**,
   oldest first, up to the cutoff (~2.6 GB on 2026-11-16).
5. Compress the archive's chunks older than 5 days; `trades_archive_enable.sql`
   adds the compression policy and the 04:00 UTC job.
6. Give the space back to the OS: the moved rows leave ~2.6 GB of reusable
   space inside `trades`. `VACUUM FULL trades` rewrites it (exclusive lock:
   the worker's writes wait, Redis buffers them) or the space is left to be
   reused by new rows. Decided after the rehearsal timing.

## Rehearsal first (on the Mac, not production)

Restore last night's production dump into a throwaway TimescaleDB **2.31.0**
container (the production version) and run the migration exactly as above:

- time each step and measure peak disk (data directory size before, during,
  after; and with/without `VACUUM FULL`);
- verify: row count and `count(DISTINCT tx_hash)` of trades_all equal the
  original trades; the dashboard queries return identical results before and
  after; `rebuild_bars` for a past day gives identical bars;
- dedupe: write an archived trade again through `zleek_store_trade` (with the
  websocket's ts and with the Data API's) and through the recovery script:
  0 rows added; a genuinely new old trade: 1 row added, into a compressed chunk;
- inserts into **compressed** chunks on 2.31.0 (production's version): an
  archived trade written again is refused by the dedupe whether its chunk is
  compressed or not, and a new old trade lands in a compressed chunk and
  reads back through `trades_all`;
- the nightly move twice in a row: the second moves nothing;
- dump size: `pg_dump -Fc -Z 6` of the copy before and after the migration,
  to see whether compression shrinks the nightly dumps (docs/storage-plan.md
  expects little change) and so whether off-site retention still needs a
  decision.

Only then plan the production run, with the rehearsal's timings.

## Known TimescaleDB 2.31.0 bug: no tie-breaker with ORDER BY ... LIMIT on trades_all

Found by the rehearsal on the 2026-11-17 production dump. A query on
`trades_all` with `ORDER BY ts DESC, <another column> ... LIMIT n` fails with
`ERROR: retrieved too many tuples in a bounded sort` once `trades_archive` has
statistics (`ANALYZE`, which autovacuum runs): the planner puts an Incremental
Sort (presorted on ts) over a Merge Append of trades and the archive's
ChunkAppend, the ChunkAppend's per-chunk Sorts are bounded by the LIMIT, and
tied timestamps make the Incremental Sort pull past that bound. Not affected:
`ORDER BY ts DESC LIMIT n` (the web's recent-trades query), the same query on
`trades_archive` alone, and ordered aggregates (`array_agg(... ORDER BY ts, id)`
in the bars rebuild). Reproduced on 8 of 8 archived markets after `ANALYZE
trades_archive`, 0 before. So: keep ORDER BY ... LIMIT on trades_all to `ts`
alone (a comment in apps/web/lib/queries.ts says so); for a deterministic order
among tied trades, select everything at or after the n-th newest ts and order
inside an aggregate or in the application. Re-check after any TimescaleDB
upgrade, and consider reporting it upstream (timescale/timescaledb).

## Websocket vs Data API timestamps

Recorded for anything that matches trades by time: the websocket stores its
event time in milliseconds, 1-5 s **earlier** than the Data API's whole-second
timestamp for the same trade (production, 2026-11-15). Match trades by
`tx_hash`, never by exact `ts`; a time-window match needs at least +-5 s.
