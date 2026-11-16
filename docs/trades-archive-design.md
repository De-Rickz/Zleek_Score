# trades_archive: compressing trades (ROADMAP phase 4 step 4, storage)

Status: design, 2026-11-16, for review. Nothing here is built yet.

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
so the writer routes it to `ARCHIVE_QUERY` instead:

```sql
INSERT INTO trades_archive (...)
SELECT $1, ... WHERE NOT EXISTS (
    SELECT 1 FROM trades_archive a
    WHERE a.market_id = $1 AND a.tx_hash = $9
      AND a.ts BETWEEN $3 - interval '1 minute' AND $3 + interval '1 minute')
  AND NOT EXISTS (SELECT 1 FROM trades t WHERE t.tx_hash = $9);
```

The `market_id` + `ts` window lets TimescaleDB decompress only that market's
segment for one chunk (it can't use a tx_hash index on compressed data); the
minute covers the websocket's 1-5 s offset. Old writes are single-threaded
(the recovery script), so there is no race between the two checks. The
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

- Heartbeat: `trades` holds no row older than cutoff + 1 day (the move ran);
  the archive job's last run succeeded within 26 h.
- Heartbeat, cheap overlap check near the cutoff: no `tx_hash` in both tables
  for `ts` in the 2 days either side of the cutoff (0 expected).
- `storage-report.sql`: trades and trades_archive sizes, compressed chunks.

## Initial migration (production, once)

1. Fresh manual backup with the off-site copy; updated restore test passes on
   it (database now ~5.8 GB).
2. Create `trades_archive`, the view and the procedure (no job yet).
3. Move the backlog **one day per transaction**, oldest first (from the oldest stored
   day to the cutoff; ~2.6 GB on 2026-11-16), each with the same `DELETE ... RETURNING` statement.
4. Compress the archive's chunks; deploy the worker and web changes (reads
   via `trades_all`, `ARCHIVE_QUERY`); add the job.
5. Give the space back to the OS: the moved rows leave ~2.6 GB of reusable
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
- dedupe: write an archived trade again through `ARCHIVE_QUERY` (with the
  websocket's ts and with the Data API's) and through the recovery script:
  0 rows added; a genuinely new old trade: 1 row added, into a compressed chunk;
- the nightly move twice in a row: the second moves nothing.

Only then plan the production run, with the rehearsal's timings.

## Websocket vs Data API timestamps

Recorded for anything that matches trades by time: the websocket stores its
event time in milliseconds, 1-5 s **earlier** than the Data API's whole-second
timestamp for the same trade (production, 2026-11-15). Match trades by
`tx_hash`, never by exact `ts`; a time-window match needs at least +-5 s.
