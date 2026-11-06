-- Phase 4 step 1 (ROADMAP): index trades by time.
--
-- trades only had ix_trades_mkt_ts (market_id, ts), so queries that filter by
-- time across all markets scan the whole table, and that cost grows with every
-- trade (342 MB, ~74 MB/day on 2026-11-06):
--   - the bars job, every minute: WHERE t.ts >= $1 AND t.ts < $2 (65 min window)
--   - the startup gap backfill: SELECT max(ts) FROM trades
--   - storage-report.sql and the acceptance queries
--
-- CONCURRENTLY: built without blocking the worker's inserts; no restart, no
-- worker change. It can't run inside a transaction, so run this file with
-- plain psql (no -1 / --single-transaction). Idempotent: IF NOT EXISTS.
-- Expected size: ~10-20 MB at ~360k rows; seconds to build.
--
-- If the build is interrupted, Postgres leaves an INVALID index behind, which
-- IF NOT EXISTS would then skip. The last query shows indisvalid: if it says
-- false, run  DROP INDEX CONCURRENTLY ix_trades_ts;  and run this file again.
--
-- Run on the server as postgres (zleek_web is read-only), from the repo:
--   cd ~/zleek && docker compose -f docker-compose.prod.yml exec -T db \
--     psql -U postgres -d postgres -X -v ON_ERROR_STOP=1 < apps/worker/scripts/phase4_step1_trades_ts_index.sql
-- Measure before and after with phase4_step1_check.sql (read-only).

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_trades_ts ON trades (ts);

SELECT i.indexrelid::regclass AS index_name,
       i.indisvalid AS valid,
       pg_size_pretty(pg_relation_size(i.indexrelid)) AS size
FROM pg_index i
WHERE i.indexrelid = 'ix_trades_ts'::regclass;
