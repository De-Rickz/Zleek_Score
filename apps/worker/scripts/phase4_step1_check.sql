-- Phase 4 step 1 check: read-only. Run BEFORE and AFTER
-- phase4_step1_trades_ts_index.sql and compare.
--
-- 1. Does ix_trades_ts exist, is it valid, how big is it?
-- 2. The bars job's query (only its SELECT part: the real query is an upsert,
--    which EXPLAIN ANALYZE would execute, so it's not used here) over the same
--    65-minute window the job uses. Before: a scan of all of trades. After:
--    "Index Scan" or "Bitmap Index Scan" on ix_trades_ts, far fewer buffers.
-- 3. The gap backfill's SELECT max(ts) FROM trades. After: an index-only read.
--
-- EXPLAIN ANALYZE runs the SELECTs (that's how it times them); they only read.
-- Run on the server:
--   cd ~/zleek && docker compose -f docker-compose.prod.yml exec -T \
--     -e PGOPTIONS='-c default_transaction_read_only=on' \
--     db psql -U zleek_web -d postgres -X < apps/worker/scripts/phase4_step1_check.sql

SELECT c.relname AS index_name,
       i.indisvalid AS valid,
       pg_size_pretty(pg_relation_size(c.oid)) AS size,
       pg_size_pretty(pg_total_relation_size('trades')) AS trades_total_size
FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
WHERE c.relname IN ('ix_trades_ts', 'ix_trades_mkt_ts')
ORDER BY c.relname;

EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON)
SELECT t.market_id,
       date_trunc('minute', t.ts) AS bucket,
       (array_agg(t.price ORDER BY t.ts ASC, t.id ASC))[1] AS open,
       max(t.price) AS high,
       min(t.price) AS low,
       (array_agg(t.price ORDER BY t.ts DESC, t.id DESC))[1] AS close,
       sum(t.size_usd) AS volume_usd,
       count(*) AS trades_cnt
FROM trades t
WHERE t.ts >= now() - interval '65 minutes' AND t.ts < now() - interval '1 minute'
GROUP BY t.market_id, date_trunc('minute', t.ts);

EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON)
SELECT max(ts) FROM trades;
