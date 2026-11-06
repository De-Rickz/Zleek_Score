-- Phase 4 step 2a check: read-only. How much is trades being rewritten?
--
-- Postgres counts, per table since its statistics were last reset: rows
-- inserted, rows updated (each update writes a new row version), dead row
-- versions waiting for vacuum, and autovacuum runs. The counters are
-- cumulative, so run this twice about an hour apart and compare: the
-- difference divided by the hours between measured_at values is the rate.
--
-- Before 2a, every re-fetch of a trade by the backfill updates it, even with
-- the same price. After 2a (ON CONFLICT ... WHERE price IS DISTINCT FROM),
-- updates_per_hour should drop to only the trades whose price really changed
-- (the Data API's precise fill price replacing the websocket's rounded one).
-- Today the backfill covers few busy markets, so the "before" rate is small;
-- 2a matters for step 2, where it would otherwise be millions a day.
--
-- Run on the server (before the deploy, ~1 h later, then after the deploy and
-- ~1 h later):
--   cd ~/zleek && docker compose -f docker-compose.prod.yml exec -T \
--     -e PGOPTIONS='-c default_transaction_read_only=on' \
--     db psql -U zleek_web -d postgres -X < apps/worker/scripts/phase4_step2a_check.sql

SELECT now()::timestamp(0) AS measured_at_utc,
       n_tup_ins      AS rows_inserted,
       n_tup_upd      AS rows_updated,
       n_tup_hot_upd  AS hot_updates,
       n_dead_tup     AS dead_rows_now,
       n_live_tup     AS live_rows_now,
       autovacuum_count,
       last_autovacuum::timestamp(0) AS last_autovacuum_utc,
       round(100.0 * n_tup_upd / nullif(n_tup_ins, 0), 1) AS updates_per_100_inserts
FROM pg_stat_user_tables
WHERE relname = 'trades';

-- How many trades the backfill (Data API object) vs the websocket (JSON
-- string) wrote in the last hour: the backfill share grows a lot in step 2.
SELECT CASE jsonb_typeof(raw) WHEN 'string' THEN 'websocket' ELSE 'backfill' END AS source,
       count(*) AS trades_last_hour
FROM trades
WHERE ts > now() - interval '1 hour'
GROUP BY 1
ORDER BY 1;
