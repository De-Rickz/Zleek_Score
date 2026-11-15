-- storage-step-check.sql: read-only baseline for ROADMAP phase 4 step 4
-- (storage). Run once before the storage work starts and again after each
-- storage change, next to storage-report.sql. Counts and sizes only.
--
-- Run on the server, from the repo:
--   cd ~/zleek && docker compose -f docker-compose.prod.yml exec -T \
--     -e PGOPTIONS='-c default_transaction_read_only=on' \
--     db psql -U zleek_web -d postgres -X < apps/worker/scripts/storage-step-check.sql
-- (If zleek_web lacks a privilege for a statistics view or size function,
-- use -U postgres: the read-only transaction still applies.)

SELECT now()::timestamp(0) AS measured_at_utc,
       (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb') AS timescaledb_version,
       (SELECT stats_reset::timestamp(0) FROM pg_stat_database WHERE datname = current_database())
         AS stats_since;

-- 1) Row churn and vacuum per table, since stats_since. For markets, n_tup_upd
--    per day is the Gamma refresher's rewrite rate (~2,100 rows every 10 min
--    = ~300,000/day expected); n_dead_tup is what autovacuum hasn't reclaimed.
SELECT relname AS table_name,
       n_live_tup, n_dead_tup,
       n_tup_ins, n_tup_upd, n_tup_hot_upd, n_tup_del,
       autovacuum_count, last_autovacuum::timestamp(0), last_autoanalyze::timestamp(0)
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY n_tup_upd + n_tup_ins DESC;

-- 2) Heap vs TOAST vs indexes for the plain (non-hypertable) tables: where
--    the bytes of markets (raw JSON) and ob_snapshots (book JSON) sit.
SELECT c.relname AS table_name,
       pg_size_pretty(pg_relation_size(c.oid)) AS heap,
       pg_size_pretty(COALESCE(pg_total_relation_size(NULLIF(c.reltoastrelid, 0)), 0)) AS toast,
       pg_size_pretty(pg_indexes_size(c.oid)) AS indexes,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total,
       c.reloptions AS storage_options
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
LEFT JOIN timescaledb_information.hypertables h
       ON h.hypertable_schema = 'public' AND h.hypertable_name = c.relname
WHERE c.relkind = 'r' AND h.hypertable_name IS NULL
ORDER BY pg_total_relation_size(c.oid) DESC;

-- 3) markets: how many rows the refresher keeps fresh, and what a row's raw
--    JSON is made of (the nested events are most of it). Sizes are as stored
--    (after TOAST compression), so compare them with each other, not with
--    the ~7.3 KB uncompressed payload measured from Gamma.
SELECT count(*) AS markets_rows,
       count(*) FILTER (WHERE updated_at > now() - interval '2 hours') AS refreshed_last_2h,
       count(*) FILTER (WHERE status = 'open') AS open_rows,
       pg_size_pretty(avg(pg_column_size(raw))::bigint) AS raw_avg_stored,
       pg_size_pretty(sum(pg_column_size(raw))::bigint) AS raw_total_stored,
       pg_size_pretty(avg(length(raw::text))::bigint) AS raw_avg_text,
       pg_size_pretty(avg(length((raw::jsonb -> 'events')::text))::bigint) AS events_avg_text
FROM markets;

-- 4) Hypertables: chunk interval, chunk count, compression on or off and
--    how many chunks are compressed (all expected off before the step).
SELECT h.hypertable_name,
       d.column_name AS time_column,
       d.time_interval AS chunk_interval,
       h.num_chunks,
       h.compression_enabled,
       (SELECT count(*) FROM timescaledb_information.chunks ch
         WHERE ch.hypertable_name = h.hypertable_name AND ch.is_compressed) AS compressed_chunks
FROM timescaledb_information.hypertables h
JOIN timescaledb_information.dimensions d
  ON d.hypertable_name = h.hypertable_name AND d.dimension_number = 1
WHERE h.hypertable_schema = 'public'
ORDER BY h.hypertable_name;

-- 5) Background jobs (compression and retention policies; none expected yet
--    besides TimescaleDB's own).
SELECT job_id, application_name, hypertable_name, schedule_interval, config
FROM timescaledb_information.jobs
ORDER BY job_id;

-- 6) How the big JSON columns are stored (TOAST compression method; empty =
--    the server default, pglz) and the default itself.
SELECT c.relname AS table_name, a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS type,
       a.attstorage AS storage, NULLIF(a.attcompression, '') AS compression
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
WHERE c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
  AND format_type(a.atttypid, a.atttypmod) IN ('json', 'jsonb', 'text')
  AND c.relname IN ('trades', 'markets', 'ob_snapshots')
ORDER BY 1, 2;
SHOW default_toast_compression;
