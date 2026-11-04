-- storage-report.sql: daily storage report. Read-only (sizes and counts only).
-- Sizes per table, the whole database, rows per day for the last 3 days, and
-- bytes per row (on disk, including indexes; and the row itself, including
-- how much of a trade row is the raw JSON and of a book snapshot the levels).
--
-- Run on the server, from the repo:
--   cd ~/zleek && docker compose -f docker-compose.prod.yml exec -T \
--     -e PGOPTIONS='-c default_transaction_read_only=on' \
--     db psql -U zleek_web -d postgres -X < apps/worker/scripts/storage-report.sql
-- (If zleek_web lacks a privilege for one of the size functions, use
-- -U postgres: the read-only transaction still applies.)

-- 1) Size of each table (hypertables include all their chunks) and the database.
SELECT c.relname AS table_name,
       h.hypertable_name IS NOT NULL AS hypertable,
       pg_size_pretty(CASE WHEN h.hypertable_name IS NOT NULL THEN hypertable_size(c.oid)
                           ELSE pg_total_relation_size(c.oid) END) AS total_size,
       pg_size_pretty(pg_indexes_size(c.oid)) AS indexes_size_parent_only
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
LEFT JOIN timescaledb_information.hypertables h
       ON h.hypertable_schema = 'public' AND h.hypertable_name = c.relname
WHERE c.relkind IN ('r', 'p')
ORDER BY CASE WHEN h.hypertable_name IS NOT NULL THEN hypertable_size(c.oid)
              ELSE pg_total_relation_size(c.oid) END DESC;

SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size,
       now()::timestamp(0) AS measured_at_utc;

-- 2) Rows per day, last 3 full UTC days plus today so far.
WITH d AS (
  SELECT 'trades' AS table_name, ts::date AS day, count(*) AS rows FROM trades
   WHERE ts >= current_date - 3 GROUP BY 2
  UNION ALL SELECT 'bars_1m', bucket::date, count(*) FROM bars_1m
   WHERE bucket >= current_date - 3 GROUP BY 2
  UNION ALL SELECT 'features_5m', bucket::date, count(*) FROM features_5m
   WHERE bucket >= current_date - 3 GROUP BY 2
  UNION ALL SELECT 'signals_history', ts::date, count(*) FROM signals_history
   WHERE ts >= current_date - 3 GROUP BY 2
  UNION ALL SELECT 'ob_snapshots', ts::date, count(*) FROM ob_snapshots
   WHERE ts >= current_date - 3 GROUP BY 2
)
SELECT table_name, day, rows, day = current_date AS partial_day
FROM d ORDER BY table_name, day;

-- 3) Bytes per row. on_disk = total size (with indexes) / row count, which is
-- what drives growth; row_avg = average stored row from yesterday's rows.
WITH sizes AS (
  SELECT c.relname AS table_name,
         CASE WHEN h.hypertable_name IS NOT NULL THEN hypertable_size(c.oid)
              ELSE pg_total_relation_size(c.oid) END AS bytes,
         CASE WHEN h.hypertable_name IS NOT NULL THEN approximate_row_count(c.oid)
              ELSE greatest(c.reltuples, 0)::bigint END AS approx_rows
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
  LEFT JOIN timescaledb_information.hypertables h
         ON h.hypertable_schema = 'public' AND h.hypertable_name = c.relname
  WHERE c.relname IN ('trades', 'bars_1m', 'features_5m', 'signals_history', 'ob_snapshots')
),
rowsz AS (
  SELECT 'trades' AS table_name, avg(pg_column_size(t.*)) AS row_avg,
         avg(pg_column_size(t.raw)) AS part_avg, 'raw JSON' AS part
  FROM trades t WHERE ts >= current_date - 1 AND ts < current_date
  UNION ALL SELECT 'bars_1m', avg(pg_column_size(b.*)), NULL, NULL
  FROM bars_1m b WHERE bucket >= current_date - 1 AND bucket < current_date
  UNION ALL SELECT 'features_5m', avg(pg_column_size(f.*)), NULL, NULL
  FROM features_5m f WHERE bucket >= current_date - 1 AND bucket < current_date
  UNION ALL SELECT 'signals_history', avg(pg_column_size(s.*)), NULL, NULL
  FROM signals_history s WHERE ts >= current_date - 1 AND ts < current_date
  UNION ALL SELECT 'ob_snapshots', avg(pg_column_size(o.*)),
         avg(pg_column_size(o.bids) + pg_column_size(o.asks)), 'bids + asks JSON'
  FROM ob_snapshots o WHERE ts >= current_date - 1 AND ts < current_date
)
SELECT s.table_name, s.approx_rows,
       round(s.bytes::numeric / nullif(s.approx_rows, 0)) AS on_disk_bytes_per_row,
       round(r.row_avg) AS row_avg_bytes,
       r.part, round(r.part_avg) AS part_avg_bytes,
       round(100 * r.part_avg / nullif(r.row_avg, 0)) AS part_pct_of_row
FROM sizes s JOIN rowsz r USING (table_name)
ORDER BY s.bytes DESC;
