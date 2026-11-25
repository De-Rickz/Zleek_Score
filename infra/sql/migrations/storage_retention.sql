-- Storage step: hypertables, compression and retention for the time-series
-- tables other than trades (docs/storage-plan.md, "Retention").
--
--   table            hypertable         compress after  keep
--   ob_snapshots     converted here     3 days          30 days
--   signals_history  converted here     3 days          90 days
--   features_5m      already            3 days          30 days
--   bars_1m          already            3 days          forever
--
-- Compression: segment by market_id, order by time DESC. 3 days = the startup
-- gap backfill's reach (48 h) plus a day of margin; repairs can still UPDATE
-- compressed rows, more slowly. New chunks are 1 day so a policy acts within a
-- day of its threshold (the 7-day default would wait up to 10 days longer).
-- features_5m's existing 7-day chunks are kept; only new chunks become 1 day.
--
-- Idempotent: safe to run twice. Run it as the table owner (the human, on
-- production; this file never runs from the worker). Run in one transaction:
--   psql ... -1 -f infra/sql/migrations/storage_retention.sql
-- Converting a table takes an exclusive lock and copies its rows
-- (migrate_data), so run it in a quiet minute; see the PR for timings.
-- bars_1m is only compressed here: no retention policy, rows are never deleted.

-- 1) Hypertables (the keys (market_id, ts) already include the time column).
SELECT create_hypertable('ob_snapshots', 'ts',
       chunk_time_interval => interval '1 day', migrate_data => true, if_not_exists => true);
SELECT create_hypertable('signals_history', 'ts',
       chunk_time_interval => interval '1 day', migrate_data => true, if_not_exists => true);
SELECT set_chunk_time_interval('features_5m', interval '1 day');

-- 2) Compression settings.
ALTER TABLE ob_snapshots SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'market_id', timescaledb.compress_orderby = 'ts DESC');
ALTER TABLE signals_history SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'market_id', timescaledb.compress_orderby = 'ts DESC');
ALTER TABLE features_5m SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'market_id', timescaledb.compress_orderby = 'bucket DESC');
ALTER TABLE bars_1m SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'market_id', timescaledb.compress_orderby = 'bucket DESC');

-- 3) Compression policies.
SELECT add_compression_policy('ob_snapshots',    compress_after => interval '3 days', if_not_exists => true);
SELECT add_compression_policy('signals_history', compress_after => interval '3 days', if_not_exists => true);
SELECT add_compression_policy('features_5m',     compress_after => interval '3 days', if_not_exists => true);
SELECT add_compression_policy('bars_1m',         compress_after => interval '3 days', if_not_exists => true);

-- 4) Retention policies (whole chunks are dropped, so a table keeps up to one
-- extra chunk: a day for the 1-day chunks, up to a week for features_5m's old
-- 7-day chunks until they age out).
SELECT add_retention_policy('ob_snapshots',    drop_after => interval '30 days', if_not_exists => true);
SELECT add_retention_policy('signals_history', drop_after => interval '90 days', if_not_exists => true);
SELECT add_retention_policy('features_5m',     drop_after => interval '30 days', if_not_exists => true);
