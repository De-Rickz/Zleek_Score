-- trades_archive_enable.sql: the last step of the trades_archive migration
-- (docs/trades-archive-design.md). Run only after trades_archive.sql, after the
-- worker and web read through trades_all, and after the backlog move.
-- Idempotent. Run as postgres:
--   psql -U postgres -d postgres -X -v ON_ERROR_STOP=1 -f trades_archive_enable.sql

-- Compress archive chunks once their day is 5 days old: the nightly move fills
-- the day that has just passed the 4-day cutoff, so a chunk is complete before
-- it is compressed.
SELECT add_compression_policy('trades_archive', compress_after => interval '5 days',
                              if_not_exists => true);

-- The nightly move at 04:00 UTC (after the 03:20 backup). Its runs and
-- failures: SELECT * FROM timescaledb_information.job_stats
--   WHERE proc_name = 'archive_trades_job';
SELECT add_job('archive_trades_job', '1 day', initial_start => date_trunc('day', now()) + interval '1 day 4 hours')
WHERE NOT EXISTS (SELECT 1 FROM timescaledb_information.jobs WHERE proc_name = 'archive_trades_job');
