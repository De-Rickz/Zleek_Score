-- trades_archive.sql: ROADMAP phase 4 step 4 (storage), design in
-- docs/trades-archive-design.md. Creates the compressed archive, the
-- trades_all view, the store function the worker writes through, and the move
-- functions. Idempotent (safe to run twice). Moves NOTHING and schedules
-- nothing: the backlog move (archive_trades_backlog) and the nightly job and
-- compression policy (trades_archive_enable.sql) are separate steps, run only
-- after the worker and web read through trades_all.
--
-- Run as postgres (the owner of trades), in one transaction:
--   psql -U postgres -d postgres -X -v ON_ERROR_STOP=1 -1 -f trades_archive.sql

-- Cutoff: rows with ts before this live in trades_archive, the rest in trades.
-- 4 full UTC days: further back than anything the worker re-fetches (step 2c's
-- 24 hours, the gap backfill's 48) plus a day. One definition, used by the
-- writer's store function and by the move, so both use the database's clock.
CREATE OR REPLACE FUNCTION archive_cutoff() RETURNS timestamptz
LANGUAGE sql STABLE AS $$ SELECT date_trunc('day', now()) - interval '4 days' $$;

CREATE TABLE IF NOT EXISTS trades_archive (
  id bigint NOT NULL,                        -- the id it had in trades (no default)
  market_id text REFERENCES markets(id),
  asset_id text,
  ts timestamptz NOT NULL,
  price numeric NOT NULL,
  size_usd numeric NOT NULL,
  side text,
  maker_wallet text,
  taker_wallet text,
  tx_hash text,                              -- NOT unique: dedupe is zleek_store_trade's job
  raw jsonb,
  status text,
  market_order_id text,
  match_time timestamptz
);
SELECT create_hypertable('trades_archive', 'ts', chunk_time_interval => interval '1 day',
                         create_default_indexes => false, if_not_exists => true);
CREATE INDEX IF NOT EXISTS ix_trades_archive_mkt_ts ON trades_archive (market_id, ts DESC);
CREATE INDEX IF NOT EXISTS ix_trades_archive_tx_hash ON trades_archive (tx_hash);
-- Columnar compression per market, newest first (the dashboard reads a
-- market's latest trades). Policy added by trades_archive_enable.sql.
ALTER TABLE trades_archive SET (timescaledb.compress,
                                timescaledb.compress_segmentby = 'market_id',
                                timescaledb.compress_orderby = 'ts DESC');

-- Every history reader uses this. A row is in exactly one table at any moment
-- (the move is one statement), so UNION ALL never counts a trade twice.
CREATE OR REPLACE VIEW trades_all AS
  SELECT id, market_id, asset_id, ts, price, size_usd, side, maker_wallet, taker_wallet,
         tx_hash, raw, status, market_order_id, match_time FROM trades
  UNION ALL
  SELECT id, market_id, asset_id, ts, price, size_usd, side, maker_wallet, taker_wallet,
         tx_hash, raw, status, market_order_id, match_time FROM trades_archive;

-- The writer's REST path (any trade with a tx_hash). Recent trades: exactly
-- the previous REST_QUERY (ON CONFLICT (tx_hash), price updated only when it
-- changed). Trades older than the cutoff (only the recovery script or a repair
-- writes those): into trades_archive, unless either table already holds the
-- tx_hash. The archive lookup is narrowed to the market and +-1 minute (the
-- websocket's ts is 1-5 s off the Data API's for the same trade), so only one
-- segment of one chunk is decompressed; the advisory lock serializes two
-- writers of the same old trade. Returns where the row went: 'trades',
-- 'archive' or 'duplicate'.
CREATE OR REPLACE FUNCTION zleek_store_trade(
  p_market_id text, p_asset_id text, p_ts timestamptz, p_price numeric, p_size_usd numeric,
  p_side text, p_maker_wallet text, p_taker_wallet text, p_tx_hash text, p_raw jsonb,
  p_status text, p_market_order_id text, p_match_time timestamptz)
RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  IF p_ts >= archive_cutoff() THEN
    INSERT INTO trades(market_id, asset_id, ts, price, size_usd, side, maker_wallet, taker_wallet,
                       tx_hash, raw, status, market_order_id, match_time)
    VALUES (p_market_id, p_asset_id, p_ts, p_price, p_size_usd, p_side, p_maker_wallet, p_taker_wallet,
            p_tx_hash, p_raw, p_status, p_market_order_id, p_match_time)
    ON CONFLICT (tx_hash) DO UPDATE SET price = EXCLUDED.price
    WHERE trades.price IS DISTINCT FROM EXCLUDED.price;
    RETURN 'trades';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(coalesce(p_tx_hash, ''), 0));
  IF EXISTS (SELECT 1 FROM trades WHERE tx_hash = p_tx_hash)
     OR EXISTS (SELECT 1 FROM trades_archive a
                WHERE a.market_id IS NOT DISTINCT FROM p_market_id AND a.tx_hash = p_tx_hash
                  AND a.ts BETWEEN p_ts - interval '1 minute' AND p_ts + interval '1 minute') THEN
    RETURN 'duplicate';
  END IF;
  INSERT INTO trades_archive(id, market_id, asset_id, ts, price, size_usd, side, maker_wallet,
                             taker_wallet, tx_hash, raw, status, market_order_id, match_time)
  VALUES (nextval(pg_get_serial_sequence('trades', 'id')), p_market_id, p_asset_id, p_ts, p_price,
          p_size_usd, p_side, p_maker_wallet, p_taker_wallet, p_tx_hash, p_raw, p_status,
          p_market_order_id, p_match_time);
  RETURN 'archive';
END $$;

-- The move: every trades row older than p_cutoff into trades_archive, in ONE
-- statement (so one transaction: no moment where a row is in both tables or
-- neither). Returns the number of rows moved.
CREATE OR REPLACE FUNCTION archive_trades_move(p_cutoff timestamptz) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE n bigint;
BEGIN
  WITH moved AS (
    DELETE FROM trades WHERE ts < p_cutoff
    RETURNING id, market_id, asset_id, ts, price, size_usd, side, maker_wallet, taker_wallet,
              tx_hash, raw, status, market_order_id, match_time)
  INSERT INTO trades_archive(id, market_id, asset_id, ts, price, size_usd, side, maker_wallet,
                             taker_wallet, tx_hash, raw, status, market_order_id, match_time)
  SELECT * FROM moved;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END $$;

-- Nightly job body (scheduled by trades_archive_enable.sql with add_job).
CREATE OR REPLACE PROCEDURE archive_trades_job(job_id int, config jsonb)
LANGUAGE plpgsql AS $$
DECLARE n bigint;
BEGIN
  n := archive_trades_move(archive_cutoff());
  RAISE LOG 'archive_trades_job: moved % trades older than %', n, archive_cutoff();
END $$;

-- The initial backlog, ONE UTC DAY PER TRANSACTION, oldest first, up to the
-- cutoff. Run outside a transaction block (CALL commits after each day):
--   psql -U postgres -d postgres -X -v ON_ERROR_STOP=1 -c 'CALL archive_trades_backlog()'
-- Stops and can be re-run at any point: each day is either moved or not.
CREATE OR REPLACE PROCEDURE archive_trades_backlog()
LANGUAGE plpgsql AS $$
DECLARE d timestamptz; n bigint; t0 timestamptz;
BEGIN
  SELECT date_trunc('day', min(ts)) INTO d FROM trades;
  WHILE d IS NOT NULL AND d < archive_cutoff() LOOP
    t0 := clock_timestamp();
    n := archive_trades_move(least(d + interval '1 day', archive_cutoff()));
    COMMIT;
    RAISE NOTICE 'archive_trades_backlog: % moved % rows in % s', d::date, n,
      round(extract(epoch FROM clock_timestamp() - t0)::numeric, 1);
    d := d + interval '1 day';
  END LOOP;
END $$;

-- ROLLBACK ONLY (docs/trades-archive-runbook.md): move every archived row back
-- into trades, ONE UTC DAY PER TRANSACTION, newest first, so code that reads
-- only trades sees full history again. Unschedule archive_trades_job first.
-- A plain INSERT (no ON CONFLICT): a tx_hash already in trades aborts that day
-- instead of silently dropping a row. Run outside a transaction block:
--   CALL archive_trades_unarchive()
CREATE OR REPLACE PROCEDURE archive_trades_unarchive()
LANGUAGE plpgsql AS $$
DECLARE d timestamptz; n bigint; t0 timestamptz;
BEGIN
  IF EXISTS (SELECT 1 FROM timescaledb_information.jobs WHERE proc_name = 'archive_trades_job') THEN
    RAISE EXCEPTION 'archive_trades_job is still scheduled: unschedule it first (SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE proc_name = %)', quote_literal('archive_trades_job');
  END IF;
  SELECT date_trunc('day', max(ts)) INTO d FROM trades_archive;
  WHILE d IS NOT NULL LOOP
    t0 := clock_timestamp();
    WITH back AS (
      DELETE FROM trades_archive WHERE ts >= d AND ts < d + interval '1 day'
      RETURNING id, market_id, asset_id, ts, price, size_usd, side, maker_wallet, taker_wallet,
                tx_hash, raw, status, market_order_id, match_time)
    INSERT INTO trades(id, market_id, asset_id, ts, price, size_usd, side, maker_wallet, taker_wallet,
                       tx_hash, raw, status, market_order_id, match_time)
    SELECT * FROM back;
    GET DIAGNOSTICS n = ROW_COUNT;
    COMMIT;
    RAISE NOTICE 'archive_trades_unarchive: % moved back % rows in % s', d::date, n,
      round(extract(epoch FROM clock_timestamp() - t0)::numeric, 1);
    SELECT date_trunc('day', max(ts)) INTO d FROM trades_archive;
  END LOOP;
END $$;

-- The read-only role (web, heartbeat, ops queries) reads the archive and view.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'zleek_web') THEN
    GRANT SELECT ON trades_archive, trades_all TO zleek_web;
  END IF;
END $$;
