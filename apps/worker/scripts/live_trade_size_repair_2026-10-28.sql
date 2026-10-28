-- One-time repair: live websocket trades were stored with size_usd = 0 because
-- clob_ws.py read a "notionalUsd" field that last_trade_price events never have.
-- trades.raw holds the original event (a JSON-encoded string, hence #>> '{}'),
-- which includes size (shares) and price, so size_usd = size * price, the same
-- formula as the fixed handler and the REST backfill.
--
-- Investigated 2026-10-28 (read-only): 59,518 live rows, every one matched
-- exactly one last_trade_price event by asset_id + millisecond timestamp, all
-- with size > 0; about $26.6M of volume recovered.
--
-- Skipped on purpose:
--   * rows with a tx_hash (backfilled, or live rows after the fix): size is already correct
--   * 58 live rows whose fill was ALSO stored by the backfill (same transaction
--     hash): the backfill row carries the size, so filling these in would count
--     the fill twice. They stay at 0.
--
-- Safe to re-run: it only touches rows still at size_usd = 0.
-- Run with a role that can UPDATE trades (zleek_web cannot).

-- Before: rows to repair
SELECT count(*) AS live_rows_to_repair
FROM trades WHERE tx_hash IS NULL AND size_usd = 0 AND jsonb_typeof(raw) = 'string';

BEGIN;

UPDATE trades t
SET size_usd = (ev->>'size')::numeric * (ev->>'price')::numeric
FROM (
  SELECT id, (raw #>> '{}')::jsonb AS ev
  FROM trades
  WHERE tx_hash IS NULL AND size_usd = 0 AND jsonb_typeof(raw) = 'string'
) s
WHERE t.id = s.id
  AND s.ev->>'event_type' = 'last_trade_price'
  AND s.ev->>'asset_id' = t.asset_id
  AND (s.ev->>'timestamp')::bigint = (extract(epoch FROM t.ts) * 1000)::bigint
  AND (s.ev->>'size')::numeric > 0
  AND NOT EXISTS (SELECT 1 FROM trades b WHERE b.tx_hash = s.ev->>'transaction_hash');

COMMIT;

-- After: expect 58 left (the backfill duplicates above)
SELECT count(*) AS live_rows_still_zero
FROM trades WHERE tx_hash IS NULL AND size_usd = 0 AND jsonb_typeof(raw) = 'string';

-- OPTIONAL, separate decision: bars_1m volume is only rebuilt for the last 65
-- minutes, so older bars keep volume_usd = 0 until rebuilt. This recomputes
-- volume_usd for every existing bar from the repaired trades (other bar
-- columns are unchanged). Uncomment to run.

BEGIN;
UPDATE bars_1m b
SET volume_usd = s.vol
FROM (
  SELECT market_id, date_trunc('minute', ts) AS bucket, sum(size_usd) AS vol
  FROM trades GROUP BY 1, 2
) s
WHERE b.market_id = s.market_id AND b.bucket = s.bucket
   AND b.volume_usd IS DISTINCT FROM s.vol;
COMMIT;
