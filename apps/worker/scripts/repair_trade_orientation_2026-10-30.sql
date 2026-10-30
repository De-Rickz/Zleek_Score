-- One-time repair: express every trade in outcome-0 ("YES") terms.
-- Trades were stored at their own token's price, so YES (p) and NO (~1-p)
-- trades shared a market's bars and the close zigzagged. The fixed worker
-- (branch fix/outcome-orientation) converts at write time via
-- writer.to_yes_terms; this fixes rows written before that.
--
-- For every trade, recomputes from the trade's own raw payload (live rows
-- store the websocket message as a JSON string, backfill rows the Data API
-- object) and the market's stored clobTokenIds:
--   asset = clobTokenIds[0]  ->  price = raw price,      side = raw side
--   asset = clobTokenIds[1]  ->  price = 1 - raw price,  side = BUY<->SELL
-- It never reads the current price/side, and orientation never comes from the
-- token columns, so it cannot flip a correct row back, is idempotent (a second
-- run updates 0 rows), and is safe before or after the worker restart.
-- size_usd is not touched: it is already size x raw token price (USD paid).
--
-- Why it is run twice (deploy order): after the restart, the backfill re-fetches
-- the last hour every 5 min and REST_QUERY's ON CONFLICT (tx_hash) overwrites
-- price using the token mapping loaded at that moment, and the websocket keeps
-- the mapping it loaded at connect time. Until repair_token_columns has run and
-- the websocket has resubscribed (watch_subscription, <= 5 min, or any
-- reconnect), some new rows can be written with the wrong orientation. Run
-- this once after the token-column repair, and once more after the websocket
-- has resubscribed ("Subscribed to N assets" in worker.log after the repair).
--
-- Dry run: acceptance query D in acceptance_price_orientation.sql (read-only).
-- Baseline 2026-10-30: 107,152 prices / 109,344 sides wrong of 260,889;
-- 0 trades without a raw price. The extra side mismatches are NO trades at
-- exactly 0.5, where only the side changes.
--
-- ob_snapshots are NOT repaired (no asset_id or raw column, so they can't be
-- recomputed from source). Cutoff = the worker restart time that deploys
-- branch fix/outcome-orientation: snapshots before it are in mixed
-- orientation (~97% have only bid/ask sides swapped, ~3% are NO books in NO
-- terms); snapshots after it are in YES terms. Record the exact time here and
-- in infra/TODO.md after restarting:
--   CUTOFF (UTC): ______________________
--   (last "Starting main application" line in apps/worker/ingestion/worker.log)
--
-- Run with a role that can UPDATE trades (zleek_web cannot).

-- Before
WITH e AS (
  SELECT t.price, t.side,
         CASE WHEN t.asset_id = (m.raw->>'clobTokenIds')::jsonb->>0
              THEN (CASE WHEN jsonb_typeof(t.raw) = 'string' THEN (t.raw #>> '{}')::jsonb ELSE t.raw END ->> 'price')::numeric
              ELSE 1 - (CASE WHEN jsonb_typeof(t.raw) = 'string' THEN (t.raw #>> '{}')::jsonb ELSE t.raw END ->> 'price')::numeric END AS yes_price
  FROM trades t JOIN markets m ON m.id = t.market_id)
SELECT count(*) AS trades, count(*) FILTER (WHERE abs(price - yes_price) > 1e-6) AS price_wrong
FROM e;

BEGIN;

UPDATE trades t
SET price = s.yes_price,
    side  = s.yes_side
FROM (
  SELECT t.id,
         CASE WHEN t.asset_id = ids->>0 THEN raw_price ELSE 1 - raw_price END AS yes_price,
         CASE WHEN t.asset_id = ids->>0 THEN raw_side
              ELSE CASE raw_side WHEN 'BUY' THEN 'SELL' WHEN 'SELL' THEN 'BUY' ELSE raw_side END END AS yes_side
  FROM trades t
  JOIN markets m ON m.id = t.market_id
  CROSS JOIN LATERAL (
    SELECT (m.raw->>'clobTokenIds')::jsonb AS ids,
           (CASE WHEN jsonb_typeof(t.raw) = 'string' THEN (t.raw #>> '{}')::jsonb ELSE t.raw END ->> 'price')::numeric AS raw_price,
           (CASE WHEN jsonb_typeof(t.raw) = 'string' THEN (t.raw #>> '{}')::jsonb ELSE t.raw END ->> 'side') AS raw_side
  ) r
  WHERE t.asset_id IN (ids->>0, ids->>1)
    AND raw_price IS NOT NULL
) s
WHERE t.id = s.id
  AND (abs(t.price - s.yes_price) > 1e-6 OR t.side IS DISTINCT FROM s.yes_side);

COMMIT;

-- After: expect price_wrong = 0 (acceptance query D also checks side)
WITH e AS (
  SELECT t.price, t.side,
         CASE WHEN t.asset_id = (m.raw->>'clobTokenIds')::jsonb->>0
              THEN (CASE WHEN jsonb_typeof(t.raw) = 'string' THEN (t.raw #>> '{}')::jsonb ELSE t.raw END ->> 'price')::numeric
              ELSE 1 - (CASE WHEN jsonb_typeof(t.raw) = 'string' THEN (t.raw #>> '{}')::jsonb ELSE t.raw END ->> 'price')::numeric END AS yes_price
  FROM trades t JOIN markets m ON m.id = t.market_id)
SELECT count(*) AS trades, count(*) FILTER (WHERE abs(price - yes_price) > 1e-6) AS price_wrong
FROM e;
