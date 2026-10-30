-- Read-only acceptance tests for the trade price-orientation fix.
-- Run before and after the fix; compare. Both are SELECTs only.

-- A) One-minute bars in the last 24h whose close jumps by more than 0.5 from
--    the previous bar of the same market (mixed outcome prices zigzag p <-> 1-p).
--    Real jumps (resolution spikes, big news) will remain after the fix.
WITH b AS (
  SELECT market_id, close,
         lag(close) OVER (PARTITION BY market_id ORDER BY bucket) AS prev
  FROM bars_1m
  WHERE bucket > now() - interval '24 hours'
)
SELECT count(*)                                                  AS bars,
       count(*) FILTER (WHERE abs(close - prev) > 0.5)           AS jumps_over_half,
       round(100.0 * count(*) FILTER (WHERE abs(close - prev) > 0.5) / nullif(count(*), 0), 1) AS jump_pct,
       count(DISTINCT market_id) FILTER (WHERE abs(close - prev) > 0.5) AS markets_with_jumps,
       count(DISTINCT market_id)                                 AS markets
FROM b
WHERE prev IS NOT NULL;

-- B) Markets currently scored, and how many are above the "tremor detected"
--    threshold of 50 (inflated by the zigzag z-scores).
SELECT count(*)                                   AS scored_markets,
       count(*) FILTER (WHERE tremor_score > 50)  AS over_50,
       round(100.0 * count(*) FILTER (WHERE tremor_score > 50) / nullif(count(*), 0), 1) AS over_50_pct
FROM signals_latest;

-- C) Token labels: token_yes_id must be clobTokenIds[0] (outcome 0, "Yes" on
--    Yes/No markets). Catches the reversed labels stored until 2026-10-30.
--    Expect 0 rows wrong after the token-column repair.
SELECT count(*)                                                                         AS markets,
       count(*) FILTER (WHERE token_yes_id IS DISTINCT FROM (raw->>'clobTokenIds')::jsonb->>0
                           OR token_no_id  IS DISTINCT FROM (raw->>'clobTokenIds')::jsonb->>1) AS labels_wrong
FROM markets;

-- D) Trade orientation: stored price/side must equal the YES-terms value
--    recomputed from the trade's own raw payload (live rows store the
--    websocket message as a JSON string; backfill rows store the Data API
--    object). Orientation comes from the market's clobTokenIds, not from the
--    token columns. Expect 0 wrong after the trade-price repair.
WITH t AS (
  SELECT t.price, t.side, t.asset_id,
         CASE WHEN jsonb_typeof(t.raw) = 'string' THEN (t.raw #>> '{}')::jsonb ELSE t.raw END AS r,
         (m.raw->>'clobTokenIds')::jsonb->>0 AS yes_token
  FROM trades t JOIN markets m ON m.id = t.market_id
), e AS (
  SELECT price, side,
         CASE WHEN asset_id = yes_token THEN (r->>'price')::numeric ELSE 1 - (r->>'price')::numeric END AS yes_price,
         CASE WHEN asset_id = yes_token THEN r->>'side'
              ELSE CASE r->>'side' WHEN 'BUY' THEN 'SELL' WHEN 'SELL' THEN 'BUY' ELSE r->>'side' END END AS yes_side
  FROM t
)
SELECT count(*)                                                       AS trades,
       count(*) FILTER (WHERE yes_price IS NULL)                      AS no_raw_price,
       count(*) FILTER (WHERE abs(price - yes_price) > 1e-6)          AS price_wrong,
       count(*) FILTER (WHERE side IS DISTINCT FROM yes_side)         AS side_wrong
FROM e;

-- E) Order-book snapshots (last 24h) must be in YES terms. The old code swapped
--    bids/asks of the wrong token without repricing, which leaves two
--    signatures: crossed books (best_bid > best_ask), and books whose mid sits
--    on the other side of the market (|mid - same-minute bar close| > 0.5).
--    best_bid/best_ask must also agree with the stored bids/asks JSON.
--    The mid-vs-close check is only meaningful once trades and bars are in
--    YES terms (after the trade repair and bars rebuild). Expect ~0 for
--    snapshots written after the restart; real gaps (empty side, thin book)
--    can still trip the mid check occasionally.
WITH s AS (
  SELECT o.market_id, o.ts, o.best_bid, o.best_ask,
         (SELECT max((l->>'price')::numeric) FROM jsonb_array_elements(o.bids) l) AS json_best_bid,
         (SELECT min((l->>'price')::numeric) FROM jsonb_array_elements(o.asks) l) AS json_best_ask,
         b.close
  FROM ob_snapshots o
  LEFT JOIN bars_1m b ON b.market_id = o.market_id AND b.bucket = date_trunc('minute', o.ts)
  WHERE o.ts > now() - interval '24 hours'
)
SELECT count(*)                                                                   AS snapshots,
       count(*) FILTER (WHERE best_bid > 0 AND best_ask > 0 AND best_bid > best_ask) AS crossed,
       count(*) FILTER (WHERE json_best_bid IS DISTINCT FROM best_bid::numeric
                           AND abs(coalesce(json_best_bid, 0) - best_bid::numeric) > 1e-9) AS best_bid_mismatch,
       count(*) FILTER (WHERE close IS NOT NULL AND best_bid > 0 AND best_ask > 0
                           AND abs((best_bid + best_ask) / 2 - close) > 0.5)        AS mid_far_from_close,
       count(*) FILTER (WHERE close IS NOT NULL AND best_bid > 0 AND best_ask > 0)  AS comparable
FROM s;
