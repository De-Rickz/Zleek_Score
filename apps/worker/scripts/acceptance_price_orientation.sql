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

-- B) Dashboard scores (scoring calibration, 2026-10-30). The tremor score is
--    the price z-score only: min(|z| / 3, 1) * 100 from the newest features row
--    at or before the score's timestamp (NULL z = 0). Also checks that only
--    eligible markets are on the dashboard: open, and refreshed by Gamma in the
--    2h or traded in the 24h before the row was scored (same rule as
--    signals.features.eligible_market_ids). Eligibility is judged as of the
--    scoring time, not now(): the windows slide, so a market can age out
--    between 5-minute tremor runs and is removed on the next one. stale_scores
--    catches the other failure, a row the tremor job stopped updating.
--    (status and updated_at are current values, so a market closed or
--    refreshed since its last scoring run can still differ for up to one run.)
--    Expect score_mismatch = 0, ineligible_on_dashboard = 0, stale_scores = 0.
--    over_50 is a tracking number, not a pass/fail: it was 97 of 268 (36%) on
--    the old formula and 7 of 211 after deploy (2026-10-30).
SELECT count(*)                                                       AS scored_markets,
       count(*) FILTER (WHERE l.tremor_score > 50)                    AS over_50,
       round(100.0 * count(*) FILTER (WHERE l.tremor_score > 50) / nullif(count(*), 0), 1) AS over_50_pct,
       count(*) FILTER (WHERE abs(l.tremor_score
             - round(least(abs(coalesce(f.price_z_score, 0)) / 3.0, 1) * 100, 2)) > 0.01) AS score_mismatch,
       count(*) FILTER (WHERE NOT (m.status = 'open'
             AND (m.updated_at > l.ts - interval '2 hours'
                  OR EXISTS (SELECT 1 FROM trades t WHERE t.market_id = l.market_id
                             AND t.ts > l.ts - interval '24 hours' AND t.ts <= l.ts)))) AS ineligible_on_dashboard,
       count(*) FILTER (WHERE l.ts < now() - interval '10 minutes')  AS stale_scores
FROM signals_latest l
JOIN markets m ON m.id = l.market_id
LEFT JOIN LATERAL (SELECT price_z_score FROM features_5m f
                   WHERE f.market_id = l.market_id AND f.bucket <= l.ts
                   ORDER BY f.bucket DESC LIMIT 1) f ON true;

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
--    Price tolerance is one tick (0.001), not exact: when the backfill
--    re-fetches a live fill, REST_QUERY's ON CONFLICT updates price with the
--    Data API's more precise fill price while raw keeps the websocket's rounded
--    one (2026-10-31: 237 of 305k live rows, max difference 0.00022). An
--    orientation error is off by |1 - 2p|, far more than a tick unless p is
--    within 0.0005 of 0.5, where orientation doesn't matter.
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
       count(*) FILTER (WHERE abs(price - yes_price) > 0.001)         AS price_wrong,
       count(*) FILTER (WHERE side IS DISTINCT FROM yes_side)         AS side_wrong
FROM e;

-- E) Order-book snapshots written after the order-book cutoff must be in YES
--    terms: no crossed books (best_bid > best_ask), best_bid/best_ask agreeing
--    with the stored bids/asks JSON, and mids on the same side of the market as
--    the last trade at or before the snapshot (if that trade is under 10
--    minutes old). Expect 0 for all three. (It used the same-minute bar close
--    until 2026-10-31, which can come after the snapshot: a market jumping
--    mid-minute, e.g. a goal, looked like an orientation error.)
--    Cutoff = 2026-10-30 17:24:17 UTC, the first websocket subscribe after the
--    token-column repair. Snapshots before it can't be repaired (no asset_id or
--    raw column): before the 17:06:04 restart ~97% are crossed, and the 65
--    written 17:09:08-17:24:08 were converted with the still-reversed mapping.
WITH s AS (
  SELECT o.market_id, o.ts, o.best_bid, o.best_ask,
         (SELECT max((l->>'price')::numeric) FROM jsonb_array_elements(o.bids) l) AS json_best_bid,
         (SELECT min((l->>'price')::numeric) FROM jsonb_array_elements(o.asks) l) AS json_best_ask,
         lt.price AS last_trade
  FROM ob_snapshots o
  LEFT JOIN LATERAL (SELECT price FROM trades t
                     WHERE t.market_id = o.market_id AND t.ts <= o.ts AND t.ts > o.ts - interval '10 minutes'
                     ORDER BY t.ts DESC LIMIT 1) lt ON true
  WHERE o.ts >= '2026-10-30 17:24:17+00'   -- order-book cutoff (see above)
)
SELECT count(*)                                                                   AS snapshots,
       count(*) FILTER (WHERE best_bid > 0 AND best_ask > 0 AND best_bid > best_ask) AS crossed,
       count(*) FILTER (WHERE json_best_bid IS DISTINCT FROM best_bid::numeric
                           AND abs(coalesce(json_best_bid, 0) - best_bid::numeric) > 1e-9) AS best_bid_mismatch,
       count(*) FILTER (WHERE last_trade IS NOT NULL AND best_bid > 0 AND best_ask > 0
                           AND abs((best_bid + best_ask) / 2 - last_trade) > 0.5)   AS mid_far_from_last_trade,
       count(*) FILTER (WHERE last_trade IS NOT NULL AND best_bid > 0 AND best_ask > 0) AS comparable
FROM s;
