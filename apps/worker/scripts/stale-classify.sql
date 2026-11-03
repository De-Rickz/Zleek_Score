-- stale-classify.sql: classify the top 100 markets by tremor score. Read-only.
--   (a) active: a trade in the last hour
--   (b) quiet, covered: no trade in the last hour, not ended, and currently
--       selected by the websocket or the backfill
--   (c) ended: closed, resolved, or past its Gamma endDate
--   (d) not covered: open, but selected by neither the websocket nor the backfill
-- The selection rules copy the worker (apps/worker/ingestion/writer.py):
--   websocket: open, liquidity >= $10k, Gamma refresh within 2 h, top 20 by 24h volume
--   backfill:  open, liquidity >= $10k, top 200 conditions by liquidity, no freshness check
-- Columns for (d): ws_cap_100_would_add / ws_cap_200_would_add = covered if the
-- websocket cap were raised to 100 / 200; vol_rank_over_200 = only a backfill
-- ranked by recent volume would reach them.
--
-- Run on the server, from the repo (after git pull; file piped in on stdin):
--   cd ~/zleek && docker compose -f docker-compose.prod.yml exec -T \
--     -e PGOPTIONS='-c default_transaction_read_only=on' \
--     db psql -U zleek_web -d postgres -X < apps/worker/scripts/stale-classify.sql

WITH top AS (
  SELECT market_id FROM signals_latest ORDER BY tremor_score DESC LIMIT 100
),
elig AS (
  SELECT id, condition_id, (raw->>'volume24hr')::float8 AS vol24, liquidity_usd
  FROM markets
  WHERE status = 'open' AND liquidity_usd >= 10000
),
ws AS (
  SELECT e.id, rank() OVER (ORDER BY e.vol24 DESC NULLS LAST) AS vol_rank
  FROM elig e JOIN markets m ON m.id = e.id
  WHERE m.updated_at > now() - interval '2 hours'
),
bf AS (
  SELECT condition_id, rank() OVER (ORDER BY max(liquidity_usd) DESC) AS liq_rank
  FROM elig
  WHERE condition_id IS NOT NULL
    AND id NOT IN (SELECT id FROM ws WHERE vol_rank <= 20)
  GROUP BY condition_id
),
c AS (
  SELECT t.market_id,
         m.status IN ('closed', 'resolved')
           OR (m.raw->>'endDate' ~ '^\d{4}-' AND (m.raw->>'endDate')::timestamptz < now()) AS ended,
         EXISTS (SELECT 1 FROM trades x
                 WHERE x.market_id = t.market_id AND x.ts > now() - interval '1 hour') AS traded_1h,
         ws.vol_rank,
         bf.liq_rank,
         m.liquidity_usd >= 10000 AS liq_ok,
         m.updated_at > now() - interval '2 hours' AS gamma_fresh
  FROM top t
  JOIN markets m ON m.id = t.market_id
  LEFT JOIN ws ON ws.id = t.market_id
  LEFT JOIN bf ON bf.condition_id = m.condition_id
),
k AS (
  SELECT *,
         CASE WHEN ended THEN 'c ended'
              WHEN traded_1h THEN 'a active'
              WHEN vol_rank <= 20 OR liq_rank <= 200 THEN 'b quiet, covered'
              ELSE 'd not covered' END AS class
  FROM c
)
SELECT class,
       count(*) AS markets,
       count(*) FILTER (WHERE vol_rank <= 20) AS on_ws_now,
       count(*) FILTER (WHERE liq_rank <= 200) AS on_backfill_now,
       count(*) FILTER (WHERE vol_rank BETWEEN 21 AND 100) AS ws_cap_100_would_add,
       count(*) FILTER (WHERE vol_rank BETWEEN 101 AND 200) AS ws_cap_200_would_add,
       count(*) FILTER (WHERE vol_rank > 200) AS vol_rank_over_200,
       count(*) FILTER (WHERE vol_rank IS NULL AND liq_ok AND NOT gamma_fresh) AS gamma_stale,
       count(*) FILTER (WHERE NOT liq_ok OR liq_ok IS NULL) AS under_10k_liquidity,
       count(*) FILTER (WHERE liq_rank > 200) AS crowded_out_of_backfill
FROM k
GROUP BY class
ORDER BY class;
