-- dashboard-classes.sql: how many scored markets land in each state of the
-- web/data-freshness dashboard. Read-only. Mirrors apps/web/lib/freshness.ts:
--   hidden (ended): status closed/resolved, Gamma closed, not accepting
--                   orders, or past endDate with no trade in the last 3 hours
--   live:           at least 10 one-minute bars in the last 240 minutes
--                   (listed by score, up to 100)
--   not live:       fewer than 10 (collapsed into one line; up to 100 listed
--                   by liquidity when expanded)
--   live, old data: live, but the newest trade (else newest bar) is over
--                   30 minutes old ("data 2h old" marker)
--
-- Run on the server, from the repo (after git pull; file piped in on stdin):
--   cd ~/zleek && docker compose -f docker-compose.prod.yml exec -T \
--     -e PGOPTIONS='-c default_transaction_read_only=on' \
--     db psql -U zleek_web -d postgres -X < apps/worker/scripts/dashboard-classes.sql

WITH s AS (
  SELECT s.market_id, s.tremor_score, m.status,
         coalesce(m.raw->>'closed', 'false') = 'true' AS gamma_closed,
         coalesce(m.raw->>'acceptingOrders', 'true') <> 'false' AS accepting_orders,
         CASE WHEN m.raw->>'endDate' ~ '^[0-9]{4}-' THEN (m.raw->>'endDate')::timestamptz END AS end_date,
         lt.ts AS last_trade,
         lb.bucket AS last_bar,
         (SELECT count(*) FROM bars_1m b
          WHERE b.market_id = s.market_id AND b.bucket > now() - interval '240 minutes') AS bars_in_window
  FROM signals_latest s
  JOIN markets m ON m.id = s.market_id
  LEFT JOIN LATERAL (SELECT max(ts) AS ts FROM trades t WHERE t.market_id = s.market_id) lt ON true
  LEFT JOIN LATERAL (SELECT max(bucket) AS bucket FROM bars_1m b WHERE b.market_id = s.market_id) lb ON true
),
k AS (
  SELECT *,
         status IN ('closed', 'resolved') OR gamma_closed OR NOT accepting_orders
           OR coalesce(end_date < now() AND (last_trade IS NULL OR last_trade < now() - interval '3 hours'), false) AS ended,
         bars_in_window >= 10 AS live,
         coalesce(last_trade, last_bar) < now() - interval '30 minutes' AS old_data
  FROM s
)
-- (coalesce: a market with no endDate is not ended by date; without it the
-- NULL comparison would drop it from every count below)
SELECT count(*) AS scored_markets,
       count(*) FILTER (WHERE ended) AS hidden_ended,
       count(*) FILTER (WHERE ended AND (status IN ('closed', 'resolved') OR gamma_closed OR NOT accepting_orders)) AS ended_by_status,
       count(*) FILTER (WHERE ended AND NOT (status IN ('closed', 'resolved') OR gamma_closed OR NOT accepting_orders)) AS ended_by_date_and_quiet,
       count(*) FILTER (WHERE NOT ended AND live) AS live,
       count(*) FILTER (WHERE NOT ended AND live AND old_data) AS live_with_old_data_marker,
       count(*) FILTER (WHERE NOT ended AND live AND tremor_score >= 50) AS live_scoring_50_plus,
       count(*) FILTER (WHERE NOT ended AND NOT live) AS not_live,
       count(*) FILTER (WHERE NOT ended AND NOT live AND tremor_score > 0) AS not_live_with_nonzero_score
FROM k;
