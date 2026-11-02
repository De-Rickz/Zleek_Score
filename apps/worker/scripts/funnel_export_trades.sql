-- Every trade in [t0, t1) as CSV, for compare_funnels.py. Read-only.
--   psql ... -v t0='...' -v t1='...' < funnel_export_trades.sql > trades.csv
-- tx_hash is unique in trades (a fill seen live and by the backfill is one row),
-- so it identifies the same trade in both databases.
COPY (
  SELECT tx_hash, market_id, extract(epoch FROM ts)::bigint AS ts_unix,
         CASE jsonb_typeof(raw) WHEN 'string' THEN 'websocket' ELSE 'backfill' END AS source,
         round(size_usd, 2) AS size_usd
  FROM trades
  WHERE ts >= :'t0' AND ts < :'t1'
  ORDER BY tx_hash
) TO STDOUT WITH (FORMAT csv, HEADER true);
