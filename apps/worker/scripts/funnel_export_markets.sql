-- Per-market funnel in [t0, t1) as CSV, for compare_funnels.py. Read-only.
--   psql ... -v t0='...' -v t1='...' < funnel_export_markets.sql > markets.csv
COPY (
  WITH t AS (SELECT market_id, count(*) n FROM trades WHERE ts >= :'t0' AND ts < :'t1' GROUP BY 1),
       b AS (SELECT market_id, count(*) n FROM bars_1m WHERE bucket >= :'t0' AND bucket < :'t1' GROUP BY 1),
       f AS (SELECT market_id, count(*) n FROM features_5m WHERE bucket >= :'t0' AND bucket < :'t1' GROUP BY 1),
       s AS (SELECT market_id, count(*) n, max(tremor_score) max_score
             FROM signals_history WHERE ts >= :'t0' AND ts < :'t1' GROUP BY 1),
       o AS (SELECT market_id, count(*) n FROM ob_snapshots WHERE ts >= :'t0' AND ts < :'t1' GROUP BY 1),
       ids AS (SELECT market_id FROM t UNION SELECT market_id FROM b UNION SELECT market_id FROM f
               UNION SELECT market_id FROM s UNION SELECT market_id FROM o)
  SELECT ids.market_id, coalesce(t.n, 0) AS trades, coalesce(b.n, 0) AS bars, coalesce(f.n, 0) AS features,
         coalesce(s.n, 0) AS scores, s.max_score, coalesce(o.n, 0) AS snapshots
  FROM ids LEFT JOIN t USING (market_id) LEFT JOIN b USING (market_id) LEFT JOIN f USING (market_id)
           LEFT JOIN s USING (market_id) LEFT JOIN o USING (market_id)
  ORDER BY 1
) TO STDOUT WITH (FORMAT csv, HEADER true);
