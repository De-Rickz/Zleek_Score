-- Funnel summary for one fixed UTC window, run the same way on the Mac DB and
-- the production DB (parallel run, ROADMAP phase 3). Read-only.
--
--   psql ... -v t0='2026-11-03 00:00:00+00' -v t1='2026-11-04 00:00:00+00' < funnel_compare.sql
--
-- Pick a window that ended at least 70 minutes ago on BOTH machines: the
-- periodic backfill re-fetches the last hour and the bars job rebuilds the
-- last 65 minutes, so a younger window is still filling in.
-- Counts are of rows whose own timestamp is in [t0, t1): trades.ts, bars_1m
-- bucket, features_5m bucket, signals_history ts, ob_snapshots ts.

SELECT * FROM (
SELECT 'trades'              AS stage,
       count(*)              AS rows,
       count(DISTINCT market_id) AS markets,
       'without tx_hash'     AS flag_meaning,
       count(*) FILTER (WHERE tx_hash IS NULL)              AS flag_count,
       count(*) FILTER (WHERE jsonb_typeof(raw) = 'string') AS from_websocket,
       count(*) FILTER (WHERE jsonb_typeof(raw) = 'object') AS from_backfill,
       round(sum(size_usd))  AS volume_usd
FROM trades WHERE ts >= :'t0' AND ts < :'t1'
UNION ALL
SELECT 'bars_1m', count(*), count(DISTINCT market_id), NULL, NULL, NULL, NULL, round(sum(volume_usd))
FROM bars_1m WHERE bucket >= :'t0' AND bucket < :'t1'
UNION ALL
SELECT 'features_5m', count(*), count(DISTINCT market_id), 'z NULL (quiet)',
       count(*) FILTER (WHERE price_z_score IS NULL), NULL, NULL, NULL
FROM features_5m WHERE bucket >= :'t0' AND bucket < :'t1'
UNION ALL
SELECT 'signals_history', count(*), count(DISTINCT market_id), 'score > 50',
       count(*) FILTER (WHERE tremor_score > 50), NULL, NULL, NULL
FROM signals_history WHERE ts >= :'t0' AND ts < :'t1'
UNION ALL
SELECT 'ob_snapshots', count(*), count(DISTINCT market_id), NULL, NULL, NULL, NULL, NULL
FROM ob_snapshots WHERE ts >= :'t0' AND ts < :'t1'
) f
ORDER BY array_position(ARRAY['trades','bars_1m','features_5m','signals_history','ob_snapshots'], stage);
