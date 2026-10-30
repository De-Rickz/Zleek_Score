-- Rebuild step 2 of 3: features_5m.price_z_score from the rebuilt bars.
-- Run AFTER rebuild_bars_1m_2026-10-30.sql.
--
-- Same formula as signals/features.py: take the 20 newest bars, z = (newest
-- close - mean) / sample stddev, and 0 when the stddev is 0 or undefined.
-- For each existing row the bars are those at or before the row's bucket,
-- which reproduced 1,468 of 1,478 stored z-scores exactly on markets untouched
-- by the bug (2026-10-30; the rest differ by bar timing). Rows with fewer than
-- 20 bars by then (16 on 2026-10-30) fall back to bars before bucket + 5 min,
-- the most the live job could have seen. Rows still under 20 are left as is.
--
-- ob_imbalance is deliberately NOT touched. ob_snapshots history can't be
-- recomputed from source (no asset_id or raw column); before the CUTOFF (the
-- worker restart, see infra/TODO.md) its sign is unreliable and ~3% of values
-- have the wrong magnitude. Setting it to NULL would be worse: tremor treats
-- NULL as 0, silently taking up to 50 points off every pre-cutoff score.
--
-- Idempotent (recomputes from bars, only rewrites rows that change).
-- Run with a role that can UPDATE features_5m (zleek_web cannot).

-- Before: rows whose z-score will change
WITH z AS (
  SELECT f.market_id, f.bucket, f.price_z_score AS stored,
         coalesce(a.z, b.z) AS rebuilt
  FROM features_5m f
  CROSS JOIN LATERAL (
    SELECT CASE WHEN count(*) < 20 THEN NULL
                WHEN coalesce(stddev_samp(close), 0) = 0 THEN 0
                ELSE ((array_agg(close ORDER BY bucket DESC))[1] - avg(close)) / stddev_samp(close) END AS z
    FROM (SELECT close, bucket FROM bars_1m WHERE market_id = f.market_id AND bucket <= f.bucket
          ORDER BY bucket DESC LIMIT 20) x) a
  CROSS JOIN LATERAL (
    SELECT CASE WHEN count(*) < 20 THEN NULL
                WHEN coalesce(stddev_samp(close), 0) = 0 THEN 0
                ELSE ((array_agg(close ORDER BY bucket DESC))[1] - avg(close)) / stddev_samp(close) END AS z
    FROM (SELECT close, bucket FROM bars_1m WHERE market_id = f.market_id AND bucket < f.bucket + interval '5 minutes'
          ORDER BY bucket DESC LIMIT 20) x) b
)
SELECT count(*) AS feature_rows,
       count(*) FILTER (WHERE rebuilt IS NULL) AS under_20_bars_left_as_is,
       count(*) FILTER (WHERE rebuilt IS NOT NULL AND stored IS DISTINCT FROM rebuilt
                           AND (stored IS NULL OR abs(stored - rebuilt) > 1e-9)) AS rows_changing
FROM z;

BEGIN;

UPDATE features_5m f
SET price_z_score = z.rebuilt
FROM (
  SELECT f.market_id, f.bucket, coalesce(a.z, b.z) AS rebuilt
  FROM features_5m f
  CROSS JOIN LATERAL (
    SELECT CASE WHEN count(*) < 20 THEN NULL
                WHEN coalesce(stddev_samp(close), 0) = 0 THEN 0
                ELSE ((array_agg(close ORDER BY bucket DESC))[1] - avg(close)) / stddev_samp(close) END AS z
    FROM (SELECT close, bucket FROM bars_1m WHERE market_id = f.market_id AND bucket <= f.bucket
          ORDER BY bucket DESC LIMIT 20) x) a
  CROSS JOIN LATERAL (
    SELECT CASE WHEN count(*) < 20 THEN NULL
                WHEN coalesce(stddev_samp(close), 0) = 0 THEN 0
                ELSE ((array_agg(close ORDER BY bucket DESC))[1] - avg(close)) / stddev_samp(close) END AS z
    FROM (SELECT close, bucket FROM bars_1m WHERE market_id = f.market_id AND bucket < f.bucket + interval '5 minutes'
          ORDER BY bucket DESC LIMIT 20) x) b
) z
WHERE f.market_id = z.market_id AND f.bucket = z.bucket
  AND z.rebuilt IS NOT NULL
  AND (f.price_z_score IS NULL OR abs(f.price_z_score - z.rebuilt) > 1e-9);

COMMIT;

-- After: run the "Before" query again; expect rows_changing = 0 (the newest
-- bucket may differ briefly if the live features job writes during the run).
