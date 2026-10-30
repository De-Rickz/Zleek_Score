-- Rebuild history for the scoring calibration (ROADMAP phase 1, 2026-10-30).
-- Run AFTER the worker restart that deploys the calibration code
-- (signals/features.py, signals/tremor.py). Uses the same rules as that code:
--
--   features_5m.price_z_score: z over up to the 20 newest bars in the 240
--     minutes ending at the row's bucket; NULL if fewer than 10 such bars.
--     Bars "at or before the bucket" (no look-ahead), as in the earlier
--     rebuild, which reproduced 99.3% of stored z-scores on untouched markets.
--   features_5m.ob_imbalance: from the newest snapshot at or before the bucket,
--     only if it is under 15 minutes old and has both sides; else NULL.
--     Rows before the order-book cutoff (2026-10-30 17:24:17 UTC) are set to
--     NULL: those books are in mixed orientation and can't be repaired, and
--     imbalance is no longer part of the score (display only, newest row only).
--   tremor_score (signals_history, signals_latest): min(|z| / 3, 1) * 100,
--     from the newest features row at or before the score's timestamp;
--     NULL z = 0. Timestamps are kept.
--
-- Constants must match signals/features.py (Z_WINDOW, Z_MAX_BARS, Z_MIN_BARS,
-- BOOK_MAX_AGE). Markets that are no longer eligible are removed from
-- signals_latest by the live tremor job on its first run after the restart,
-- so this script doesn't repeat that rule. History rows of such markets are
-- kept and rebuilt like the rest.
--
-- Idempotent: every value is recomputed from bars / ob_snapshots / features,
-- and only rows that change are written. The live jobs rewrite the current
-- 5-minute bucket concurrently; that row may differ slightly afterwards
-- (the live job sees bars up to its run time) and is fixed on its next run.
-- Run with a role that can UPDATE features_5m, signals_history and signals_latest.

-- Before: rows that will change
WITH f AS (
  SELECT f.market_id, f.bucket, f.price_z_score AS old_z, f.ob_imbalance AS old_imb,
         (SELECT CASE WHEN count(*) < 10 THEN NULL
                      WHEN coalesce(stddev_samp(close), 0) = 0 THEN 0
                      ELSE ((array_agg(close ORDER BY bucket DESC))[1] - avg(close)) / stddev_samp(close) END
          FROM (SELECT close, bucket FROM bars_1m b
                WHERE b.market_id = f.market_id AND b.bucket <= f.bucket AND b.bucket > f.bucket - interval '240 minutes'
                ORDER BY b.bucket DESC LIMIT 20) x) AS new_z,
         CASE WHEN f.bucket < '2026-10-30 17:24:17+00' THEN NULL ELSE
           (SELECT CASE WHEN o.bid_depth_usd + o.ask_depth_usd > 0
                        THEN (o.bid_depth_usd - o.ask_depth_usd) / (o.bid_depth_usd + o.ask_depth_usd) END
            FROM (SELECT * FROM ob_snapshots o WHERE o.market_id = f.market_id AND o.ts <= f.bucket
                  ORDER BY o.ts DESC LIMIT 1) o
            WHERE o.ts > f.bucket - interval '15 minutes'
              AND jsonb_array_length(o.bids) > 0 AND jsonb_array_length(o.asks) > 0) END AS new_imb
  FROM features_5m f
)
SELECT count(*) AS feature_rows,
       count(*) FILTER (WHERE old_z IS DISTINCT FROM new_z AND (old_z IS NULL OR new_z IS NULL OR abs(old_z - new_z) > 1e-9)) AS z_changing,
       count(*) FILTER (WHERE new_z IS NULL) AS z_null_after,
       count(*) FILTER (WHERE old_imb IS DISTINCT FROM new_imb AND (old_imb IS NULL OR new_imb IS NULL OR abs(old_imb - new_imb) > 1e-9)) AS imbalance_changing,
       count(*) FILTER (WHERE new_imb IS NOT NULL) AS imbalance_not_null_after
FROM f;

BEGIN;

UPDATE features_5m t
SET price_z_score = f.new_z,
    ob_imbalance  = f.new_imb
FROM (
  SELECT f.market_id, f.bucket,
         (SELECT CASE WHEN count(*) < 10 THEN NULL
                      WHEN coalesce(stddev_samp(close), 0) = 0 THEN 0
                      ELSE ((array_agg(close ORDER BY bucket DESC))[1] - avg(close)) / stddev_samp(close) END
          FROM (SELECT close, bucket FROM bars_1m b
                WHERE b.market_id = f.market_id AND b.bucket <= f.bucket AND b.bucket > f.bucket - interval '240 minutes'
                ORDER BY b.bucket DESC LIMIT 20) x) AS new_z,
         CASE WHEN f.bucket < '2026-10-30 17:24:17+00' THEN NULL ELSE
           (SELECT CASE WHEN o.bid_depth_usd + o.ask_depth_usd > 0
                        THEN (o.bid_depth_usd - o.ask_depth_usd) / (o.bid_depth_usd + o.ask_depth_usd) END
            FROM (SELECT * FROM ob_snapshots o WHERE o.market_id = f.market_id AND o.ts <= f.bucket
                  ORDER BY o.ts DESC LIMIT 1) o
            WHERE o.ts > f.bucket - interval '15 minutes'
              AND jsonb_array_length(o.bids) > 0 AND jsonb_array_length(o.asks) > 0) END AS new_imb
  FROM features_5m f
) f
WHERE t.market_id = f.market_id AND t.bucket = f.bucket
  AND ((t.price_z_score IS DISTINCT FROM f.new_z
        AND (t.price_z_score IS NULL OR f.new_z IS NULL OR abs(t.price_z_score - f.new_z) > 1e-9))
    OR (t.ob_imbalance IS DISTINCT FROM f.new_imb
        AND (t.ob_imbalance IS NULL OR f.new_imb IS NULL OR abs(t.ob_imbalance - f.new_imb) > 1e-9)));

UPDATE signals_history h
SET tremor_score = s.new_score
FROM (
  SELECT h.market_id, h.ts,
         round(least(abs(coalesce(f.price_z_score, 0)) / 3.0, 1) * 100, 2) AS new_score
  FROM signals_history h
  CROSS JOIN LATERAL (SELECT price_z_score FROM features_5m f
                      WHERE f.market_id = h.market_id AND f.bucket <= h.ts
                      ORDER BY f.bucket DESC LIMIT 1) f
) s
WHERE h.market_id = s.market_id AND h.ts = s.ts
  AND abs(h.tremor_score - s.new_score) > 1e-9;

UPDATE signals_latest l
SET tremor_score = s.new_score
FROM (
  SELECT l.market_id,
         round(least(abs(coalesce(f.price_z_score, 0)) / 3.0, 1) * 100, 2) AS new_score
  FROM signals_latest l
  CROSS JOIN LATERAL (SELECT price_z_score FROM features_5m f
                      WHERE f.market_id = l.market_id AND f.bucket <= l.ts
                      ORDER BY f.bucket DESC LIMIT 1) f
) s
WHERE l.market_id = s.market_id
  AND abs(l.tremor_score - s.new_score) > 1e-9;

COMMIT;

-- After: run the "Before" query again (expect z_changing and
-- imbalance_changing ~0, apart from the current bucket), then acceptance
-- query B in acceptance_price_orientation.sql (expect score_mismatch = 0).
