-- Rebuild step 3 of 3: tremor scores (signals_history and signals_latest)
-- from the rebuilt features. Run AFTER rebuild_features_5m_2026-10-30.sql.
--
-- Decision: RECOMPUTE signals_history rather than mark it with a cutoff.
-- Scores are derived data, and the recompute is exact: on markets untouched by
-- the bug it reproduced 1,475 of 1,475 stored scores (max difference 1.6e-15,
-- 2026-10-30). A cutoff would leave ~140k corrupted scores in the table for
-- any future history view to show. Caveat: pre-cutoff scores keep the old
-- ob_imbalance (see rebuild_features_5m), so ~3% of them still carry a wrong
-- imbalance magnitude; the z-score half is corrected everywhere.
--
-- Same formula and lookup as signals/tremor.py: the newest features row at or
-- before the score's timestamp;
--   score = clamp(round(min(|z| * 50/3, 50) + min(|imbalance| * 50, 50), 2), 0, 100)
-- Timestamps are kept; only tremor_score changes. Idempotent (recomputes from
-- features, only rewrites rows that change). signals_latest is also rewritten
-- by the live tremor job every 5 minutes; this just makes it correct at once.
--
-- Run with a role that can UPDATE signals_history and signals_latest.

-- Before: scores that will change
WITH h AS (
  SELECT h.tremor_score AS stored,
         greatest(0, least(100, round(least(abs(coalesce(f.price_z_score, 0)) * 50 / 3.0, 50)
                                    + least(abs(coalesce(f.ob_imbalance, 0)) * 50, 50), 2))) AS rebuilt
  FROM signals_history h
  CROSS JOIN LATERAL (SELECT price_z_score, ob_imbalance FROM features_5m f
                      WHERE f.market_id = h.market_id AND f.bucket <= h.ts
                      ORDER BY f.bucket DESC LIMIT 1) f
)
SELECT count(*) AS history_rows_with_features,
       count(*) FILTER (WHERE abs(stored - rebuilt) > 1e-9) AS history_changing,
       (SELECT count(*) FROM signals_history) AS history_rows_total
FROM h;

BEGIN;

UPDATE signals_history h
SET tremor_score = s.rebuilt
FROM (
  SELECT h.market_id, h.ts,
         greatest(0, least(100, round(least(abs(coalesce(f.price_z_score, 0)) * 50 / 3.0, 50)
                                    + least(abs(coalesce(f.ob_imbalance, 0)) * 50, 50), 2))) AS rebuilt
  FROM signals_history h
  CROSS JOIN LATERAL (SELECT price_z_score, ob_imbalance FROM features_5m f
                      WHERE f.market_id = h.market_id AND f.bucket <= h.ts
                      ORDER BY f.bucket DESC LIMIT 1) f
) s
WHERE h.market_id = s.market_id AND h.ts = s.ts
  AND abs(h.tremor_score - s.rebuilt) > 1e-9;

UPDATE signals_latest l
SET tremor_score = s.rebuilt
FROM (
  SELECT l.market_id,
         greatest(0, least(100, round(least(abs(coalesce(f.price_z_score, 0)) * 50 / 3.0, 50)
                                    + least(abs(coalesce(f.ob_imbalance, 0)) * 50, 50), 2))) AS rebuilt
  FROM signals_latest l
  CROSS JOIN LATERAL (SELECT price_z_score, ob_imbalance FROM features_5m f
                      WHERE f.market_id = l.market_id AND f.bucket <= l.ts
                      ORDER BY f.bucket DESC LIMIT 1) f
) s
WHERE l.market_id = s.market_id
  AND abs(l.tremor_score - s.rebuilt) > 1e-9;

COMMIT;

-- After: run the "Before" query again; expect history_changing = 0. Then run
-- acceptance queries A and B in acceptance_price_orientation.sql.
