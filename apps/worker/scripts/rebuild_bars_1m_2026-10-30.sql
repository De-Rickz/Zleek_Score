-- Rebuild step 1 of 3: bars_1m from trades, for all history.
-- Run AFTER repair_trade_orientation_2026-10-30.sql has run twice and
-- acceptance queries C and D show 0 wrong (deploy steps 1-6).
--
-- Same aggregation as bars_jobs.py (open = first trade, close = last trade,
-- per market-minute), with t.id as a tie-break so the result is deterministic
-- when two trades share a timestamp. Recomputes from trades (the source), so
-- it is idempotent, and only rewrites bars whose values actually change.
-- The live bars job rebuilds the last 65 minutes concurrently from the same
-- trades, so both agree.
--
-- Validated 2026-10-30 (read-only) on the 3 markets whose trades were all on
-- the outcome-0 token (untouched by the bug): 159 of 164 stored bars match
-- exactly; the rest are bars that missed late-arriving trades, and 6 minutes
-- with trades had no bar. The rebuild includes those.
--
-- Run with a role that can INSERT/UPDATE bars_1m (zleek_web cannot).

-- Before: bars that differ from a rebuild, and minutes with trades but no bar
WITH r AS (
  SELECT market_id, date_trunc('minute', ts) AS bucket,
         (array_agg(price ORDER BY ts ASC,  id ASC))[1]  AS open,
         max(price) AS high, min(price) AS low,
         (array_agg(price ORDER BY ts DESC, id DESC))[1] AS close,
         sum(size_usd) AS volume_usd, count(*) AS trades_cnt
  FROM trades GROUP BY 1, 2
)
SELECT count(*)                                                         AS rebuilt_bars,
       count(*) FILTER (WHERE b.bucket IS NULL)                         AS missing_bars,
       count(*) FILTER (WHERE b.bucket IS NOT NULL AND (b.open, b.high, b.low, b.close, b.volume_usd, b.trades_cnt)
                        IS DISTINCT FROM (r.open, r.high, r.low, r.close, r.volume_usd, r.trades_cnt)) AS bars_changing
FROM r LEFT JOIN bars_1m b USING (market_id, bucket);

BEGIN;

INSERT INTO bars_1m (market_id, bucket, open, high, low, close, volume_usd, trades_cnt)
SELECT market_id, date_trunc('minute', ts),
       (array_agg(price ORDER BY ts ASC,  id ASC))[1],
       max(price), min(price),
       (array_agg(price ORDER BY ts DESC, id DESC))[1],
       sum(size_usd), count(*)
FROM trades
GROUP BY market_id, date_trunc('minute', ts)
ON CONFLICT (market_id, bucket) DO UPDATE
SET open = excluded.open, high = excluded.high, low = excluded.low, close = excluded.close,
    volume_usd = excluded.volume_usd, trades_cnt = excluded.trades_cnt
WHERE (bars_1m.open, bars_1m.high, bars_1m.low, bars_1m.close, bars_1m.volume_usd, bars_1m.trades_cnt)
      IS DISTINCT FROM (excluded.open, excluded.high, excluded.low, excluded.close, excluded.volume_usd, excluded.trades_cnt);

COMMIT;

-- After: expect missing_bars = 0 and bars_changing = 0 (a handful may reappear
-- if trades arrive while this runs; the live bars job picks those up).
WITH r AS (
  SELECT market_id, date_trunc('minute', ts) AS bucket,
         (array_agg(price ORDER BY ts ASC,  id ASC))[1]  AS open,
         max(price) AS high, min(price) AS low,
         (array_agg(price ORDER BY ts DESC, id DESC))[1] AS close,
         sum(size_usd) AS volume_usd, count(*) AS trades_cnt
  FROM trades GROUP BY 1, 2
)
SELECT count(*)                                                         AS rebuilt_bars,
       count(*) FILTER (WHERE b.bucket IS NULL)                         AS missing_bars,
       count(*) FILTER (WHERE b.bucket IS NOT NULL AND (b.open, b.high, b.low, b.close, b.volume_usd, b.trades_cnt)
                        IS DISTINCT FROM (r.open, r.high, r.low, r.close, r.volume_usd, r.trades_cnt)) AS bars_changing
FROM r LEFT JOIN bars_1m b USING (market_id, bucket);
