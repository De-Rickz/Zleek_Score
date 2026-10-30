-- One-time repair: markets.token_yes_id / token_no_id were stored reversed.
-- gamma_client.parse_token_ids put clobTokenIds[0] in token_no_id, but
-- clobTokenIds follow `outcomes` order, so [0] is outcome 0 ("Yes" on every
-- Yes/No market: 30,427 of 30,427 checked 2026-10-30). Fixed in
-- gamma_client.py on branch fix/outcome-orientation.
--
-- Recomputes both columns from the market's own stored Gamma payload
-- (raw->>'clobTokenIds'), never from the current column values, so it is:
--   * idempotent: a second run updates 0 rows;
--   * safe before or after the worker restart: rows the fixed worker already
--     refreshed are already correct and are skipped by the WHERE clause; if the
--     OLD worker re-reverses rows after this runs, simply run it again.
--
-- Dry run: acceptance query C in acceptance_price_orientation.sql
-- (read-only, zleek_web) shows how many rows this will change.
-- Baseline 2026-10-30: 58,449 of 58,449 markets wrong.
--
-- Deploy order (CLAUDE.md): restart the worker on the fixed code FIRST, then
-- run this, then repair_trade_orientation_2026-10-30.sql.
-- Run with a role that can UPDATE markets (zleek_web cannot).
-- If any clobTokenIds fails to parse, the cast errors and the whole
-- transaction rolls back (all 58,449 parsed cleanly on 2026-10-30).

-- Before
SELECT count(*) AS markets,
       count(*) FILTER (WHERE token_yes_id IS DISTINCT FROM (raw->>'clobTokenIds')::jsonb->>0
                           OR token_no_id  IS DISTINCT FROM (raw->>'clobTokenIds')::jsonb->>1) AS labels_wrong
FROM markets;

BEGIN;

UPDATE markets
SET token_yes_id = (raw->>'clobTokenIds')::jsonb->>0,
    token_no_id  = (raw->>'clobTokenIds')::jsonb->>1
WHERE jsonb_typeof((raw->>'clobTokenIds')::jsonb) = 'array'
  AND jsonb_array_length((raw->>'clobTokenIds')::jsonb) = 2
  AND (token_yes_id IS DISTINCT FROM (raw->>'clobTokenIds')::jsonb->>0
    OR token_no_id  IS DISTINCT FROM (raw->>'clobTokenIds')::jsonb->>1);

COMMIT;

-- After: expect labels_wrong = 0
SELECT count(*) AS markets,
       count(*) FILTER (WHERE token_yes_id IS DISTINCT FROM (raw->>'clobTokenIds')::jsonb->>0
                           OR token_no_id  IS DISTINCT FROM (raw->>'clobTokenIds')::jsonb->>1) AS labels_wrong
FROM markets;
