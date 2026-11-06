# Zleek Score roadmap

## What this is
Signals tool for Polymarket: ingests trades and order books, scores markets 0-100 ("tremor score") for unusual activity, and shows them on a dashboard. Portfolio project; v1 is two pages (markets dashboard, market detail).

## v1 scope (locked)
In: dashboard sorted by tremor score, market detail page (1m price/volume chart, recent trades, features), polling refresh.
Out until after v1 ships: auth, alerts/alert_rules, journal, billing, realtime/websockets in the frontend, settings, dark-mode toggle.

## Phases (in order)
1. Data correctness: token labels, YES-equivalent prices, repair + rebuild history; scoring calibration. (done 2026-10-30: deployed; acceptance A-E pass, see apps/worker/scripts/acceptance_price_orientation.sql)
2. Data loss: backfill from newest stored trade on startup; dead-websocket detection; Redis flush only deletes after commit. (done 2026-10-31: deployed; acceptance A-E pass. Related, still open in infra/TODO.md: events dropped while Redis is down.)
3. Hosting: move worker, Postgres, Redis and web app together to a VPS (systemd, Caddy). Fresh database. (done 2026-11-03: cutover to the Oracle server; Mac data kept as rollback until 2026-11-17; see docs/parallel-run.md §5)
4. Coverage (Option B), in this order, measuring coverage and storage after each step (apps/worker/scripts/dashboard-classes.sql, stale-classify.sql, storage-report.sql):
   Soak first: phase 4 starts no earlier than 2026-11-06, and only if the heartbeat has stayed green and nothing unexplained has turned up. Frontend deploys are allowed during the soak; worker changes aren't.
   1. Add the trades(ts) index. (done 2026-11-06: ix_trades_ts, 7.7 MB; the bars job's window query reads 620 pages instead of 3,448 (24 ms vs 35 ms, the rest is sorting), max(ts) 0.06 ms instead of 56 ms; coverage unchanged)
   2a. Stop rewriting unchanged trades: REST_QUERY's ON CONFLICT (tx_hash) DO UPDATE only when the price changed (WHERE trades.price IS DISTINCT FROM EXCLUDED.price). The backfill re-fetches the last hour every 5 minutes, so each trade arrives ~12 times and every arrival wrote a new row version; step 2's larger backfill would make that millions of useless writes a day. Measure with apps/worker/scripts/phase4_step2a_check.sql (trades update and dead-row counters).
   2b. Move the backfill to Data API v2 (found 2026-11-06): Polymarket retired Data API v1 on 2026-11-02 (/trades answers HTTP 410), so the periodic and startup backfills have stored nothing since cutover. fetch_public_trades now uses /v2/trades: condition ids in batches of 20, newest-first cursor paging (no server-side time filter for markets: the client stops at the window start), snake_case fields, Retry-After on 429/503. Also: the heartbeat gets an absolute 'backfill' check (FAIL with no backfilled trade in 2 h), and a run in which every request fails is logged at ERROR with a failed-runs-in-a-row count. Step 2 depends on it. The trades lost since 2026-11-02 22:27 (~5,000, ~15 requests) can be recovered with a one-off script (to design).
   2. Rank the backfill by recent volume: top 200 by 24h volume among open markets Gamma refreshed in the last 2 h, instead of by liquidity with no freshness check. On 2026-11-03 production, all 44 uncovered markets in the top 100 were crowded out of the liquidity-ranked backfill, and none of the 16 markets that traded in the last hour came from it. Step 2 multiplies stored trades (markets ranked 21-220 by volume trade ~3x the top 20): ~74 -> ~290 MB/day of trades, so the storage step follows next, before step 4. Also fixes the selection's out-of-date comment (websocket trades have had a tx_hash since 2026-10-28).
      Then check whether the dashboard's live count rises above 20 (apps/worker/scripts/dashboard-classes.sql: on 2026-11-03 production it was 20 of 248 scored markets, exactly the websocket cap). If it doesn't, the cap raise in step 5 is the main lever for coverage.
   3. Storage, before more data arrives (moved ahead of the catch-up and the cap raise, 2026-11-06) (details and estimates: docs/storage-plan.md). TimescaleDB compression for chunks older than the backfill's reach (3 days; 1-day chunks); convert ob_snapshots and signals_history to hypertables; design how trades gets compressed (its tx_hash dedupe rules out a plain conversion). Retention: trades and bars_1m kept, compressed; features_5m 30 days; signals_history 90 days; ob_snapshots 30 days. Without this, cap 200 fills the disk to the 75% warning in ~38 days; with it, ~8.5 months (measured 2026-11-06, docs/storage-plan.md).
   4. Catch up on subscribe: when the worker subscribes to a new market, backfill its last ~15 minutes from the Data API. Trades from before a market enters the websocket set are otherwise never fetched (the backfill skips websocket markets; the gap backfill only runs at startup), so the first minutes of a surge, the most interesting part for a tremor score, are lost. At the 2026-11-03 cutover check, production missed 599 such onset trades (its websocket picked markets up ~8 minutes after the Mac's) and the Mac missed 2,836 the same way.
   5. Raise the websocket cap from 20 to 100, then 200 if load allows (it would cover 14, then 35, of those 44).
   6. Books for every scored market, then imbalance can rejoin the tremor score. Store only the top 10 levels a side, and snapshot when the top of book changes or at a fixed interval (today: every book event, at most every 5 s, all levels).

   Leftovers: measure imbalance near the price (calibration item 4) once a few days of fresh order books have built up; decide on a scoring floor for frozen and penny markets (infra/TODO.md); the worker re-stamps scores every 5 minutes regardless of data age, so a stale score can look fresh for up to 4 hours; consider storing the newest bar time alongside each score.
5. Packaging: README with architecture diagram, demo video.
6. Ship: pick a date once the VPS has run stable through phase 4.

Frontend (v1 scope pages) is a parallel track: may proceed alongside any phase, stays inside v1 scope, and features depending on phase 4 data (imbalance in the score) wait for phase 4.

## Rules for choosing work
- Finish the current phase before starting the next.
- Items in infra/TODO.md are only worked on when they belong to the current phase, or when asked.
- Anything not listed here is out of scope: ask first.