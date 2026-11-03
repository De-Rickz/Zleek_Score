# Zleek Score roadmap

## What this is
Signals tool for Polymarket: ingests trades and order books, scores markets 0-100 ("tremor score") for unusual activity, and shows them on a dashboard. Portfolio project; v1 is two pages (markets dashboard, market detail).

## v1 scope (locked)
In: dashboard sorted by tremor score, market detail page (1m price/volume chart, recent trades, features), polling refresh.
Out until after v1 ships: auth, alerts/alert_rules, journal, billing, realtime/websockets in the frontend, settings, dark-mode toggle.

## Phases (in order)
1. Data correctness: token labels, YES-equivalent prices, repair + rebuild history; scoring calibration. (done 2026-10-30: deployed; acceptance A-E pass, see apps/worker/scripts/acceptance_price_orientation.sql)
2. Data loss: backfill from newest stored trade on startup; dead-websocket detection; Redis flush only deletes after commit. (done 2026-10-31: deployed; acceptance A-E pass. Related, still open in infra/TODO.md: events dropped while Redis is down.)
3. Hosting: move worker, Postgres, Redis and web app together to a VPS (systemd, Caddy). Fresh database.
4. Coverage (Option B), in this order, measuring after each step:
   1. Add the trades(ts) index.
   2. Rank the backfill by recent volume: top 200 by 24h volume among open markets Gamma refreshed in the last 2 h, instead of by liquidity with no freshness check. On 2026-11-03 production, all 44 uncovered markets in the top 100 were crowded out of the liquidity-ranked backfill, and none of the 16 markets that traded in the last hour came from it.
      Then check whether the dashboard's live count rises above 20 (~/Desktop/dashboard-classes.sql: on 2026-11-03 production it was 20 of 248 scored markets, exactly the websocket cap). If it doesn't, the cap raise in step 3 is the main lever for coverage.
   3. Raise the websocket cap from 20 to 100, then 200 if load allows (it would cover 14, then 35, of those 44).
   4. Books for every scored market, then imbalance can rejoin the tremor score.

   Leftovers: measure imbalance near the price (calibration item 4) once a few days of fresh order books have built up; decide on a scoring floor for frozen and penny markets (infra/TODO.md); the worker re-stamps scores every 5 minutes regardless of data age, so a stale score can look fresh for up to 4 hours; consider storing the newest bar time alongside each score.
5. Packaging: README with architecture diagram, demo video.
6. Ship: pick a date once the VPS has run stable through phase 4.

Frontend (v1 scope pages) is a parallel track: may proceed alongside any phase, stays inside v1 scope, and features depending on phase 4 data (imbalance in the score) wait for phase 4.

## Rules for choosing work
- Finish the current phase before starting the next.
- Items in infra/TODO.md are only worked on when they belong to the current phase, or when asked.
- Anything not listed here is out of scope: ask first.