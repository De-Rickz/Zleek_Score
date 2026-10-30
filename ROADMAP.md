# Zleek Score roadmap

## What this is
Signals tool for Polymarket: ingests trades and order books, scores markets 0-100 ("tremor score") for unusual activity, and shows them on a dashboard. Portfolio project; v1 is two pages (markets dashboard, market detail).

## v1 scope (locked)
In: dashboard sorted by tremor score, market detail page (1m price/volume chart, recent trades, features), polling refresh.
Out until after v1 ships: auth, alerts/alert_rules, journal, billing, realtime/websockets in the frontend, settings, dark-mode toggle.

## Phases (in order)
1. Data correctness: token labels, YES-equivalent prices, repair + rebuild history; scoring calibration. (done 2026-10-30: deployed; acceptance A-E pass, see apps/worker/scripts/acceptance_price_orientation.sql)
2. Data loss: backfill from newest stored trade on startup; dead-websocket detection; Redis flush only deletes after commit.
3. Hosting: move worker, Postgres, Redis and web app together to a VPS (systemd, Caddy). Fresh database.
4. Coverage (Option B): raise the websocket market cap; add trades(ts) index first. Books for every scored market, then imbalance can rejoin the tremor score. Leftover from phase 1: measure imbalance near the price (calibration item 4) once a few days of fresh order books have built up.
5. Packaging: README with architecture diagram, demo video.
6. Ship: pick a date once the VPS has run stable through phase 4.

## Rules for choosing work
- Finish the current phase before starting the next.
- Items in infra/TODO.md are only worked on when they belong to the current phase, or when asked.
- Anything not listed here is out of scope: ask first.