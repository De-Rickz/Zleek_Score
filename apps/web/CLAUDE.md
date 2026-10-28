# Zleek Score — apps/web

## Scope (locked)

(a) **Markets dashboard**: markets sorted by tremor score desc, showing title, category, liquidity, tremor score, last update.

(b) **Market detail page**: 1-minute price+volume chart from `bars_1m`, recent trades, current tremor score, latest features (price z-score, orderbook imbalance). Polling refresh every 10s.

**NOT in v1**: auth, Supabase, alerts, journal, billing, websockets/realtime, dark mode, settings. Don't add anything outside the scope. Don't use react-query.

## Architecture

App Router Server Components query Postgres directly with the `postgres` npm package. No API layer. The web app connects as a read-only role (`zleek_web`) via `DATABASE_URL` in `apps/web/.env.local`.

## Files

`lib/db.ts`, `lib/types.ts`, `lib/queries.ts`, `lib/format.ts`, `app/page.tsx`, `app/error.tsx`, `app/components/AutoRefresh.tsx`, `app/components/PriceChart.tsx`, `app/markets/[id]/page.tsx`, `app/markets/[id]/not-found.tsx`, `.env.example`.

## Rules

- `lib/db.ts` imports `server-only` and keeps the client on `globalThis` so dev hot reload doesn't leak connection pools; max 5 connections.
- All SQL lives in `lib/queries.ts`, as postgres.js tagged templates (never string concatenation). Explicit column lists, no `SELECT *`.
- Cast numeric columns with `::float8` in SQL (postgres.js returns numeric/bigint as strings). Return timestamps as epoch milliseconds (`extract(epoch FROM col) * 1000`), not `Date` objects. Return plain objects so they can cross into client components.
- Hand-written types in `lib/types.ts`. No ORM, no codegen.
- Both pages: `export const dynamic = 'force-dynamic'`. In Next 16, route `params` is a Promise; await it.
- `AutoRefresh` is a client component calling `router.refresh()` every 10s, only while the tab is visible.
- Dashboard shows a warning banner if the newest signal `ts` is older than 5 minutes (worker may be down). Every list has an empty state instead of crashing.
- `PriceChart`: Chart.js line for close plus bars for volume on a second axis. Chart.js has no candlestick type, so don't add a plugin. Register only the components used, set `animation: false`, destroy the chart in the effect cleanup.
- Detail page: `notFound()` for unknown ids; run independent queries with `Promise.all`.
- Bars query: newest 500 bars, reversed to chronological order (not a time window, because data is sparse).
- `error.tsx` is a client error boundary with a retry button.

## Data facts

- `signals_latest` has only ~7 rows, so the dashboard will list only 7 markets. That's a backend coverage issue, not a frontend bug.
- Market `3398287` has the most bars (338), so use `/markets/3398287` to test the chart.
- Known column names: `signals_latest(market_id text, ts, tremor_score)`; `features_5m(market_id text, bucket, price_z_score, ob_imbalance)`.
- `markets.id` is `text`, matching `signals_latest.market_id` and `features_5m.market_id` — no cast needed on joins.

## Working rules

- Don't run docker, launchctl, or anything that touches the worker or writes to the database. Read-only queries only.
- Install only `postgres` and `server-only`.
- If a future change conflicts with this spec, stop and ask instead of guessing.
