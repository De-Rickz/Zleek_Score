// lib/queries.ts. Tagged templates are parameterized (injection-safe).
// Never string-concat SQL.
import { sql } from './db'
import type { Bar, Features, Market, MarketRow, Signal, Trade } from './types'

// postgres.js returns array/row subclasses; hand Next plain objects.
const plain = <T extends object>(rows: readonly T[]): T[] =>
  Array.from(rows, (r) => ({ ...r }))

export async function getMarket(id: string): Promise<Market | null> {
  const rows = await sql<Market[]>`
    SELECT id, title, category, status, liquidity_usd::float8 AS liquidity_usd
    FROM markets WHERE id = ${id} LIMIT 1`
  return rows[0] ? { ...rows[0] } : null
}

export async function getBars(marketId: string, limit = 500) {
  const rows = await sql<Bar[]>`
    SELECT (extract(epoch FROM bucket) * 1000)::float8 AS bucket_ms,
           open::float8 AS open, high::float8 AS high,
           low::float8 AS low, close::float8 AS close,
           volume_usd::float8 AS volume_usd
    FROM bars_1m WHERE market_id = ${marketId}
    ORDER BY bucket DESC LIMIT ${limit}`
  return plain(rows).reverse()
}
export async function getMarketsByTremor(limit = 100) {
  return plain(await sql<MarketRow[]>`
    SELECT m.id, m.title, m.category, m.status,
           m.liquidity_usd::float8 AS liquidity_usd,
           s.tremor_score::float8 AS tremor_score,
           (extract(epoch FROM s.ts) * 1000)::float8 AS scored_at_ms
    FROM signals_latest s
    JOIN markets m ON m.id = s.market_id
    ORDER BY s.tremor_score DESC
    LIMIT ${limit}`)
}

export async function getSignal(marketId: string): Promise<Signal | null> {
  const rows = await sql<Signal[]>`
    SELECT tremor_score::float8 AS tremor_score,
           (extract(epoch FROM ts) * 1000)::float8 AS scored_at_ms
    FROM signals_latest WHERE market_id = ${marketId} LIMIT 1`
  return rows[0] ? { ...rows[0] } : null
}

export async function getLatestFeatures(marketId: string): Promise<Features | null> {
  const rows = await sql<Features[]>`
    SELECT (extract(epoch FROM bucket) * 1000)::float8 AS bucket_ms,
           price_z_score::float8 AS price_zscore,
           -- The worker only stores imbalance from a fresh, two-sided book; also
           -- hide it if this row is old (market no longer scored, or pre-cutoff).
           CASE WHEN bucket > now() - interval '15 minutes'
                THEN ob_imbalance::float8 END AS ob_imbalance
    FROM features_5m WHERE market_id = ${marketId}
    ORDER BY bucket DESC LIMIT 1`
  return rows[0] ? { ...rows[0] } : null
}

// Newest N bars, reversed to chronological. Robust to sparse data, unlike a
// "last 4 hours" window that returns nothing when the worker had a gap.
export async function getRecentTrades(marketId: string, limit = 50) {
  return plain(await sql<Trade[]>`
    SELECT (extract(epoch FROM ts) * 1000)::float8 AS ts_ms,
           price::float8 AS price, size_usd::float8 AS size_usd, side
    FROM trades WHERE market_id = ${marketId}
    ORDER BY ts DESC LIMIT ${limit}`)
}