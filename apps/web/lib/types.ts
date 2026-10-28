// lib/types.ts. Timestamps are epoch-ms numbers: serializable, timezone-proof,
// no Date objects crossing the server→client boundary.
export type Market = {
  id: string
  title: string
  category: string | null
  status: string
  liquidity_usd: number | null
}
export type MarketRow = Market & { tremor_score: number | null; scored_at_ms: number }
export type Bar = {
  bucket_ms: number
  open: number
  high: number
  low: number
  close: number
  volume_usd: number
}
export type Trade = { ts_ms: number; price: number; size_usd: number; side: string }
export type Signal = { tremor_score: number | null; scored_at_ms: number }
export type Features = { bucket_ms: number; price_zscore: number | null; ob_imbalance: number | null }