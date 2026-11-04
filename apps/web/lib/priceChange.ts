// lib/priceChange.ts
// 1-hour price change for the market detail page. Bars only exist for minutes
// with trades, so "60 minutes ago" is the last close AT OR BEFORE that moment
// (the price that was current then), and "now" is the newest close in the last
// hour. No arrow or number when either is missing: a dash with a reason.

export type PriceChangeRow = {
  cur_close: number | null   // newest close in the last 60 minutes
  ref_close: number | null   // last close at or before 60 minutes ago, within 24 h before that
  ref_ms: number | null      // that reference bar's time
}

export type PriceChange =
  | { kind: 'none'; reason: string }
  | { kind: 'change'; text: string; arrow: string; value: string; label: string; refMs: number }

const MINUS = '−'

export function describeChange(row: PriceChangeRow, hhmm: (ms: number) => string): PriceChange {
  if (row.cur_close == null) return { kind: 'none', reason: 'No trades in the last hour' }
  if (row.ref_close == null || row.ref_ms == null) {
    return { kind: 'none', reason: 'No earlier price in the 24 hours before that' }
  }
  const delta = Math.round((row.cur_close - row.ref_close) * 1000) / 1000
  const abs = Math.abs(delta).toFixed(3)
  // Arrow and sign always shown together, so colour is never needed. The arrow
  // is returned separately too: IBM Plex Mono has no triangle glyph, so the
  // page sizes it on its own.
  const arrow = delta > 0 ? '▲' : delta < 0 ? '▼' : ''
  const value = delta > 0 ? `+${abs}` : delta < 0 ? `${MINUS}${abs}` : '0.000'
  const text = arrow ? `${arrow} ${value}` : value
  const label = `Change since ${hhmm(row.ref_ms)}, the last price at or before 60 minutes ago`
  return { kind: 'change', text, arrow, value, label, refMs: row.ref_ms }
}
