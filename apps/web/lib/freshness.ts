// lib/freshness.ts
// Dashboard rules for ended markets and data freshness. Pure functions (no
// database), so they're unit-tested in lib/freshness.test.ts.

// Same window and minimum as the worker's price z-score (signals/features.py:
// Z_WINDOW = 240 minutes, Z_MIN_BARS = 10). With fewer bars the worker stores
// no z and the score is 0 because nothing was measured, not because it's calm.
export const SCORE_WINDOW_MS = 240 * 60_000
export const MIN_BARS_TO_SCORE = 10
// A live market whose newest trade is older than this gets a "data 45m old" marker.
export const OLD_DATA_MS = 30 * 60_000
// Past its end date with no trade for this long counts as ended.
export const ENDED_QUIET_MS = 3 * 60 * 60_000

export type EndedInput = {
  status: string              // our markets.status
  gammaClosed: boolean        // Gamma's "closed"
  acceptingOrders: boolean    // Gamma's "acceptingOrders" (true when missing)
  endMs: number | null        // Gamma's endDate
  lastTradeMs: number | null
}

// Ended: closed or resolved (ours or Gamma's), not accepting orders, or past
// its end date with no trade in the last 3 hours. The end date alone isn't
// enough: markets keep trading after it until they resolve (2026-11-03: two
// "US x Iran ceasefire" markets were past their end date and still trading).
export function isEnded(m: EndedInput, now: number): boolean {
  if (m.status === 'closed' || m.status === 'resolved' || m.gammaClosed || !m.acceptingOrders) return true
  if (m.endMs != null && m.endMs < now) {
    return m.lastTradeMs == null || now - m.lastTradeMs > ENDED_QUIET_MS
  }
  return false
}

export const isLive = (barsInWindow: number) => barsInWindow >= MIN_BARS_TO_SCORE

// Compact age: "45m", "2h", "3d" (whole units, rounded down).
export function shortAge(ms: number): string {
  const m = Math.max(0, Math.floor(ms / 60_000))
  if (m < 60) return `${m}m`
  if (m < 1440) return `${Math.floor(m / 60)}h`
  return `${Math.floor(m / 1440)}d`
}

// "data 2h old" when a scored market's newest trade is over 30 minutes old.
export function oldDataMarker(lastDataMs: number | null, now: number): string | null {
  if (lastDataMs == null) return null
  const age = now - lastDataMs
  return age > OLD_DATA_MS ? `data ${shortAge(age)} old` : null
}

export function notLiveSummary(count: number): string {
  return `${count} ${count === 1 ? 'market' : 'markets'} not live (too little trading in the last 4 hours to score)`
}
