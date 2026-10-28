// lib/format.ts
// Nullable numeric columns render as '—' rather than crashing or showing $0.
export const usd = (n: number | null) =>
  n == null ? '—' : new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(n)

export const fixed = (n: number | null, digits: number) =>
  n == null ? '—' : n.toFixed(digits)

export function timeAgo(ms: number, now = Date.now()) {
  const s = Math.max(0, Math.round((now - ms) / 1000))
  if (s < 60) return `${s}s ago`
  if (s < 3600) return `${Math.floor(s / 60)}m ago`
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`
  return `${Math.floor(s / 86400)}d ago`
}