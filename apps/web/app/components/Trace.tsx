// app/components/Trace.tsx
// A seismograph trace for one dashboard row: the market's 1-minute closes over
// the last 4 hours, drawn as an ink line that holds at the last close between
// trades, on a hairline baseline at the window's opening price.
// Scaled by odds: the line plots log-odds, ln(p / (1 - p)), not price, so a
// move in a 1-cent long shot shows as clearly as the same relative move near
// 50 cents (the tremor score reacts to both). Fixed gain, the same for every
// row: the full height is 1.0 in log-odds (about 0.24 in price near 0.50, about
// 0.01 near 0.01), so traces compare across rows; a bigger move pegs at the
// edge, like a seismograph needle (4 of the top 100 on 2026-11-03).
// A market with no trades draws a flat line, so silence is visible.
import type { TracePoint } from '@/lib/queries'

const W = 160, H = 24
const SPAN = 1.0                  // full height, in log-odds
const P_MIN = 0.001, P_MAX = 0.999 // clamp away from 0 and 1 (log-odds is infinite there)

const logOdds = (p: number) => {
  const q = Math.min(P_MAX, Math.max(P_MIN, p))
  return Math.log(q / (1 - q))
}
// cheap and near-certain prices need three decimals to show a move
const fmt = (p: number) => (p < 0.1 || p > 0.9 ? p.toFixed(3) : p.toFixed(2))

export default function Trace({ prior, points, start, end }: {
  prior: number | null; points: TracePoint[]; start: number; end: number
}) {
  const open = prior ?? points[0]?.close ?? null
  if (open == null) {
    // no price in the last 24 hours: baseline only
    return <svg aria-label="No trades in the last 24 hours" role="img" width={W} height={H} viewBox={`0 0 ${W} ${H}`}>
      <title>No trades in the last 24 hours</title>
      <line x1="0" x2={W} y1={H / 2} y2={H / 2} className="stroke-line" strokeWidth="1" />
    </svg>
  }
  const values = [open, ...points.map((p) => p.close)].map(logOdds)
  const mid = (Math.min(...values) + Math.max(...values)) / 2
  const x = (t: number) => Math.round(((t - start) / (end - start)) * W * 10) / 10
  const y = (p: number) =>
    Math.round(Math.min(H - 1, Math.max(1, H / 2 - ((logOdds(p) - mid) / SPAN) * H)) * 10) / 10

  // stepped path: hold each close until the next one, then to now
  let d = `M${prior != null ? 0 : x(points[0].t)},${y(open)}`
  for (const p of points) d += `H${x(p.t)}V${y(p.close)}`
  d += `H${W}`
  const last = points.length ? points[points.length - 1].close : open
  const label = (points.length
    ? `Price over the last 4 hours: ${fmt(open)} to ${fmt(last)}`
    : `No trades in the last 4 hours, price ${fmt(open)}`) + ', scaled by odds'
  return (
    <svg aria-label={label} role="img" width={W} height={H} viewBox={`0 0 ${W} ${H}`}>
      <title>{label}</title>
      <line x1="0" x2={W} y1={y(open)} y2={y(open)} className="stroke-line" strokeWidth="1" />
      <path d={d} fill="none" className="stroke-foreground" strokeWidth="1.25" strokeLinejoin="miter" />
    </svg>
  )
}
