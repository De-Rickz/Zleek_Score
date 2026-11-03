// app/components/Trace.tsx
// A seismograph trace for one dashboard row: the market's 1-minute closes over
// the last 4 hours, drawn as an ink line that holds at the last close between
// trades, on a hairline baseline at the window's opening price. Fixed gain, the
// same for every row: the full height is 10 cents (0.10), so traces compare
// across rows; a bigger move pegs at the edge, like a seismograph needle.
// A market with no trades draws a flat line, so silence is visible.
import type { TracePoint } from '@/lib/queries'

const W = 160, H = 24
const SPAN = 0.10

export default function Trace({ prior, points, start, end }: {
  prior: number | null; points: TracePoint[]; start: number; end: number
}) {
  const open = prior ?? points[0]?.close ?? null
  if (open == null) {
    // no price in the last 24 hours: baseline only
    return <svg aria-label="No trades in the last 24 hours" role="img" width={W} height={H} viewBox={`0 0 ${W} ${H}`}>
      <line x1="0" x2={W} y1={H / 2} y2={H / 2} className="stroke-line" strokeWidth="1" />
    </svg>
  }
  const values = [open, ...points.map((p) => p.close)]
  const mid = (Math.min(...values) + Math.max(...values)) / 2
  const x = (t: number) => Math.round(((t - start) / (end - start)) * W * 10) / 10
  const y = (v: number) => Math.round(Math.min(H - 1, Math.max(1, H / 2 - ((v - mid) / SPAN) * H)) * 10) / 10

  // stepped path: hold each close until the next one, then to now
  let d = `M${prior != null ? 0 : x(points[0].t)},${y(open)}`
  for (const p of points) d += `H${x(p.t)}V${y(p.close)}`
  d += `H${W}`
  const last = points.length ? points[points.length - 1].close : open
  const label = points.length
    ? `Price over the last 4 hours: ${open.toFixed(2)} to ${last.toFixed(2)}`
    : `No trades in the last 4 hours, price ${open.toFixed(2)}`
  return (
    <svg aria-label={label} role="img" width={W} height={H} viewBox={`0 0 ${W} ${H}`}>
      <line x1="0" x2={W} y1={y(open)} y2={y(open)} className="stroke-line" strokeWidth="1" />
      <path d={d} fill="none" className="stroke-foreground" strokeWidth="1.25" strokeLinejoin="miter" />
    </svg>
  )
}
