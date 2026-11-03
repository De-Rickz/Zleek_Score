// app/components/ScoreScale.tsx
// The tremor score as an instrument reading: a ruled 0-100 scale (minor ticks
// every 10, major ticks at the badge steps 0/25/50/75/100) with an orange pen
// mark at the value. Ticks and labels in --muted (4.73 light / 7.63 dark), pen
// mark in --accent (5.23 / 6.85): both clear the 3:1 needed for graphics.
// Decorative for screen readers: the number next to it carries the value.
const MAJOR = [0, 25, 50, 75, 100]
const MINOR = [10, 20, 30, 40, 60, 70, 80, 90]

export default function ScoreScale({ score }: { score: number | null }) {
  const v = score == null ? null : Math.min(100, Math.max(0, score))
  return (
    <svg aria-hidden className="h-10 w-full overflow-visible" role="presentation">
      <line x1="0" x2="100%" y1="20" y2="20" className="stroke-muted" strokeWidth="1" />
      {MINOR.map((t) => (
        <line key={t} x1={`${t}%`} x2={`${t}%`} y1="16" y2="20" className="stroke-muted" strokeWidth="1" />
      ))}
      {MAJOR.map((t) => (
        <g key={t}>
          <line x1={`${t}%`} x2={`${t}%`} y1="12" y2="20" className="stroke-muted" strokeWidth="1" />
          <text x={`${t}%`} y="36" textAnchor={t === 0 ? 'start' : t === 100 ? 'end' : 'middle'}
                className="fill-muted font-mono text-xs">{t}</text>
        </g>
      ))}
      {v != null && (
        <g>
          <line x1={`${v}%`} x2={`${v}%`} y1="2" y2="20" className="stroke-accent" strokeWidth="2" />
          <circle cx={`${v}%`} cy="20" r="3" className="fill-accent" />
        </g>
      )}
    </svg>
  )
}
