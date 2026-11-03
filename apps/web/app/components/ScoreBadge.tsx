// app/components/ScoreBadge.tsx
// Tremor score as a badge on a four-step single-hue (violet) ramp: the only
// strong colour in a dashboard row. The number is always shown, so colour is
// never the only signal. Each mode's ramp gets steadily stronger (light: paler
// to darker; dark: darker to brighter). Text contrast per step (WCAG AA 4.5):
//   step    light                           dark
//   0-24    violet-800 on violet-50  8.35   violet-300 on violet-950 8.21
//   25-49   violet-900 on violet-200 7.93   violet-100 on violet-900 9.28
//   50-74   violet-950 on violet-400 5.36   white on violet-700      7.29
//   75-100  white on violet-700      7.29   white on violet-600      5.88
// No brighter dark fill exists (nothing on violet-500 reaches 4.5:1), and
// violet-600 alone is too close to 700, so the dark top step adds a violet-300 ring.
const STEPS = [
  { min: 75, cls: 'bg-violet-700 text-white dark:bg-violet-600 dark:text-white dark:ring-2 dark:ring-inset dark:ring-violet-300' },
  { min: 50, cls: 'bg-violet-400 text-violet-950 dark:bg-violet-700 dark:text-white' },
  { min: 25, cls: 'bg-violet-200 text-violet-900 dark:bg-violet-900 dark:text-violet-100' },
  { min: 0, cls: 'bg-violet-50 text-violet-800 dark:bg-violet-950 dark:text-violet-300' },
]

export default function ScoreBadge({ score, large = false }: { score: number | null; large?: boolean }) {
  if (score == null) return <span className="text-muted">—</span>
  const value = Math.round(score)
  const step = STEPS.find((s) => value >= s.min) ?? STEPS[STEPS.length - 1]
  return (
    <span
      className={`inline-block min-w-10 rounded px-2 text-center font-semibold tabular-nums ${
        large ? 'text-xl leading-8' : 'leading-6'} ${step.cls}`}
      title={`Tremor score ${value} of 100`}
    >
      {value}
    </span>
  )
}
