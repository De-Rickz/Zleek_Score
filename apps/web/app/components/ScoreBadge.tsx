// app/components/ScoreBadge.tsx
// Tremor score as a badge on a four-step single-hue (orange, the one accent
// colour) ramp: the only strong colour in a dashboard row. The number is always
// shown, so colour is never the only signal. Each mode's ramp gets steadily
// stronger (fill lightness, OKLCH L: light 98 / 90 / 75 / 55; dark 27 / 41 / 55 / 71).
// Text contrast per step (WCAG AA 4.5):
//   step    light                           dark
//   0-24    orange-800 on orange-50  6.93   orange-300 on orange-950 9.20
//   25-49   orange-900 on orange-200 6.94   orange-100 on orange-900 8.21
//   50-74   orange-950 on orange-400 6.58   white on orange-700      5.23
//   75-100  white on orange-700      5.23   orange-950 on orange-500 5.42
const STEPS = [
  { min: 75, cls: 'bg-orange-700 text-white dark:bg-orange-500 dark:text-orange-950' },
  { min: 50, cls: 'bg-orange-400 text-orange-950 dark:bg-orange-700 dark:text-white' },
  { min: 25, cls: 'bg-orange-200 text-orange-900 dark:bg-orange-900 dark:text-orange-100' },
  { min: 0, cls: 'bg-orange-50 text-orange-800 dark:bg-orange-950 dark:text-orange-300' },
]

export default function ScoreBadge({ score, large = false }: { score: number | null; large?: boolean }) {
  if (score == null) return <span className="text-muted">—</span>
  const value = Math.round(score)
  const step = STEPS.find((s) => value >= s.min) ?? STEPS[STEPS.length - 1]
  return (
    <span
      className={`inline-block min-w-10 rounded px-2 text-center font-mono font-semibold tabular-nums ${
        large ? 'text-xl leading-8' : 'leading-6'} ${step.cls}`}
      title={`Tremor score ${value} of 100`}
    >
      {value}
    </span>
  )
}
