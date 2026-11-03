// app/components/ScoreBadge.tsx
// Tremor score as a badge on a four-step single-hue (violet) ramp: the only
// strong colour in a dashboard row. The number is always shown, so colour is
// never the only signal. Text contrast per step, light / dark (WCAG AA 4.5):
//   0-24   violet-800 on violet-50  8.35  |  violet-300 on violet-950 8.21
//   25-49  violet-900 on violet-100 9.28  |  violet-100 on violet-900 9.28
//   50-74  violet-950 on violet-300 8.21  |  white on violet-700      7.29
//   75-100 white on violet-700      7.29  |  violet-950 on violet-300 8.21
const STEPS = [
  { min: 75, cls: 'bg-violet-700 text-white dark:bg-violet-300 dark:text-violet-950' },
  { min: 50, cls: 'bg-violet-300 text-violet-950 dark:bg-violet-700 dark:text-white' },
  { min: 25, cls: 'bg-violet-100 text-violet-900 dark:bg-violet-900 dark:text-violet-100' },
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
