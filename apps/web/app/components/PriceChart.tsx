// app/components/PriceChart.tsx
'use client'
import { useEffect, useRef } from 'react'
import {
  Chart, BarController, BarElement, LineController, LineElement, PointElement,
  LinearScale, Tooltip, type Plugin,
} from 'chart.js'

// No Legend: the axis titles name both series. Tooltip stays (it carries data).
Chart.register(BarController, BarElement, LineController, LineElement, PointElement,
  LinearScale, Tooltip)

type Point = { bucket_ms: number; close: number; volume_usd: number }

const MINUTE = 60_000
// Tick spacing candidates (minutes); the smallest giving at most ~8 labels wins.
const STEPS_MIN = [1, 2, 5, 10, 15, 30, 60, 120, 180, 240, 360, 720, 1440]

// Colours come from the CSS tokens in globals.css (light/dark, contrast-checked);
// the chart is rebuilt on every poll, so a theme switch is picked up within 10 s.
const token = (name: string) =>
  getComputedStyle(document.documentElement).getPropertyValue(name).trim()

// Pen head: an accent-coloured dot where the price line meets "now", like the
// pen of a chart recorder. Static (no motion).
const penHead = (color: string): Plugin<'bar'> => ({
  id: 'penHead',
  afterDatasetsDraw(chart) {
    const points = chart.getDatasetMeta(0).data   // the price line, in pixels
    const last = points[points.length - 1]
    if (!last) return
    const { ctx } = chart
    ctx.save()
    ctx.fillStyle = color
    ctx.beginPath()
    ctx.arc(last.x, last.y, 4, 0, 2 * Math.PI)
    ctx.fill()
    ctx.restore()
  },
})

const hhmm = (ms: number) =>
  new Date(ms).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })

export default function PriceChart({ bars }: { bars: Point[] }) {
  const ref = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    if (!ref.current) return
    const axis = token('--chart-axis'), grid = token('--chart-grid')
    const mono = { family: token('--font-plex-mono') }  // numbers in IBM Plex Mono
    // The axis runs from the first bar to now, so silence since the last trade
    // shows as empty paper at the right edge.
    const start = bars.length ? bars[0].bucket_ms : Date.now()
    const end = Math.max(Date.now(), bars.length ? bars[bars.length - 1].bucket_ms + MINUTE : 0)
    const span = end - start
    const step = (STEPS_MIN.find((m) => span / (m * MINUTE) <= 8) ?? 1440) * MINUTE
    const chart = new Chart(ref.current, {
      type: 'bar',
      data: {
        // True time axis: x is the bar's time in ms on a linear scale, so a quiet
        // stretch takes its real width and a spike after it stands out (bars only
        // exist for minutes with trades). No date adapter needed.
        datasets: [
          // stepped 'before': the price holds at the last close until the next
          // trade, instead of a straight line implying trades in between.
          // ...and through to now: the pen head sits at the current price.
          { type: 'line', label: 'Close',
            data: [...bars.map((b) => ({ x: b.bucket_ms, y: b.close })),
                   ...(bars.length ? [{ x: end, y: bars[bars.length - 1].close }] : [])],
            yAxisID: 'y', pointRadius: 0, borderWidth: 2, stepped: 'before',
            borderColor: token('--chart-price'), backgroundColor: token('--chart-price') },
          { type: 'bar', label: 'Volume (USD)', data: bars.map((b) => ({ x: b.bucket_ms, y: b.volume_usd })),
            yAxisID: 'y1', backgroundColor: token('--chart-volume') },
        ],
      },
      plugins: [penHead(token('--accent'))],
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false, // chart is rebuilt on each poll; animation would flicker
        plugins: {
          tooltip: { callbacks: { title: (items) => (items.length ? hhmm(items[0].parsed.x ?? 0) : '') } },
        },
        // Recorder paper, minimal ink: faint rules at the round clock times and
        // at price levels only, axis titles instead of a legend.
        scales: {
          x: { type: 'linear', min: start, max: end, offset: false, grid: { color: grid }, border: { display: false },
               // Labels at round clock times (multiples of the step), not counted
               // from the first bar.
               afterBuildTicks: (scale) => {
                 const ticks = []
                 for (let t = Math.ceil(start / step) * step; t <= end; t += step) ticks.push({ value: t })
                 scale.ticks = ticks
               },
               ticks: { color: axis, font: mono, maxRotation: 0, callback: (v) => hhmm(Number(v)) } },
          y: { position: 'left', grid: { color: grid }, border: { display: false },
               ticks: { color: axis, font: mono }, title: { display: true, text: 'Price (YES)', color: axis } },
          y1: { position: 'right', beginAtZero: true, grid: { display: false }, border: { display: false },
                ticks: { color: axis, font: mono }, title: { display: true, text: 'Volume (USD)', color: axis } },
        },
      },
    })
    return () => chart.destroy()
  }, [bars])

  return <div className="h-80"><canvas ref={ref} /></div>
}
