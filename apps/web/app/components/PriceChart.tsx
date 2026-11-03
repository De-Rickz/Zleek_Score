// app/components/PriceChart.tsx
'use client'
import { useEffect, useRef } from 'react'
import {
  Chart, BarController, BarElement, LineController, LineElement, PointElement,
  LinearScale, CategoryScale, Tooltip,
} from 'chart.js'

// No Legend: the axis titles name both series. Tooltip stays (it carries data).
Chart.register(BarController, BarElement, LineController, LineElement, PointElement,
  LinearScale, CategoryScale, Tooltip)

type Point = { bucket_ms: number; close: number; volume_usd: number }

// Colours come from the CSS tokens in globals.css (light/dark, contrast-checked);
// the chart is rebuilt on every poll, so a theme switch is picked up within 10 s.
const token = (name: string) =>
  getComputedStyle(document.documentElement).getPropertyValue(name).trim()

export default function PriceChart({ bars }: { bars: Point[] }) {
  const ref = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    if (!ref.current) return
    const axis = token('--chart-axis'), grid = token('--chart-grid')
    const chart = new Chart(ref.current, {
      type: 'bar',
      data: {
        labels: bars.map((b) =>
          new Date(b.bucket_ms).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })),
        // Explicit colours: without the Colors plugin Chart.js defaults to
        // rgba(0,0,0,0.1), which is invisible on the dark background.
        datasets: [
          { type: 'line', label: 'Close', data: bars.map((b) => b.close),
            yAxisID: 'y', pointRadius: 0, borderWidth: 2,
            borderColor: token('--chart-price'), backgroundColor: token('--chart-price') },
          { type: 'bar', label: 'Volume (USD)', data: bars.map((b) => b.volume_usd),
            yAxisID: 'y1', backgroundColor: token('--chart-volume') },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false, // chart is rebuilt on each poll; animation would flicker
        // Minimal ink: no vertical gridlines, faint horizontal ones for price
        // only, about 8 time labels, axis titles instead of a legend.
        scales: {
          x: { grid: { display: false }, ticks: { color: axis, maxTicksLimit: 8, maxRotation: 0 } },
          y: { position: 'left', grid: { color: grid }, border: { display: false },
               ticks: { color: axis }, title: { display: true, text: 'Price (YES)', color: axis } },
          y1: { position: 'right', beginAtZero: true, grid: { display: false }, border: { display: false },
                ticks: { color: axis }, title: { display: true, text: 'Volume (USD)', color: axis } },
        },
      },
    })
    return () => chart.destroy()
  }, [bars])

  return <div className="h-80"><canvas ref={ref} /></div>
}