// app/components/PriceChart.tsx
'use client'
import { useEffect, useRef } from 'react'
import {
  Chart, BarController, BarElement, LineController, LineElement, PointElement,
  LinearScale, CategoryScale, Tooltip, Legend,
} from 'chart.js'

Chart.register(BarController, BarElement, LineController, LineElement, PointElement,
  LinearScale, CategoryScale, Tooltip, Legend)

type Point = { bucket_ms: number; close: number; volume_usd: number }

export default function PriceChart({ bars }: { bars: Point[] }) {
  const ref = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    if (!ref.current) return
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
            borderColor: '#3b82f6', backgroundColor: '#3b82f6' },
          { type: 'bar', label: 'Volume (USD)', data: bars.map((b) => b.volume_usd),
            yAxisID: 'y1', backgroundColor: 'rgba(148, 163, 184, 0.5)' },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false, // chart is rebuilt on each poll; animation would flicker
        scales: {
          y: { position: 'left' },
          y1: { position: 'right', beginAtZero: true, grid: { drawOnChartArea: false } },
        },
      },
    })
    return () => chart.destroy()
  }, [bars])

  return <div className="h-80"><canvas ref={ref} /></div>
}