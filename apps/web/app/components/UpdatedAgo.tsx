// app/components/UpdatedAgo.tsx
'use client'
import { useEffect, useState } from 'react'

// "Updated 4s ago" for the polling refresh. The page renders it with
// key={renderedAt}, so every successful refresh (AutoRefresh, 10 s) remounts
// it at 0; if refreshes stop (network down, tab hidden) it keeps counting.
export default function UpdatedAgo() {
  const [since] = useState(() => Date.now())
  const [now, setNow] = useState(since)
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(id)
  }, [])
  const s = Math.max(0, Math.round((now - since) / 1000))
  const ago = s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${s % 60}s`
  return <span className="text-xs text-muted tabular-nums">Updated {ago} ago</span>
}
