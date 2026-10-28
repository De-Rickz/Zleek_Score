// app/components/AutoRefresh.tsx
'use client'
import { useEffect } from 'react'
import { useRouter } from 'next/navigation'

export default function AutoRefresh({ intervalMs = 10_000 }: { intervalMs?: number }) {
  const router = useRouter()
  useEffect(() => {
    const id = setInterval(() => {
      if (document.visibilityState === 'visible') router.refresh() // re-runs server components
    }, intervalMs)
    return () => clearInterval(id)
  }, [router, intervalMs])
  return null
}