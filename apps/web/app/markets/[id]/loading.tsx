// app/markets/[id]/loading.tsx: market detail skeleton (header, stats, chart,
// trades), shown while navigating to the page.
import Skeleton from '@/app/components/Skeleton'

export default function Loading() {
  return (
    <main className="mx-auto max-w-5xl space-y-6 p-4 md:p-6" aria-busy="true">
      <span className="sr-only">Loading market…</span>
      <Skeleton className="h-4 w-24" />
      <div className="space-y-2">
        <Skeleton className="h-8 w-3/4" />
        <Skeleton className="h-4 w-48" />
      </div>
      <div className="grid grid-cols-3 gap-4">
        {Array.from({ length: 3 }, (_, i) => <Skeleton key={i} className="h-24" />)}
      </div>
      <Skeleton className="h-80" />
      <div className="space-y-2">
        {Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-4" />)}
      </div>
    </main>
  )
}
