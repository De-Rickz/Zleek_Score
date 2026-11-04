// app/loading.tsx: dashboard skeleton, shown while navigating to the page.
// The 10 s background refresh (router.refresh) swaps content in place and
// never shows this, so there's no flicker on each poll.
import Skeleton from './components/Skeleton'

export default function Loading() {
  return (
    <main className="mx-auto max-w-5xl p-6" aria-busy="true">
      <span className="sr-only">Loading markets…</span>
      <div className="mb-4 flex items-baseline justify-between">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-4 w-24" />
      </div>
      <div className="space-y-2">
        {Array.from({ length: 12 }, (_, i) => (
          <div key={i} className="flex items-center gap-4 border-t py-2">
            <Skeleton className="h-4 flex-1" />
            <Skeleton className="h-4 w-24" />
            <Skeleton className="h-4 w-16" />
            <Skeleton className="h-6 w-10" />
            <Skeleton className="h-4 w-16" />
          </div>
        ))}
      </div>
    </main>
  )
}
