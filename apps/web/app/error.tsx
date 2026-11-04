// app/error.tsx
'use client'
export default function ErrorPage({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  return (
    <main className="mx-auto max-w-5xl p-4 md:p-6">
      <h2 className="text-xl font-semibold">Couldn&apos;t load data</h2>
      <p className="my-2 text-sm text-muted">The database may be unreachable.</p>
      <button className="rounded border px-4 py-2 text-sm" onClick={reset}>Retry</button>
    </main>
  )
}