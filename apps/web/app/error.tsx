// app/error.tsx
'use client'
export default function ErrorPage({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  return (
    <main className="mx-auto max-w-5xl p-6">
      <h2 className="text-lg font-semibold">Couldn&apos;t load data</h2>
      <p className="my-2 text-neutral-500">The database may be unreachable.</p>
      <button className="rounded border px-3 py-1" onClick={reset}>Retry</button>
    </main>
  )
}