// app/markets/[id]/not-found.tsx
import Link from 'next/link'
export default function NotFound() {
  return (
    <main className="mx-auto max-w-5xl p-6">
      <p>Market not found.</p>
      <Link href="/" className="underline">Back to markets</Link>
    </main>
  )
}