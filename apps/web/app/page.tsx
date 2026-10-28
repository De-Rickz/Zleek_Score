// app/page.tsx
import Link from 'next/link'
import { getMarketsByTremor } from '@/lib/queries'
import { fixed, timeAgo, usd } from '@/lib/format'
import AutoRefresh from './components/AutoRefresh'

// Without this, `next build` prerenders the page and freezes today's DB snapshot.
export const dynamic = 'force-dynamic'

const STALE_AFTER_MS = 5 * 60_000

export default async function Home() {
  const markets = await getMarketsByTremor()
  const newest = Math.max(0, ...markets.map((m) => m.scored_at_ms))
  // Async Server Component: runs once per request, not memoized by the compiler,
  // so reading the current time here is safe despite the purity rule.
  // eslint-disable-next-line react-hooks/purity
  const stale = markets.length > 0 && Date.now() - newest > STALE_AFTER_MS

  return (
    <main className="mx-auto max-w-5xl p-6">
      <AutoRefresh />
      <h1 className="mb-4 text-2xl font-semibold">Markets by Tremor Score</h1>

      {stale && (
        <p className="mb-4 rounded bg-amber-100 p-3 text-sm text-amber-900">
          Newest score is {timeAgo(newest)}. The worker may be down.
        </p>
      )}

      {markets.length === 0 ? (
        <p className="text-neutral-500">No scored markets yet.</p>
      ) : (
        <table className="w-full text-sm">
          <thead className="text-left text-neutral-500">
            <tr>
              <th className="py-2">Market</th>
              <th>Category</th>
              <th className="text-right">Liquidity</th>
              <th className="text-right">Tremor</th>
              <th className="text-right">Updated</th>
            </tr>
          </thead>
          <tbody>
            {markets.map((m) => (
              <tr key={m.id} className="border-t">
                <td className="py-2 pr-4">
                  <Link className="hover:underline" href={`/markets/${encodeURIComponent(m.id)}`}>
                    {m.title}
                  </Link>
                </td>
                <td>{m.category ?? '—'}</td>
                <td className="text-right tabular-nums">{usd(m.liquidity_usd)}</td>
                <td className="text-right font-medium tabular-nums">{fixed(m.tremor_score, 0)}</td>
                <td className="text-right text-neutral-500">{timeAgo(m.scored_at_ms)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  )
}