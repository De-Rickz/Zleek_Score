// app/page.tsx
import Link from 'next/link'
import { getMarketsByTremor } from '@/lib/queries'
import { timeAgo, usd } from '@/lib/format'
import AutoRefresh from './components/AutoRefresh'
import ScoreBadge from './components/ScoreBadge'
import UpdatedAgo from './components/UpdatedAgo'

// Without this, `next build` prerenders the page and freezes today's DB snapshot.
export const dynamic = 'force-dynamic'

const STALE_AFTER_MS = 5 * 60_000

export default async function Home() {
  const markets = await getMarketsByTremor()
  const newest = Math.max(0, ...markets.map((m) => m.scored_at_ms))
  // Async Server Component: runs once per request, not memoized by the compiler,
  // so reading the current time here is safe despite the purity rule.
  // eslint-disable-next-line react-hooks/purity
  const renderedAt = Date.now()
  const stale = markets.length > 0 && renderedAt - newest > STALE_AFTER_MS

  return (
    <main className="mx-auto max-w-5xl p-6">
      <AutoRefresh />
      <div className="mb-4 flex items-baseline justify-between gap-4">
        <h1 className="text-xl font-semibold">Markets by Tremor Score</h1>
        <UpdatedAgo key={renderedAt} />
      </div>

      {stale && (
        <p className="mb-4 rounded bg-warn-bg px-4 py-2 text-sm text-warn-fg">
          Newest score is {timeAgo(newest)}. The worker may be down.
        </p>
      )}

      {markets.length === 0 ? (
        <p className="text-sm text-muted">No scored markets yet.</p>
      ) : (
        <table className="w-full text-sm">
          <thead className="text-left text-muted">
            <tr>
              <th className="py-2">Market</th>
              <th>Category</th>
              <th className="pl-4 text-right">Liquidity</th>
              <th className="pl-4 text-right">Tremor</th>
              <th className="pl-4 text-right">Updated</th>
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
                <td className="pl-4 text-right tabular-nums">{usd(m.liquidity_usd)}</td>
                <td className="pl-4 text-right"><ScoreBadge score={m.tremor_score} /></td>
                <td className="pl-4 text-right text-muted tabular-nums">{timeAgo(m.scored_at_ms)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  )
}