// app/page.tsx
import Link from 'next/link'
import { getMarketsByTremor, getTraces, TRACE_WINDOW_MS } from '@/lib/queries'
import { timeAgo, usd } from '@/lib/format'
import AutoRefresh from './components/AutoRefresh'
import ScoreBadge from './components/ScoreBadge'
import Trace from './components/Trace'
import UpdatedAgo from './components/UpdatedAgo'

// Without this, `next build` prerenders the page and freezes today's DB snapshot.
export const dynamic = 'force-dynamic'

const STALE_AFTER_MS = 5 * 60_000

export default async function Home() {
  const markets = await getMarketsByTremor()
  const traces = await getTraces(markets.map((m) => m.id))
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
        <p className="mb-4 border-l-2 border-accent py-2 pl-4 text-sm">
          Newest score is <span className="font-mono">{timeAgo(newest)}</span>. The worker may be down.
        </p>
      )}

      {markets.length === 0 ? (
        <p className="text-sm text-muted">No scored markets yet.</p>
      ) : (
        <table className="w-full text-sm">
          <thead className="text-left text-muted">
            <tr>
              <th className="py-2">Market</th>
              <th className="pl-4 text-right">Liquidity</th>
              <th className="pl-4">Last 4h</th>
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
                <td className="pl-4 text-right font-mono tabular-nums">{usd(m.liquidity_usd)}</td>
                <td className="pl-4 align-middle">
                  <Trace {...traces.get(m.id)!} start={renderedAt - TRACE_WINDOW_MS} end={renderedAt} />
                </td>
                <td className="pl-4 text-right"><ScoreBadge score={m.tremor_score} /></td>
                <td className="pl-4 text-right font-mono text-muted tabular-nums">{timeAgo(m.scored_at_ms)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  )
}