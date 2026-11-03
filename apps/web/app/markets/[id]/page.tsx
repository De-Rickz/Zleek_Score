// app/markets/[id]/page.tsx
import type { Metadata } from 'next'
import Link from 'next/link'
import { notFound } from 'next/navigation'
import { getBars, getLatestFeatures, getMarket, getRecentTrades, getSignal } from '@/lib/queries'
import { fixed, timeAgo, usd } from '@/lib/format'
import AutoRefresh from '@/app/components/AutoRefresh'
import PriceChart from '@/app/components/PriceChart'
import ScoreBadge from '@/app/components/ScoreBadge'
import UpdatedAgo from '@/app/components/UpdatedAgo'

export const dynamic = 'force-dynamic'

// Tab/link title, e.g. "Will the Democrats win ...? · Zleek Score" (template in
// app/layout.tsx). Unknown ids fall back to the default title; the page 404s.
export async function generateMetadata({ params }: { params: Promise<{ id: string }> }): Promise<Metadata> {
  const { id } = await params
  const market = await getMarket(id)
  return market ? { title: market.title } : {}
}

// params is a Promise in Next 15+/16, so it must be awaited
export default async function MarketPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params
  const market = await getMarket(id)
  if (!market) notFound()

  const [signal, features, bars, trades] = await Promise.all([
    getSignal(id), getLatestFeatures(id), getBars(id), getRecentTrades(id),
  ])
  // Server Component: runs once per request, so reading the time is safe.
  // eslint-disable-next-line react-hooks/purity
  const renderedAt = Date.now()

  return (
    <main className="mx-auto max-w-5xl space-y-6 p-6">
      <AutoRefresh />
      <div className="flex items-baseline justify-between gap-4">
        <Link href="/" className="text-sm text-muted hover:underline">← Markets</Link>
        <UpdatedAgo key={renderedAt} />
      </div>
      <header>
        <h1 className="text-xl font-semibold">{market.title}</h1>
        <p className="text-sm text-muted">
          {market.category ?? 'Uncategorised'} · <span className="font-mono">{usd(market.liquidity_usd)}</span> liquidity
        </p>
      </header>

      <section className="grid grid-cols-3 gap-4">
        <Stat label="Tremor score" value={<ScoreBadge score={signal?.tremor_score ?? null} large />}
              sub={signal ? <span className="font-mono">{timeAgo(signal.scored_at_ms)}</span> : undefined} />
        <Stat label="Price z-score" value={features ? fixed(features.price_zscore, 2) : '—'} />
        {/* Not part of the tremor score; null when the book is stale or one-sided */}
        {features?.ob_imbalance != null
          ? <Stat label="Order-book imbalance" value={fixed(features.ob_imbalance, 2)}
                  sub="whole book, incl. orders far from the price" />
          : <Stat label="Order-book imbalance" value="n/a" />}
      </section>

      <section>
        <h2 className="mb-2 text-sm font-semibold">1-minute price & volume</h2>
        {bars.length === 0
          ? <p className="text-sm text-muted">No 1-minute bars yet for this market.</p>
          : <PriceChart bars={bars} />}
      </section>

      <section>
        <h2 className="mb-2 text-sm font-semibold">Recent trades</h2>
        <table className="w-full text-sm">
          <tbody>
            {trades.map((t, i) => (
              <tr key={i} className="border-t">
                <td className="py-1 font-mono text-muted tabular-nums">{timeAgo(t.ts_ms)}</td>
                <td>{t.side}</td>
                <td className="text-right font-mono tabular-nums">{t.price.toFixed(3)}</td>
                <td className="text-right font-mono tabular-nums">{usd(t.size_usd)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  )
}

function Stat({ label, value, sub }: { label: string; value: React.ReactNode; sub?: React.ReactNode }) {
  return (
    <div className="rounded border p-4">
      <div className="text-xs text-muted">{label}</div>
      <div className="font-mono text-xl leading-8 font-semibold tabular-nums">{value}</div>
      {sub && <div className="text-xs text-muted tabular-nums">{sub}</div>}
    </div>
  )
}