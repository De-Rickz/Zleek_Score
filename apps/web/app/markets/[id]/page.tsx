// app/markets/[id]/page.tsx
import Link from 'next/link'
import { notFound } from 'next/navigation'
import { getBars, getLatestFeatures, getMarket, getRecentTrades, getSignal } from '@/lib/queries'
import { fixed, timeAgo, usd } from '@/lib/format'
import AutoRefresh from '@/app/components/AutoRefresh'
import PriceChart from '@/app/components/PriceChart'

export const dynamic = 'force-dynamic'

// params is a Promise in Next 15+/16, so it must be awaited
export default async function MarketPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params
  const market = await getMarket(id)
  if (!market) notFound()

  const [signal, features, bars, trades] = await Promise.all([
    getSignal(id), getLatestFeatures(id), getBars(id), getRecentTrades(id),
  ])

  return (
    <main className="mx-auto max-w-5xl space-y-6 p-6">
      <AutoRefresh />
      <Link href="/" className="text-sm text-neutral-500 hover:underline">← Markets</Link>
      <header>
        <h1 className="text-2xl font-semibold">{market.title}</h1>
        <p className="text-sm text-neutral-500">
          {market.category ?? 'Uncategorised'} · {usd(market.liquidity_usd)} liquidity
        </p>
      </header>

      <section className="grid grid-cols-3 gap-4">
        <Stat label="Tremor score" value={signal ? fixed(signal.tremor_score, 0) : '—'}
              sub={signal ? timeAgo(signal.scored_at_ms) : undefined} />
        <Stat label="Price z-score" value={features ? fixed(features.price_zscore, 2) : '—'} />
        {/* Not part of the tremor score; null when the book is stale or one-sided */}
        {features?.ob_imbalance != null
          ? <Stat label="Order-book imbalance" value={fixed(features.ob_imbalance, 2)}
                  sub="whole book, incl. orders far from the price" />
          : <Stat label="Order-book imbalance" value="n/a" />}
      </section>

      <section>
        <h2 className="mb-2 font-medium">1-minute price & volume</h2>
        {bars.length === 0
          ? <p className="text-neutral-500">No 1-minute bars yet for this market.</p>
          : <PriceChart bars={bars} />}
      </section>

      <section>
        <h2 className="mb-2 font-medium">Recent trades</h2>
        <table className="w-full text-sm">
          <tbody>
            {trades.map((t, i) => (
              <tr key={i} className="border-t">
                <td className="py-1 text-neutral-500">{timeAgo(t.ts_ms)}</td>
                <td>{t.side}</td>
                <td className="text-right tabular-nums">{t.price.toFixed(3)}</td>
                <td className="text-right tabular-nums">{usd(t.size_usd)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  )
}

function Stat({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="rounded border p-4">
      <div className="text-xs text-neutral-500">{label}</div>
      <div className="text-2xl font-semibold tabular-nums">{value}</div>
      {sub && <div className="text-xs text-neutral-500">{sub}</div>}
    </div>
  )
}