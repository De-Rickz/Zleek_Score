// app/markets/[id]/page.tsx
import type { Metadata } from 'next'
import Link from 'next/link'
import { notFound } from 'next/navigation'
import { getBars, getLatestFeatures, getMarket, getPriceChangeRow, getRecentTrades, getSignal } from '@/lib/queries'
import { describeChange } from '@/lib/priceChange'
import { isLive, oldDataMarker, SCORE_WINDOW_MS } from '@/lib/freshness'
import { fixed, timeAgo, usd } from '@/lib/format'
import AutoRefresh from '@/app/components/AutoRefresh'
import PriceChart from '@/app/components/PriceChart'
import ScoreScale from '@/app/components/ScoreScale'
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

  const [signal, features, bars, trades, changeRow] = await Promise.all([
    getSignal(id), getLatestFeatures(id), getBars(id), getRecentTrades(id), getPriceChangeRow(id),
  ])
  const change = describeChange(changeRow, (ms) =>
    new Date(ms).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }))
  // Server Component: runs once per request, so reading the time is safe.
  // eslint-disable-next-line react-hooks/purity
  const renderedAt = Date.now()
  // Same rule as the dashboard: with fewer than 10 bars in the last 4 hours the
  // worker has nothing to measure, so show "not live" rather than a 0.
  const live = isLive(bars.filter((b) => b.bucket_ms > renderedAt - SCORE_WINDOW_MS).length)
  const lastTradeMs = trades[0]?.ts_ms ?? null   // recent trades are newest first
  const marker = live ? oldDataMarker(lastTradeMs, renderedAt) : null

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
          {hasCategory(market.category) && <>{market.category} · </>}
          <span className="font-mono">{usd(market.liquidity_usd)}</span> liquidity
        </p>
      </header>

      {/* Readout strip: flat, thin dividers. Tremor score as an instrument
          reading; z-score and imbalance as smaller readings beside it. */}
      <section className="grid grid-cols-[2fr_1fr_1fr_1fr] divide-x border-y">
        <div className="py-4 pr-6">
          <Label>Tremor score</Label>
          <div className="flex items-baseline gap-4">
            <span className="font-mono text-xl leading-8 font-semibold tabular-nums">
              {!live || signal?.tremor_score == null ? '—' : Math.round(signal.tremor_score)}
            </span>
            {/* Age of the data, not when scoring last ran (the worker re-stamps every 5 min) */}
            {!live
              ? <span className="text-xs text-muted">not live: too little trading in the last 4 hours to score</span>
              : marker
                ? <span className="font-mono text-xs text-muted">{marker}</span>
                : lastTradeMs != null && <span className="font-mono text-xs text-muted">last trade {timeAgo(lastTradeMs)}</span>}
          </div>
          <ScoreScale score={live ? signal?.tremor_score ?? null : null} />
        </div>
        {/* Ink, not orange: the arrow and sign carry the direction. Tooltip gives
            the reference time or why there's no change. */}
        <Reading label="1h change"
                 value={change.kind === 'change'
                   ? <>{change.arrow && <span className="mr-1 align-[0.1em] text-xs">{change.arrow}</span>}{change.value}</>
                   : '—'}
                 title={change.kind === 'change' ? change.label : change.reason}
                 note={change.kind === 'change' ? `since ${new Date(change.refMs).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })}` : change.reason} />
        <Reading label="Price z-score" value={features ? fixed(features.price_zscore, 2) : '—'}
                 note="3 or more scores 100" />
        {/* Not part of the tremor score; null when the book is stale or one-sided */}
        <Reading label="Order-book imbalance"
                 value={features?.ob_imbalance != null ? fixed(features.ob_imbalance, 2) : 'n/a'}
                 note={features?.ob_imbalance != null ? 'whole book, incl. orders far from the price' : undefined} />
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
          <thead className="text-left text-muted">
            <tr>
              <th className="py-1">Time</th>
              <th>Side</th>
              <th className="text-right">Price (YES)</th>
              <th className="text-right">Size (USD)</th>
            </tr>
          </thead>
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

// Gamma sends "Uncategorized" (or nothing) for markets without a category;
// show nothing rather than a placeholder word.
const hasCategory = (c: string | null) => !!c && c.toLowerCase() !== 'uncategorized'

function Label({ children }: { children: React.ReactNode }) {
  return <div className="text-xs text-muted">{children}</div>
}

function Reading({ label, value, note, title }: { label: string; value: React.ReactNode; note?: string; title?: string }) {
  return (
    <div className="px-6 py-4" title={title}>
      <Label>{label}</Label>
      <div className="font-mono text-xl leading-8 font-semibold tabular-nums">{value}</div>
      {note && <div className="text-xs text-muted">{note}</div>}
    </div>
  )
}
