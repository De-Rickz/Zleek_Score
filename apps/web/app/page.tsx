// app/page.tsx
import Link from 'next/link'
import { getDashboardRows, getTraces, TRACE_WINDOW_MS } from '@/lib/queries'
import { timeAgo, usd } from '@/lib/format'
import { isEnded, isLive, notLiveSummary, oldDataMarker } from '@/lib/freshness'
import type { DashboardRow } from '@/lib/types'
import AutoRefresh from './components/AutoRefresh'
import ScoreBadge from './components/ScoreBadge'
import Trace, { type TraceData } from './components/Trace'
import UpdatedAgo from './components/UpdatedAgo'

// Without this, `next build` prerenders the page and freezes today's DB snapshot.
export const dynamic = 'force-dynamic'

const STALE_AFTER_MS = 5 * 60_000
const LIVE_LIMIT = 100       // live markets listed, by score
const NOT_LIVE_LIMIT = 100   // not-live markets listed when expanded, by liquidity

// Newest data for a market: its newest trade, else its newest bar.
const lastDataMs = (m: DashboardRow) => m.last_trade_ms ?? m.last_bar_ms

export default async function Home() {
  const rows = await getDashboardRows()
  // Async Server Component: runs once per request, not memoized by the compiler,
  // so reading the current time here is safe despite the purity rule.
  // eslint-disable-next-line react-hooks/purity
  const renderedAt = Date.now()

  // Ended markets are hidden (lib/freshness.ts: closed or resolved, not accepting
  // orders, or past the end date with no trade in 3 hours). The rest split into
  // live (enough trades in the last 4 hours to score) and not live.
  const shown = rows.filter((m) => !isEnded({
    status: m.status, gammaClosed: m.gamma_closed, acceptingOrders: m.accepting_orders,
    endMs: m.end_ms, lastTradeMs: m.last_trade_ms,
  }, renderedAt))
  const live = shown.filter((m) => isLive(m.bars_in_window))
    .sort((a, b) => (b.tremor_score ?? 0) - (a.tremor_score ?? 0)).slice(0, LIVE_LIMIT)
  const notLiveAll = shown.filter((m) => !isLive(m.bars_in_window))
    .sort((a, b) => (b.liquidity_usd ?? 0) - (a.liquidity_usd ?? 0))
  const notLive = notLiveAll.slice(0, NOT_LIVE_LIMIT)

  const traces = await getTraces([...live, ...notLive].map((m) => m.id))
  // Worker check: when did scoring last run (any market, ended or not)?
  const newestScore = Math.max(0, ...rows.map((m) => m.scored_at_ms))
  const stale = rows.length > 0 && renderedAt - newestScore > STALE_AFTER_MS
  const traceWindow = { start: renderedAt - TRACE_WINDOW_MS, end: renderedAt }

  return (
    <main className="mx-auto max-w-5xl p-4 md:p-6">
      <AutoRefresh />
      <div className="mb-4 flex items-baseline justify-between gap-4">
        <h1 className="text-xl font-semibold">Markets by Tremor Score</h1>
        <UpdatedAgo key={renderedAt} />
      </div>

      {stale && (
        <p className="mb-4 border-l-2 border-accent py-2 pl-4 text-sm">
          Newest score is <span className="font-mono">{timeAgo(newestScore)}</span>. The worker may be down.
        </p>
      )}

      {shown.length === 0 ? (
        <p className="text-sm text-muted">No scored markets yet.</p>
      ) : (
        <>
          <Markets head rows={live} traces={traces} {...traceWindow}
                   score={(m) => <ScoreBadge score={m.tremor_score} />}
                   marker={(m) => oldDataMarker(lastDataMs(m), renderedAt)} />

          {notLiveAll.length > 0 && (
            // Native disclosure: no JavaScript, keyboard accessible, and it keeps
            // its open state across the 10 s refresh.
            <details className="group border-t">
              <summary className="cursor-pointer py-2 text-sm text-muted hover:text-foreground">
                {notLiveSummary(notLiveAll.length)}
                {notLiveAll.length > notLive.length && `, showing ${notLive.length} by liquidity`}
              </summary>
              <Markets rows={notLive} traces={traces} {...traceWindow}
                       score={() => (
                         <span className="font-mono text-muted" title="Not live: too little trading in the last 4 hours to score">
                           — <span className="text-xs">not live</span>
                         </span>
                       )}
                       marker={() => null} />
            </details>
          )}
        </>
      )}
    </main>
  )
}

type MarketsProps = {
  rows: DashboardRow[]
  traces: Map<string, TraceData>
  start: number
  end: number
  head?: boolean
  score: (m: DashboardRow) => React.ReactNode
  marker: (m: DashboardRow) => string | null
}

// The same rows two ways, one shown at a time: a table from 768 px (md) up, and
// below that a stacked list, one tappable row per market (title and badge,
// then trace, liquidity and data age), the usual phone pattern for wide tables.
function Markets({ rows, traces, start, end, head = false, score, marker }: MarketsProps) {
  return (
    <>
      <MarketTable head={head}>
        {rows.map((m) => (
          <Row key={m.id} m={m} trace={traces.get(m.id)!} start={start} end={end}>
            {marker(m) && <span className="mr-2 font-mono text-xs text-muted">{marker(m)}</span>}
            {score(m)}
          </Row>
        ))}
      </MarketTable>
      <ul className="md:hidden">
        {rows.map((m) => (
          <MobileRow key={m.id} m={m} trace={traces.get(m.id)!} start={start} end={end}
                     score={score(m)} marker={marker(m)} />
        ))}
      </ul>
    </>
  )
}

function MobileRow({ m, trace, start, end, score, marker }: {
  m: DashboardRow; trace: TraceData; start: number; end: number; score: React.ReactNode; marker: string | null
}) {
  const data = lastDataMs(m)
  return (
    <li className="border-t">
      <Link href={`/markets/${encodeURIComponent(m.id)}`} className="block py-3">
        <div className="flex items-start justify-between gap-4">
          <span className="line-clamp-2 text-sm">{m.title}</span>
          <span className="shrink-0 whitespace-nowrap">{score}</span>
        </div>
        <div className="mt-2 flex items-center justify-between gap-4">
          <Trace {...trace} start={start} end={end} />
          <span className="text-right font-mono text-xs text-muted tabular-nums">
            {usd(m.liquidity_usd)} · {data == null ? '—' : timeAgo(data)}
            {marker && <><br />{marker}</>}
          </span>
        </div>
      </Link>
    </li>
  )
}

// Both tables share fixed column widths so the expanded not-live rows line up
// with the live rows above them. Desktop only (hidden below md).
function MarketTable({ head = false, children }: { head?: boolean; children: React.ReactNode }) {
  return (
    <table className="hidden w-full table-fixed text-sm md:table">
      <colgroup>
        <col />
        <col className="w-28" />
        <col className="w-44" />
        <col className="w-36" />
        <col className="w-24" />
      </colgroup>
      {head && (
        <thead className="text-left text-muted">
          <tr>
            <th className="py-2">Market</th>
            <th className="pl-4 text-right">Liquidity</th>
            <th className="pl-4">Last 4h</th>
            <th className="pl-4 text-right">Tremor</th>
            <th className="pl-4 text-right" title="Time of the newest trade">Updated</th>
          </tr>
        </thead>
      )}
      <tbody>{children}</tbody>
    </table>
  )
}

function Row({ m, trace, start, end, children }: {
  m: DashboardRow; trace: TraceData; start: number; end: number; children: React.ReactNode
}) {
  const data = lastDataMs(m)
  return (
    <tr className="border-t">
      <td className="py-2 pr-4">
        <Link className="hover:underline" href={`/markets/${encodeURIComponent(m.id)}`}>{m.title}</Link>
      </td>
      <td className="pl-4 text-right font-mono tabular-nums">{usd(m.liquidity_usd)}</td>
      <td className="pl-4 align-middle"><Trace {...trace} start={start} end={end} /></td>
      <td className="pl-4 text-right whitespace-nowrap">{children}</td>
      {/* Age of the data (newest trade), not when scoring last ran */}
      <td className="pl-4 text-right font-mono text-muted tabular-nums">{data == null ? '—' : timeAgo(data)}</td>
    </tr>
  )
}
