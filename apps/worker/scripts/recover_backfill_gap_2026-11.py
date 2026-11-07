"""
One-off recovery of the 2026-11-02..06 backfill gap (ROADMAP phase 4 step 2b).

Polymarket retired Data API v1 on 2026-11-02; from 22:27:43 UTC that day
every backfill request failed until step 2b (Data API v2) deployed on
2026-11-06 (the startup gap backfill re-fetched from 15:58 UTC). This script
re-fetches that window from v2 and stores what is missing, through the same
code as the live backfill: load_condition_ids_for_backfill, condition_batches,
fetch_public_trades, queue_public_trades (YES terms, size in USD), the
writer's _map_rest and REST_QUERY (ON CONFLICT (tx_hash), price updated only
when it changed). Only the write path differs: direct batched inserts instead
of the Redis queue.

Markets, reported as two groups:
  backfill   today's backfill selection (200 by liquidity, websocket markets
             excluded): the stand-in for the selection that shifted during the
             gap. Markets that have ended since are not in it.
  websocket  today's websocket markets: stored by the websocket for nearly all
             of the gap, but some were backfill-only on some days. The dry run
             always reports how many of their trades would be NEW; --apply
             writes them only with --include-websocket-markets.

Per batch, the report shows the oldest trade reached and why paging stopped:
"reached window start" or "feed ended (no older)" are complete; an HTTP error
or a 429 that outlasted the retries makes the batch INCOMPLETE (its trades are
missing from the counts, not absent), and the other batches still run.
Requests are spaced 0.5 s apart (/v2/trades allows 300 per 10 s per IP and
throttles, i.e. delays, anything over). Each request has a 20 s timeout; any
request slower than 3 s is logged, since throttling shows up as delay;
timeouts, 429 and 503 are retried with backoff (Retry-After when given, else
2, 4, 8, 16 s).

Modes:
  default    DRY RUN, read-only (the database session is read-only): fetch,
             map, look up which tx_hashes are already stored, report per group
             and per day, with a sample of new rows.
  --apply    insert the new trades (about 1,000 per transaction), then rebuild
             bars_1m for the window (bars_jobs.rebuild_bars: only bars whose
             values change are rewritten). features_5m and signals_history are
             deliberately left as recorded.

Idempotent and safe while the worker runs: everything goes through
ON CONFLICT (tx_hash), so a second --apply inserts 0 rows, and a run that stops
part-way leaves committed batches in place for the next run to complete.

Run on the server, in a one-off container from the worker image with the
worker's own environment (its DATABASE_URL), without touching the running
worker:
  $PROD run --rm --no-deps worker python /app/scripts/recover_backfill_gap_2026-11.py
  $PROD run --rm --no-deps worker python /app/scripts/recover_backfill_gap_2026-11.py --apply
"""
import argparse
import asyncio
import collections
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import asyncpg
import httpx

import writer as w

DEFAULT_START = "2026-11-02T22:20:00Z"   # a few minutes before the last v1 trade (22:27:43)
DEFAULT_END = "2026-11-06T16:05:00Z"     # a few minutes after 2b deployed (gap backfill from 15:58)
INSERT_BATCH = 1000
LOOKUP_BATCH = 5000
REQUEST_GAP_SEC = 0.5     # ~20 req / 10 s, far under /v2/trades' 300 req / 10 s
REQUEST_TIMEOUT_SEC = 20  # per request; a timeout is retried with backoff
SLOW_REQUEST_SEC = 3      # /v2 throttles (delays) excess requests rather than rejecting them,
                          # so a slow response is the early sign of hitting the limit: logged
MAX_ATTEMPTS = 5          # per page, for timeouts, 429 and 503


def parse_utc(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


class Collector:
    """Stands in for the writer: queue_public_trades hands it mapped items."""
    def __init__(self):
        self.items = []

    async def add_item(self, item):
        self.items.append(item)


class Stats:
    requests = retries = slow = 0
    slowest = 0.0


async def get_page(client, params):
    """One /v2/trades page: (trades, next_cursor). Each request has a timeout;
    a request slower than SLOW_REQUEST_SEC is logged (throttling shows up as
    delay). Timeouts, 429 and 503 are retried up to MAX_ATTEMPTS times, waiting
    Retry-After when given, else 2, 4, 8, 16 s. Any other error raises."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        t0 = asyncio.get_running_loop().time()
        Stats.requests += 1
        try:
            resp = await client.get(w.DATA_API_TRADES, params=params, timeout=REQUEST_TIMEOUT_SEC)
        except httpx.TimeoutException:
            resp = None
        took = asyncio.get_running_loop().time() - t0
        Stats.slowest = max(Stats.slowest, took)
        if took > SLOW_REQUEST_SEC:
            Stats.slow += 1
            print(f"   slow request: {took:.1f}s ({'timeout' if resp is None else resp.status_code}, "
                  f"{params['condition'].count(',') + 1} markets, {'cursor' if 'cursor' in params else 'first page'})")
        if resp is not None and resp.status_code not in (429, 503):
            resp.raise_for_status()
            body = resp.json()
            return body.get("data") or [], (body.get("pagination") or {}).get("next_cursor")
        if attempt == MAX_ATTEMPTS:
            if resp is None:
                raise TimeoutError(f"no response within {REQUEST_TIMEOUT_SEC}s, {MAX_ATTEMPTS} attempts")
            resp.raise_for_status()
        wait = 2.0 ** attempt
        if resp is not None:
            try:
                wait = float(resp.headers.get("Retry-After", "")) or wait
            except ValueError:
                pass
        Stats.retries += 1
        print(f"   retry {attempt}/{MAX_ATTEMPTS - 1} in {min(wait, 60):.0f}s after "
              f"{'timeout' if resp is None else resp.status_code}")
        await asyncio.sleep(min(wait, 60))


async def fetch_batch(client, batch, start_unix, end_unix):
    """Page one batch (up to 20 markets) back to the window start, like
    writer.fetch_public_trades, but also say how far it got and why it
    stopped. Pages come from get_page (timeout, slow-request logging, backoff
    on 429/503/timeouts). Returns (trades, info); info status:
      "reached window start"      a page went back past start_unix (complete)
      "feed ended (no older)"     the cursor ran out: there are no older trades (complete)
    An exception (an HTTP error, or a timeout/429 that outlasted the retries) propagates
    to the caller, which records the batch as incomplete."""
    filters = {"condition": ",".join(batch), "limit": w.BACKFILL_PAGE_SIZE}
    trades, seen, cursor, pages, oldest = [], set(), None, 0, None
    while True:
        params = filters if cursor is None else {**filters, "cursor": cursor}
        page, cursor = await get_page(client, params)
        pages += 1
        await asyncio.sleep(REQUEST_GAP_SEC)
        for t in page:
            ts = t.get("timestamp", 0)
            oldest = ts if oldest is None else min(oldest, ts)
            if ts < start_unix or ts > end_unix:
                continue
            key = w._trade_key(t)
            if key not in seen:
                seen.add(key)
                trades.append(t)
        if page and page[-1].get("timestamp", 0) < start_unix:
            return trades, {"pages": pages, "oldest": oldest, "status": "reached window start", "complete": True}
        if not page or not cursor:
            return trades, {"pages": pages, "oldest": oldest, "status": "feed ended (no older)", "complete": True}


async def fetch_group(client, conds, start_unix, end_unix, asset_maps):
    """Fetch and map one group's trades in the window, batch by batch. A
    failing batch is recorded as incomplete and the others still run.
    Returns (fetched, unmapped, items, markets_with_trades, batch_infos)."""
    fetched = unmapped = 0
    items, markets, infos = [], set(), []
    for i, batch in enumerate(w.condition_batches(conds), 1):
        try:
            trades, info = await fetch_batch(client, batch, start_unix, end_unix)
        except Exception as e:
            trades, info = [], {"pages": None, "oldest": None, "complete": False,
                                "status": f"INCOMPLETE: {type(e).__name__}: {str(e)[:120]}"}
        info.update(batch=i, markets=len(batch), trades=len(trades))
        infos.append(info)
        fetched += len(trades)
        col = Collector()
        mapped = await w.queue_public_trades(col, trades, asset_maps)
        unmapped += len(trades) - mapped
        items += col.items
        markets.update(t.get("condition_id") for t in trades)
    return fetched, unmapped, items, markets, infos


def fmt_unix(ts):
    return "-" if ts is None else datetime.fromtimestamp(ts, timezone.utc).strftime("%m-%d %H:%M")


def split_new(items, existing):
    """New items (first per tx_hash, not already stored) and how many were
    already stored. Items without a tx_hash can't be deduped and are skipped."""
    new, seen, already = [], set(), 0
    for e in items:
        h = e.get("tx_hash")
        if not h or h in seen:
            continue
        seen.add(h)
        if h in existing:
            already += 1
        else:
            new.append(e)
    return new, already


NEAR_SEC = 300   # a missing trade this close to a stored one of the same market counts as "near"


def classify_missing(new, stored_ts):
    """Split NEW (missing) trades by whether the market was being followed at
    that moment. stored_ts: market_id -> sorted epoch seconds of its stored
    trades in the window. Returns counts:
      near_stored     a stored trade of the same market within NEAR_SEC: the
                      market was being followed, so the trade was dropped
                      (the case that would mean a real ingestion fault)
      not_followed    no stored trade within NEAR_SEC: the market was outside
                      the websocket and backfill selections at the time
                      (selection rotation, late entry)
      never_stored    the market has no stored trade in the window at all
    (2026-11-07, Mac data for market 562802: every trade stored while it was on
    the websocket, then a block of misses after it left the top 20.)
    """
    import bisect
    out = {"near_stored": 0, "not_followed": 0, "never_stored": 0}
    for e in new:
        ts_list = stored_ts.get(e["market_id"])
        if not ts_list:
            out["never_stored"] += 1
            continue
        t = int(datetime.fromisoformat(e["ts"]).timestamp())
        i = bisect.bisect_left(ts_list, t - NEAR_SEC)
        near = i < len(ts_list) and ts_list[i] <= t + NEAR_SEC
        out["near_stored" if near else "not_followed"] += 1
    return out


TIMELINE_BLOCK_SEC = 600


def timeline(missing_ts, stored_ts, block=TIMELINE_BLOCK_SEC):
    """Per 10-minute block with any trade: (block start epoch, stored, missing).
    A block with many of both means trades were dropped while the market was
    followed; blocks of only missing mean it wasn't followed then."""
    rows = collections.defaultdict(lambda: [0, 0])
    for t in stored_ts:
        rows[t - t % block][0] += 1
    for t in missing_ts:
        rows[t - t % block][1] += 1
    return [(b, st, mi) for b, (st, mi) in sorted(rows.items())]


async def stored_times(conn, market_ids, start, end):
    """market_id -> sorted epoch seconds of its stored trades in the window (read-only)."""
    rows = await conn.fetch(
        """SELECT market_id, array_agg(extract(epoch FROM ts)::bigint ORDER BY ts) AS t FROM trades
           WHERE market_id = ANY($1::text[]) AND ts >= $2 AND ts <= $3 GROUP BY 1""",
        list(market_ids), start, end)
    return {r["market_id"]: list(r["t"]) for r in rows}


def per_day(items):
    c = collections.Counter(str(e["ts"])[:10] for e in items)
    return dict(sorted(c.items()))


async def stored_hashes(conn, hashes):
    """Which of these tx_hashes are already in trades (uses its unique index)."""
    hashes, found = list(hashes), set()
    for i in range(0, len(hashes), LOOKUP_BATCH):
        rows = await conn.fetch("SELECT tx_hash FROM trades WHERE tx_hash = ANY($1::text[])",
                                hashes[i:i + LOOKUP_BATCH])
        found.update(r["tx_hash"] for r in rows)
    return found


async def insert_new(conn, items):
    """Insert through the live REST_QUERY, INSERT_BATCH rows per transaction."""
    q = w.EliteZleekWriter(db_pool=None, redis_client=None)
    for i in range(0, len(items), INSERT_BATCH):
        rows = [q._map_rest(e) for e in items[i:i + INSERT_BATCH]]
        async with conn.transaction():
            await conn.executemany(q.REST_QUERY, rows)


def report(name, fetched, unmapped, markets, new, already, infos):
    print(f"\n== {name}: {fetched} trades fetched from {len(markets)} markets with trades; "
          f"{already} already stored, {len(new)} NEW; {unmapped} unmapped (token not in markets)")
    for b in infos:
        print(f"   batch {b['batch']:>2} ({b['markets']} markets): {b['trades']:>6} trades in window, "
              f"{b['pages'] if b['pages'] is not None else '?'} pages, oldest reached {fmt_unix(b['oldest'])}: {b['status']}")
    bad = [b for b in infos if not b["complete"]]
    if bad:
        print(f"   !! {len(bad)} INCOMPLETE batch(es): {[b['batch'] for b in bad]}. Their trades are missing "
              f"from these counts, not absent; run again to complete them.")
    for day, n in per_day(new).items():
        print(f"   new on {day}: {n}")


async def main(args):
    start, end = parse_utc(args.start), parse_utc(args.end)
    start_unix, end_unix = int(start.timestamp()), int(end.timestamp())
    settings = {} if args.apply else {"default_transaction_read_only": "on"}
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=2,
                                     server_settings=settings)
    try:
        asset_maps = await w.load_asset_maps(pool)
        ws_assets = await w.load_asset_ids(pool)
        backfill_conds = await w.load_condition_ids_for_backfill(pool, exclude_asset_ids=ws_assets)
        async with pool.acquire() as conn:
            ws_conds = [r["condition_id"] for r in await conn.fetch(
                """SELECT DISTINCT condition_id FROM markets
                   WHERE condition_id IS NOT NULL
                     AND (token_yes_id = ANY($1::text[]) OR token_no_id = ANY($1::text[]))""", ws_assets)]
            ended = await conn.fetchval(
                """SELECT count(*) FROM markets
                   WHERE status <> 'open' AND liquidity_usd >= 10000
                     AND updated_at >= $1""", start)
        print(f"{'APPLY' if args.apply else 'DRY RUN (read-only)'}: window {start:%Y-%m-%d %H:%M} to "
              f"{end:%Y-%m-%d %H:%M} UTC; backfill markets {len(backfill_conds)}, websocket markets {len(ws_conds)}; "
              f"{ended} markets with >= $10k liquidity closed since the window began (not covered)")

        groups = {}
        async with httpx.AsyncClient(base_url=w.DATA_API_URL, timeout=REQUEST_TIMEOUT_SEC) as client:
            for name, conds in (("backfill", backfill_conds), ("websocket", ws_conds)):
                groups[name] = await fetch_group(client, conds, start_unix, end_unix, asset_maps)
        print(f"Data API: {Stats.requests} requests, {Stats.retries} retries, {Stats.slow} slower than "
              f"{SLOW_REQUEST_SEC}s (slowest {Stats.slowest:.1f}s)")

        async with pool.acquire() as conn:
            all_hashes = {e["tx_hash"] for g in groups.values() for e in g[2] if e.get("tx_hash")}
            existing = await stored_hashes(conn, all_hashes)

        to_write, seen = [], set()
        incomplete = 0
        for name, (fetched, unmapped, items, markets, infos) in groups.items():
            new, already = split_new(items, existing)
            report(name, fetched, unmapped, markets, new, already, infos)
            if args.diagnose and new:
                async with pool.acquire() as conn:
                    times = await stored_times(conn, {e["market_id"] for e in new}, start, end)
                print(f"   missing trades: {classify_missing(new, times)}  (near_stored = dropped while followed)")
                by_market = collections.Counter(e["market_id"] for e in new)
                for m, n in by_market.most_common(args.diagnose):
                    mc = classify_missing([e for e in new if e["market_id"] == m], times)
                    print(f"     {m:>10}: {n:>6} missing ({mc['near_stored']} near a stored trade), "
                          f"{len(times.get(m, []))} stored in window")
                    miss_ts = [int(datetime.fromisoformat(e["ts"]).timestamp()) for e in new if e["market_id"] == m]
                    cells = [f"{datetime.fromtimestamp(b, timezone.utc):%H:%M} {st}/{mi}"
                             for b, st, mi in timeline(miss_ts, times.get(m, []))]
                    print("        stored/missing per 10 min: " + "  ".join(cells))
            if name == "backfill" or args.include_websocket_markets:
                incomplete += sum(1 for b in infos if not b["complete"])
            if name == "backfill" or args.include_websocket_markets:
                for e in new:
                    if e["tx_hash"] not in seen:
                        seen.add(e["tx_hash"])
                        to_write.append(e)

        if to_write:
            print("\nsample of new rows (ts, market, YES price, side, USD):")
            for e in sorted(to_write, key=lambda e: e["ts"])[::max(1, len(to_write) // args.sample)][:args.sample]:
                print(f"   {e['ts'][:19]}  {e['market_id']:>10}  {e['price']:.4f}  {e['side']:<4}  {e['size_usd']:.2f}")

        groups_written = "backfill" + (" + websocket" if args.include_websocket_markets else "")
        print(f"\nincomplete batches in the groups to write: {incomplete}")
        if not args.apply:
            print(f"\nDRY RUN: nothing written. --apply would insert {len(to_write)} trades ({groups_written}) "
                  f"and rebuild bars_1m for the window.")
            return
        async with pool.acquire() as conn:
            before = len(await stored_hashes(conn, [e["tx_hash"] for e in to_write]))
            await insert_new(conn, to_write)
            after = len(await stored_hashes(conn, [e["tx_hash"] for e in to_write]))
            print(f"\nAPPLIED: {after - before} trades inserted ({groups_written}); "
                  f"{len(to_write) - (after - before)} were stored meanwhile by the running worker")
            for day, n in per_day(to_write).items():
                print(f"   recovered on {day}: {n}")
            if incomplete:
                print(f"   !! {incomplete} batch(es) were incomplete: run --apply again to complete them")
            await w_rebuild_bars(conn, start, end)
            print(f"bars_1m rebuilt for {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M} UTC")
    finally:
        await pool.close()


async def w_rebuild_bars(conn, start, end):
    from bars_jobs import rebuild_bars   # sibling module, imported only when writing
    await rebuild_bars(conn, start, end)


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run, read-only)")
    ap.add_argument("--include-websocket-markets", action="store_true",
                    help="with --apply, also write the websocket group's new trades")
    ap.add_argument("--sample", type=int, default=10)
    ap.add_argument("--diagnose", type=int, default=0, metavar="N",
                    help="read-only: split missing trades into near a stored trade of the same market "
                         "(dropped while followed) / not followed / never stored, and list the N "
                         "markets with the most missing")
    return ap.parse_args(argv)


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
