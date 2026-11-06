"""
Tests for the startup gap backfill (ROADMAP phase 2, item 1).

Read-only: uses the live Data API v2 and the database through a read-only
session (DATABASE_URL must be a read-only role, e.g. zleek_web). Nothing is
written: the writer is a fake, and the bars rebuild is captured, not run.
Live markets come from v2's global trade feed (not the database), so this
runs against any database that has a markets table. The offline v2 client
tests are in test_data_api_v2.py.

    cd apps/worker && DATABASE_URL=... .venv/bin/python tests/test_gap_backfill.py
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import asyncpg
import httpx

import bars_jobs
import writer as w

UTC = timezone.utc


def test_compute_gap_window():
    now = datetime(2026, 10, 31, 12, 0, tzinfo=UTC)
    # fresh database: nothing to fill
    assert w.compute_gap_window(None, now) is None
    # 2-hour outage: start 5 minutes before the newest stored trade, not capped
    start, capped = w.compute_gap_window(now - timedelta(hours=2), now)
    assert start == now - timedelta(hours=2, minutes=5) and not capped
    # 5-day outage: capped at 48 hours
    start, capped = w.compute_gap_window(now - timedelta(days=5), now)
    assert start == now - timedelta(hours=48) and capped
    # just inside the cap is not capped
    start, capped = w.compute_gap_window(now - timedelta(hours=47), now)
    assert not capped
    print("ok  compute_gap_window: fresh DB, short gap, capped gap, edge")


async def test_v2_paging_live(conds):
    """Live, read-only: v2 cursor paging returns the same trades whatever the
    page size, has no duplicates, stays inside the window, and a batch of
    markets in one request returns the union of the markets fetched alone.
    Comparisons stop SETTLE_SEC before the window end: the Data API shows a
    trade only some time after its timestamp (observed up to ~80 s on
    2026-11-06), so calls a few seconds apart can differ in the newest
    minute. The periodic backfill re-reads the last hour every 5 minutes, so
    late-appearing trades are picked up on the next run."""
    SETTLE_SEC = 300
    now = datetime.now(UTC)
    start_unix, end_unix = int((now - timedelta(hours=1)).timestamp()), int(now.timestamp())
    key = w._trade_key
    async with httpx.AsyncClient(base_url=w.DATA_API_URL, timeout=60.0) as client:
        small = await w.fetch_public_trades(client, conds[0], start_unix, end_unix=end_unix, page_size=50)
        big = await w.fetch_public_trades(client, conds[0], start_unix, end_unix=end_unix, page_size=1000)
        assert small, f"{conds[0]}: no trades in the last hour; pick a busier market"
        cut = end_unix - SETTLE_SEC
        assert sorted(key(t) for t in small if t["timestamp"] <= cut) == \
               sorted(key(t) for t in big if t["timestamp"] <= cut), \
            f"{conds[0]}: {len(small)} vs {len(big)}"
        assert len({key(t) for t in big}) == len(big), "duplicate trades"
        assert all(start_unix <= t["timestamp"] <= end_unix for t in big)
        print(f"ok  v2 paging: {conds[0][:12]}... {len(small)} trades in 1h, pages of 50 == pages of 1000, "
              f"no duplicates, all inside the window")

        alone = []
        for c in conds[:3]:
            alone += await w.fetch_public_trades(client, c, start_unix, end_unix=end_unix)
        together = await w.fetch_public_trades(client, conds[:3], start_unix, end_unix=end_unix)
        a = {key(t) for t in alone if t["timestamp"] <= cut}
        b = {key(t) for t in together if t["timestamp"] <= cut}
        assert a == b, f"batch of 3 differs from one at a time: {len(a)} vs {len(b)}"
        late = len({key(t) for t in together} - {key(t) for t in alone})
        assert {key(t) for t in alone} <= {key(t) for t in together}, "batch lost trades a single fetch found"
        print(f"ok  v2 batching: 3 markets in one request == one at a time ({len(b)} settled trades; "
              f"{late} newer trades seen only by the later batched call)")


class FakeRedis:
    async def llen(self, key):
        return 0  # the queue is already drained


class FakeWriter:
    REDIS_REST = "buffer:trades:rest"
    flush_interval_sec = 0

    def __init__(self):
        self.redis = FakeRedis()
        self.items = []

    async def add_item(self, item):
        self.items.append(item)


async def test_run_gap_backfill(pool, conds, ws_conds):
    """Full run, read-only: queues trades for the window (websocket markets
    included), then asks for a bars rebuild of exactly that window."""
    seen = {}

    async def three_markets(pool, **kw):
        seen["kwargs"] = kw
        return conds + ws_conds

    rebuilt = []

    async def capture_rebuild(conn, start, end):
        rebuilt.append((start, end))

    orig_load, orig_rebuild = w.load_condition_ids_for_backfill, bars_jobs.rebuild_bars
    w.load_condition_ids_for_backfill, bars_jobs.rebuild_bars = three_markets, capture_rebuild
    try:
        fake = FakeWriter()
        newest = datetime.now(UTC) - timedelta(hours=2)
        await w.run_gap_backfill(pool, fake, newest)
    finally:
        w.load_condition_ids_for_backfill, bars_jobs.rebuild_bars = orig_load, orig_rebuild

    assert seen["kwargs"] == {}, f"gap backfill must not exclude websocket markets: {seen['kwargs']}"
    assert len(rebuilt) == 1 and rebuilt[0][0] == newest - w.GAP_OVERLAP, rebuilt
    lo = newest - w.GAP_OVERLAP
    ts = [datetime.fromisoformat(i["ts"]) for i in fake.items]
    assert ts and min(ts) >= lo - timedelta(seconds=1), "queued a trade from before the window"
    assert all(i["tx_hash"] for i in fake.items), "every queued trade must carry tx_hash (dedupe)"
    markets = {i["market_id"] for i in fake.items}
    print(f"ok  run_gap_backfill: {len(fake.items)} trades queued from {len(markets)} markets "
          f"(incl. websocket markets), all with tx_hash, oldest {min(ts):%H:%M}Z >= window start "
          f"{lo:%H:%M}Z; bars rebuild requested for exactly [{lo:%H:%M}Z, now)")

    # capped run: a 5-day-old newest trade only fetches the last 48 hours
    w.load_condition_ids_for_backfill, bars_jobs.rebuild_bars = (lambda pool, **kw: _one(conds)), capture_rebuild
    try:
        fake, rebuilt[:] = FakeWriter(), []
        await w.run_gap_backfill(pool, fake, datetime.now(UTC) - timedelta(days=5))
    finally:
        w.load_condition_ids_for_backfill, bars_jobs.rebuild_bars = orig_load, orig_rebuild
    cap_start = rebuilt[0][0]
    assert abs((datetime.now(UTC) - cap_start) - w.GAP_BACKFILL_CAP) < timedelta(minutes=2)
    print(f"ok  capped run: window starts {datetime.now(UTC) - cap_start} ago (cap {w.GAP_BACKFILL_CAP})")


async def _one(conds):
    return conds[:1]


async def live_conditions(pool, n=4):
    """Markets trading right now, from v2's global feed, that are also in the
    markets table (so queue_public_trades can map their tokens)."""
    async with httpx.AsyncClient(base_url=w.DATA_API_URL, timeout=60.0) as client:
        feed = (await client.get("/v2/trades", params={"limit": 1000})).json()["data"]
    counts = {}
    for t in feed:
        counts[t["condition_id"]] = counts.get(t["condition_id"], 0) + 1
    busiest = sorted(counts, key=counts.get, reverse=True)
    async with pool.acquire() as conn:
        known = {r["condition_id"] for r in await conn.fetch(
            "SELECT condition_id FROM markets WHERE condition_id = ANY($1::text[])", busiest)}
    return [c for c in busiest if c in known][:n]


async def main():
    test_compute_gap_window()
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=2,
                                     server_settings={"default_transaction_read_only": "on"})
    try:
        conds = await live_conditions(pool)
        assert len(conds) >= 3, f"need 3 live markets that are also in the markets table, got {conds}"
        await test_v2_paging_live(conds)
        await test_run_gap_backfill(pool, conds[:2], conds[2:3])
    finally:
        await pool.close()
    print("all gap backfill tests passed")


if __name__ == "__main__":
    asyncio.run(main())
