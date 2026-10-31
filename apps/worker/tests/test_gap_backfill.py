"""
Tests for the startup gap backfill (ROADMAP phase 2, item 1).

Read-only: uses the live Data API and the database through a read-only
session (DATABASE_URL must be a read-only role, e.g. zleek_web). Nothing is
written: the writer is a fake, and the bars rebuild is captured, not run.

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


async def test_time_cursor_paging(normal_conds, busy_cond):
    """fetch_public_trades pages by moving `end` back (offset stays 0)."""
    now = datetime.now(UTC)
    start_unix, end_unix = int((now - timedelta(hours=3)).timestamp()), int(now.timestamp())
    key = w._trade_key
    async with httpx.AsyncClient(base_url=w.DATA_API_URL, timeout=60.0) as client:
        # (a) markets under the 10,000 cap: 100-per-page cursor == one big request
        for cond in normal_conds:
            paged = await w.fetch_public_trades(client, cond, start_unix, end_unix=end_unix, page_size=100)
            one = (await client.get("/trades", params={"market": cond, "limit": 10000, "offset": 0,
                                                       "start": start_unix, "end": end_unix})).json()
            await asyncio.sleep(1)
            assert len(one) < 10000, "pick a quieter market for this check"
            assert sorted(map(key, paged)) == sorted(map(key, one)), f"{cond}: {len(paged)} vs {len(one)}"
            print(f"ok  cursor paging == single request: {cond[:12]}... {len(paged)} trades, "
                  f"{max(1, -(-len(paged) // 100))} page(s) of 100")
        # (b) the busiest market: more than the 10,000 offset cap, no duplicates, reaches the window start
        busy = await w.fetch_public_trades(client, busy_cond, start_unix, end_unix=end_unix, page_size=1000)
        assert len({key(t) for t in busy}) == len(busy), "duplicate trades"
        assert all(start_unix <= t["timestamp"] <= end_unix for t in busy)
        oldest = min(t["timestamp"] for t in busy)
        print(f"ok  busiest market {busy_cond[:12]}...: {len(busy)} trades in 3h "
              f"({'over' if len(busy) > 10000 else 'under'} the 10,000 offset cap), no duplicates, "
              f"oldest {oldest - start_unix}s after the window start")


async def test_stuck_second_terminates():
    """(c) a full page inside one second must not loop forever."""
    class FakeResp:
        status_code = 200
        def __init__(self, data): self._d = data
        def raise_for_status(self): pass
        def json(self): return self._d
    class FakeClient:
        calls = 0
        async def get(self, path, params):
            FakeClient.calls += 1
            end = params["end"]
            if end >= 1000:  # every page: 5 trades, all at second 1000
                return FakeResp([{"transactionHash": f"0x{i}", "asset": "a", "timestamp": 1000, "size": 1}
                                 for i in range(5)])
            return FakeResp([{"transactionHash": "0xold", "asset": "a", "timestamp": 990, "size": 1}])
    orig = w.BACKFILL_REQUEST_GAP_SEC
    w.BACKFILL_REQUEST_GAP_SEC = 0
    try:
        got = await w.fetch_public_trades(FakeClient(), "c", 900, end_unix=1000, page_size=5)
    finally:
        w.BACKFILL_REQUEST_GAP_SEC = orig
    assert FakeClient.calls == 2 and {t["timestamp"] for t in got} == {1000, 990}, (FakeClient.calls, got)
    print("ok  full page inside one second: steps past it and terminates (2 requests)")


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


async def main():
    test_compute_gap_window()
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=2,
                                     server_settings={"default_transaction_read_only": "on"})
    try:
        ws_assets = await w.load_asset_ids(pool)
        async with pool.acquire() as conn:
            ws_conds = [r["condition_id"] for r in await conn.fetch(
                "SELECT condition_id FROM markets WHERE token_yes_id = ANY($1::text[]) LIMIT 1", ws_assets)]
        conds = (await w.load_condition_ids_for_backfill(pool, exclude_asset_ids=ws_assets, limit=2))
        async with pool.acquire() as conn:  # busiest markets, so paging spans several pages
            busy = [r["condition_id"] for r in await conn.fetch("""
                SELECT m.condition_id FROM trades t JOIN markets m ON m.id = t.market_id
                WHERE t.ts > now() - interval '3 hours' GROUP BY 1 ORDER BY count(*) DESC LIMIT 2""")]
        await test_stuck_second_terminates()
        await test_time_cursor_paging(conds + ws_conds, busy[0])
        await test_run_gap_backfill(pool, conds, ws_conds)
    finally:
        await pool.close()
    print("all gap backfill tests passed")


if __name__ == "__main__":
    asyncio.run(main())
