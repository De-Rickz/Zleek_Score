"""
End-to-end checks of the trades_archive step against a real TimescaleDB,
through the worker's own code: the writer's REST_QUERY, newest_stored_trade,
bars_jobs.rebuild_bars, the heartbeat's archive check, the recovery script's
lookups, and the web's newest-trade query. Needs ZLEEK_TEST_DATABASE_URL, a
throwaway database already set up by infra/sql/migrations/test_trades_archive.sh
(which runs this file at the end); skips without it. Never point it at a real
database: it writes.

    infra/sql/migrations/test_trades_archive.sh
"""
import asyncio
import importlib.util
import os
import sys
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, "..", "ingestion"))

import asyncpg

import bars_jobs
import writer as w


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


hb = load("heartbeat", os.path.join(HERE, "..", "scripts", "heartbeat.py"))
rec = load("recover", os.path.join(HERE, "..", "scripts", "recover_backfill_gap_2026-11.py"))

# The web's newest-trade lookup (apps/web/lib/queries.ts), for one market.
WEB_LAST_TRADE = """SELECT coalesce(
    (SELECT max(ts) FROM trades t WHERE t.market_id = $1),
    (SELECT max(ts) FROM trades_archive a WHERE a.market_id = $1))"""


def item(market, ts, tx, price=0.5):
    return {"market_id": market, "asset_id": "a", "ts": ts.isoformat(), "price": price, "size_usd": 3.0,
            "side": "BUY", "tx_hash": tx, "raw": {"proxy_wallet": "w"}}


async def main():
    url = os.environ.get("ZLEEK_TEST_DATABASE_URL")
    if not url:
        print("SKIPPED (needs a throwaway TimescaleDB: ZLEEK_TEST_DATABASE_URL; run by infra/sql/migrations/test_trades_archive.sh and the trades-archive CI job): all end-to-end checks")
        return
    pool = await asyncpg.create_pool(url, min_size=1, max_size=2)
    q = w.EliteZleekWriter(db_pool=None, redis_client=None)
    async with pool.acquire() as conn:
        cutoff = await conn.fetchval("SELECT archive_cutoff()")
        old = cutoff - timedelta(days=1, hours=-5)          # inside an archived (compressed) day
        await conn.execute("INSERT INTO markets(id, title) VALUES ('m3', 'archive only') ON CONFLICT DO NOTHING")

        # 1. the writer's REST path: recent -> trades, old -> archive, old again -> nothing
        before = await conn.fetchval("SELECT count(*) FROM trades_all")
        rows = [q._map_rest(item("m1", datetime.now(timezone.utc) - timedelta(minutes=3), "0xe2e-recent")),
                q._map_rest(item("m3", old, "0xe2e-old")),
                q._map_rest(item("m3", old - timedelta(seconds=4), "0xe2e-old"))]   # websocket's ts for it
        async with conn.transaction():
            await conn.executemany(q.REST_QUERY, rows)
        assert await conn.fetchval("SELECT count(*) FROM trades_all") == before + 2
        assert await conn.fetchval("SELECT count(*) FROM trades WHERE tx_hash = '0xe2e-recent'") == 1
        assert await conn.fetchval("SELECT count(*) FROM trades_archive WHERE tx_hash = '0xe2e-old'") == 1
        print("ok writer REST_QUERY: recent into trades, old into the archive, its re-write refused")

        # 2. the gap backfill's start reads both tables
        newest = await w.newest_stored_trade(pool)
        assert newest == await conn.fetchval("SELECT max(ts) FROM trades_all")
        print("ok newest_stored_trade covers both tables")

        # 3. bars rebuilt for an archived window come from the archive
        day = cutoff - timedelta(days=3)    # a compressed archive day with exactly the seeded trades
        await bars_jobs.rebuild_bars(conn, day, day + timedelta(days=1))
        n = await conn.fetchval("SELECT count(*) FROM bars_1m WHERE bucket >= $1 AND bucket < $2", day,
                                day + timedelta(days=1))
        assert n == 48, n          # 2 markets x 24 hourly trades, one bar each
        print("ok rebuild_bars reads archived trades (48 bars)")

        # 4. the web's newest trade for a market whose trades are all archived
        assert await conn.fetchval(WEB_LAST_TRADE, "m3") == old
        assert await conn.fetchval(WEB_LAST_TRADE, "m1") == await conn.fetchval(
            "SELECT max(ts) FROM trades WHERE market_id = 'm1'")
        print("ok web newest-trade query: archive-only market and recent market")

        # 5. heartbeat archive check: healthy now, and catches a duplicate
        state = await hb.archive_state(conn)
        problems, part = hb.archive_problems(state, datetime.now(timezone.utc))
        assert problems == [] and state[1] == 0 and state[2] == 0, (problems, state)
        await conn.execute(
            """INSERT INTO trades_archive SELECT id + 1000000, market_id, asset_id, ts, price, size_usd, side,
                      maker_wallet, taker_wallet, tx_hash, raw, status, market_order_id, match_time
               FROM trades_archive WHERE ts >= archive_cutoff() - interval '1 day' LIMIT 1""")
        problems, _ = hb.archive_problems(await hb.archive_state(conn), datetime.now(timezone.utc))
        assert any("duplicate" in p for p in problems), problems
        await conn.execute("DELETE FROM trades_archive WHERE id >= 1000000")
        print(f"ok heartbeat archive check ({part}); a planted duplicate is reported")

        # 6. the recovery script's lookups see archived trades within its window
        rec.ARCHIVE = True
        found = await rec.stored_hashes(conn, ["0xe2e-old", "0xnope"], old - timedelta(hours=1), old + timedelta(hours=1))
        assert found == {"0xe2e-old"}, found
        times = await rec.stored_times(conn, {"m3"}, old - timedelta(hours=1), old + timedelta(hours=1))
        assert len(times.get("m3", [])) == 1, times
        print("ok recovery script: stored_hashes / stored_times read the archive")
    await pool.close()
    print("all end-to-end checks passed")


if __name__ == "__main__":
    asyncio.run(main())
