"""
Read-only check for the KNOWN LIMITATION documented above REST_QUERY in
ingestion/writer.py: if one on-chain transaction carries several fills, only
the first fill's row is kept in trades (tx_hash is UNIQUE).

A real multi-fill collision is one transaction_hash seen with DIFFERENT
(asset_id, size, price). The same values seen more than once is just a
redelivery or a backfill re-fetch, which is harmless and not reported.

Once a collision happens the losing fill is never stored in the DB (its
insert only overwrites price), so the evidence is checked in four places:

  1. worker.log: the live handler logs every last_trade_price event, so this
     is the complete record for the websocket path.
  2. DB, legacy live rows: rows from before transaction_hash was captured have
     no tx_hash but keep the full event in raw.
  3. DB, price drift: a collision with a different price leaves a row whose
     stored price no longer matches the price in its own raw. (A collision with
     the same price but a different size leaves no trace in the DB; checks 1
     and 4 cover that.)
  4. Data API sample: the backfill's source, for the busiest backfilled markets.

Never writes anything. Database access uses a read-only transaction.

    DATABASE_URL=<zleek_web url> .venv/bin/python scripts/check_multifill_collisions.py \
        [--log ingestion/worker.log] [--markets 5]

Exits 1 if any collision is found, 0 if clean.
"""
import argparse
import ast
import asyncio
import collections
import json
import os
import re
import sys
from decimal import Decimal

import asyncpg
import httpx

EVENT_RE = re.compile(r"(\{'market':.*'event_type': 'last_trade_price'.*\})\s*$")
# Live prices carry float noise (~1e-17), and the Data API reports the same fill's
# price to more decimals than the websocket (diffs up to ~1e-7). Real price
# differences are at least one tick (0.001).
PRICE_TOLERANCE = Decimal("1e-6")


def fill_key(asset_id, size, price):
    return (str(asset_id), Decimal(str(size)).normalize(), Decimal(str(price)).normalize())


def report(title, by_hash):
    collisions = {h: fills for h, fills in by_hash.items() if len(fills) > 1}
    status = "COLLISIONS FOUND" if collisions else "clean"
    print(f"\n[{status}] {title}: {len(by_hash)} hashes, {len(collisions)} with >1 distinct fill")
    for h, fills in list(collisions.items())[:10]:
        print(f"  {h}")
        for asset_id, size, price in sorted(fills):
            print(f"    asset ...{asset_id[-8:]}  size={size}  price={price}")
    return len(collisions)


def check_log(path):
    if not os.path.exists(path):
        print(f"\n[skipped] worker.log not found at {path}")
        return 0
    by_hash = collections.defaultdict(set)
    with open(path, errors="replace") as f:
        for line in f:
            if "last_trade_price" not in line:
                continue
            m = EVENT_RE.search(line)
            if not m:
                continue
            try:
                ev = ast.literal_eval(m.group(1))
            except (ValueError, SyntaxError):
                continue
            h = ev.get("transaction_hash")
            if h:
                by_hash[h].add(fill_key(ev.get("asset_id"), ev.get("size", 0), ev.get("price", 0)))
    return report("1. worker.log live events", by_hash)


async def check_db(conn):
    by_hash = collections.defaultdict(set)
    rows = await conn.fetch("""
        SELECT (raw #>> '{}')::jsonb AS ev FROM trades
        WHERE tx_hash IS NULL AND jsonb_typeof(raw) = 'string'""")
    for r in rows:
        ev = json.loads(r["ev"])  # asyncpg returns jsonb as text
        h = ev.get("transaction_hash")
        if h:
            by_hash[h].add(fill_key(ev.get("asset_id"), ev.get("size", 0), ev.get("price", 0)))
    found = report("2. DB legacy live rows (raw kept, no tx_hash)", by_hash)

    # Live rows store the event as a JSON string; backfill rows store the Data API object.
    drift = await conn.fetch("""
        SELECT tx_hash, market_id, ts, price,
               coalesce((raw #>> '{}')::jsonb ->> 'price', raw ->> 'price')::numeric AS raw_price
        FROM trades
        WHERE tx_hash IS NOT NULL
          AND abs(price - coalesce((raw #>> '{}')::jsonb ->> 'price', raw ->> 'price')::numeric) > $1
        LIMIT 50""", PRICE_TOLERANCE)
    status = "COLLISIONS FOUND" if drift else "clean"
    print(f"\n[{status}] 3. DB rows whose price was overwritten by a different fill: {len(drift)}")
    for r in drift[:10]:
        print(f"  {r['tx_hash']}  market {r['market_id']}  {r['ts']:%Y-%m-%d %H:%M}  "
              f"stored={r['price']:.6f}  own raw={r['raw_price']}")
    return found + len(drift)


async def check_data_api(conn, n_markets):
    conds = [r["condition_id"] for r in await conn.fetch("""
        SELECT m.condition_id FROM trades t JOIN markets m ON m.id = t.market_id
        WHERE t.tx_hash IS NOT NULL AND t.ts > now() - interval '1 day'
        GROUP BY 1 ORDER BY count(*) DESC LIMIT $1""", n_markets)]
    by_hash = collections.defaultdict(set)
    async with httpx.AsyncClient(base_url="https://data-api.polymarket.com", timeout=20) as client:
        for cid in conds:
            resp = await client.get("/trades", params={"market": cid, "limit": 500})
            if resp.status_code == 429:
                await asyncio.sleep(10)
                resp = await client.get("/trades", params={"market": cid, "limit": 500})
            resp.raise_for_status()
            for t in resp.json():
                by_hash[t["transactionHash"]].add(fill_key(t.get("asset"), t.get("size", 0), t.get("price", 0)))
            await asyncio.sleep(1)  # the Data API rate-limits quickly
    return report(f"4. Data API sample ({len(conds)} busiest backfilled markets, last 500 fills each)", by_hash)


async def main(args):
    found = check_log(args.log)
    conn = await asyncpg.connect(os.environ["DATABASE_URL"], server_settings={
        "default_transaction_read_only": "on", "statement_timeout": "120000"})
    try:
        found += await check_db(conn)
        found += await check_data_api(conn, args.markets)
    finally:
        await conn.close()
    print("\nRESULT:", "multi-fill collisions found, see above" if found else "no multi-fill collisions")
    return 1 if found else 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--log", default=os.path.join(os.path.dirname(__file__), "..", "ingestion", "worker.log"))
    p.add_argument("--markets", type=int, default=5)
    sys.exit(asyncio.run(main(p.parse_args())))
