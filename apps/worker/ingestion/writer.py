from dotenv import load_dotenv
import asyncpg, asyncio
import logging
import os
import json
import gamma_client
from py_clob_client.clob_types import TradeParams
from clob_client import get_client
from datetime import datetime, timezone,timedelta
from collections import deque
import statistics


logger = logging.getLogger(__name__)


clob = get_client()
load_dotenv(".env.local")
uri = os.getenv("DATABASE_URL")
MIN_LIQUIDITY_USD = 10000
MAX_MARKETS = 20
BATCH=5000
SNAPSHOT_THROTTLE_SEC=5
write_lags = deque(maxlen=200)
write_lag_counter = 0


def coerce_dt(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


async def upsert_markets(pool,data):
    logger.info("Upset Markets started")
    query = """
    insert into markets(id, title, event_id, condition_id, event_title, category, status, liquidity_usd,
                        token_yes_id, token_no_id, created_at, updated_at, raw)
    values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
    on conflict (id) do update
      set title=$2, event_id=$3, condition_id=$4, event_title=$5, category=$6, status=$7,
          liquidity_usd=$8, token_yes_id=$9, token_no_id=$10, created_at=$11,
          updated_at=$12, raw=$13;
    """

    rows = [
        (
            m.get("id"),
            m.get("title"),
            m.get("event_id"),
            m.get("condition_id"),
            m.get("event_title"),
            m.get("category"),
            m.get("status", "open"),
            m.get("liquidity_usd", 0),
            m.get("token_yes_id"),
            m.get("token_no_id"),
            m.get("created_at"),
            m.get("updated_at"),
            m.get("raw"),
        )
        for m in data
    ]
    if not rows:
        logger.info("No markets to upsert")
        return
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, rows)

async def insert_trades_and_books(pool, queue: asyncio.Queue):
    # Query for REST API trades (has tx_hash, id, etc.)
    rest_trades_query = """
    insert into trades(id, market_id, asset_id, ts, price, size_usd, side, 
                       maker_wallet, taker_wallet, tx_hash, raw, status, 
                       market_order_id, match_time)
    values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
    on conflict (tx_hash) do update
      set id=excluded.id,
          market_id=excluded.market_id,
          asset_id=excluded.asset_id,
          ts=excluded.ts,
          price=excluded.price,
          size_usd=excluded.size_usd,
          side=excluded.side,
          maker_wallet=excluded.maker_wallet,
          taker_wallet=excluded.taker_wallet,
          raw=excluded.raw,
          status=excluded.status,
          market_order_id=excluded.market_order_id,
          match_time=excluded.match_time
    """
    
    # Query for WebSocket trades (simpler, no unique IDs)
    ws_trades_query = """
    insert into trades(market_id, asset_id, ts, price, size_usd, side, raw)
    values ($1,$2,$3,$4,$5,$6,$7)
    """
    
    ob_snapshot_query = """
    insert into ob_snapshots(market_id, ts, best_ask, best_bid, bid_depth_usd, ask_depth_usd,
                        bids, asks)
    values ($1,$2,$3,$4,$5,$6,$7,$8)
    on conflict (market_id,ts) do update
      set market_id=$1, ts=$2, best_ask=$3, best_bid=$4, bid_depth_usd=$5,
          ask_depth_usd=$6, bids=$7, asks=$8
    """
    
    last_snapshot = {}
    
    global write_lag_counter
    while True:
        batch = []
        
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
            batch.append(item)
        except asyncio.TimeoutError:
            pass
            
        while len(batch) < BATCH:
            try:
                batch.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
                
        if not batch:
            continue
        
        rest_trades = []  # From REST API backfill
        ws_trades = []    # From WebSocket
        snaps = []
        now = datetime.now(timezone.utc)
        
        try:   
            for e in batch:
                kind = e.get("kind")
                ingest_dt = coerce_dt(e.get("ingest_ts"))
                if ingest_dt:
                    write_lag_ms = (now - ingest_dt).total_seconds() * 1000
                    write_lags.append(write_lag_ms)
                    write_lag_counter += 1
                    if write_lag_counter % 200 == 0 and write_lags:
                        p50 = statistics.median(write_lags)
                        p95 = sorted(write_lags)[int(0.95 * len(write_lags))]
                        logger.info("write lag p50=%.0fms p95=%.0fms", p50, p95)
                
                if kind == "trade":
                    # Detect source: REST API trades have tx_hash, WS trades don't
                    if e.get("tx_hash"):
                        # REST API trade
                        row = (
                            e.get("id", ""),
                            e.get("market_id", ""),
                            e.get("asset_id", ""),
                            e.get("ts", ""),
                            e.get("price", 0),
                            e.get("size_usd", 0),
                            e.get("side", ""),
                            e.get("maker_wallet", ""),
                            e.get("taker_wallet", ""),
                            e.get("tx_hash", ""),
                            e.get("raw", ""),
                            e.get("status", ""),
                            e.get("market_order_id", ""),
                            e.get("match_time", "")
                        )
                        rest_trades.append(row)
                    else:
                        # WebSocket trade
                        row = (
                            e.get("market_id", ""),
                            e.get("asset_id", ""),
                            e.get("ts", ""),
                            e.get("price", 0),
                            e.get("size_usd", 0),
                            e.get("side", ""),
                            e.get("raw", ""),
                        )
                        ws_trades.append(row)
                
                elif kind == "book":
                    key = e["market_id"]
                    ts_prev = last_snapshot.get(key)
                    
                    row = (
                        e.get("market_id", ""),
                        e.get("ts", ""),
                        e.get("best_ask", 0.0),
                        e.get("best_bid", 0.0),
                        e.get("bid_depth_usd", 0.0),
                        e.get("ask_depth_usd", 0.0),
                        e.get("bids", []),
                        e.get("asks", []),
                    )
                    
                    if not ts_prev or (now-ts_prev).total_seconds() >= SNAPSHOT_THROTTLE_SEC:
                        snaps.append(row)
                        last_snapshot[key] = now
            
            async with pool.acquire() as conn:
                if rest_trades:
                    async with conn.transaction():
                        await conn.executemany(rest_trades_query, rest_trades)
                        logger.info("Stored %d REST API trades", len(rest_trades))
                
                if ws_trades:
                    async with conn.transaction():
                        await conn.executemany(ws_trades_query, ws_trades)
                        logger.info("Stored %d WebSocket trades", len(ws_trades))
                
                if snaps:
                    async with conn.transaction():
                        await conn.executemany(ob_snapshot_query, snaps)
                        logger.info("Stored %d book snapshots", len(snaps))
        
        except Exception as e:
            logger.error("DB write error: %s", e, exc_info=True)
        finally:
            for _ in batch:
                queue.task_done()

async def load_asset_ids(pool):
    logger.info("Load asset IDs started")
    query = """
              select token_yes_id, token_no_id
            from markets
            where status = 'open'
              and liquidity_usd >= $1
            order by liquidity_usd desc
            limit $2
        """
    logger.debug("Fetching asset IDs")
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(query, MIN_LIQUIDITY_USD, MAX_MARKETS)

    asset_ids = set()
    for r in rows:
        if r["token_yes_id"]:
            asset_ids.add(r["token_yes_id"])
        if r["token_no_id"]:
            asset_ids.add(r["token_no_id"])
            
    list_ids = list(asset_ids)
    logger.info("Returning %d assets , im requiring a minimum liquidity of %f",len(list_ids), MIN_LIQUIDITY_USD)
    return list_ids

    # load mapping once at startup


async def load_asset_maps(pool):
    logger.info("Load asset maps started")
    query = """
        select id as market_id, token_yes_id, token_no_id
        from markets
    """
    logger.debug("Fetching asset maps")
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(query)

    m = {}
    for r in rows:
        if r["token_yes_id"]:
            m[r["token_yes_id"]] = (r["market_id"], "YES")
        if r["token_no_id"]:
            m[r["token_no_id"]] = (r["market_id"], "NO")
    logger.info("Returning asset %d IDs",len(m))
    return m

def parse_iso_z(s: str) -> datetime:
    # match_time / last_update are like "2023-11-07T05:31:56Z"
    return datetime.fromisoformat(s.replace("Z", "+00:00"))
async def load_condition_ids_for_backfill(pool):
    logger.info("Load condition IDs started")
    query = """
        select distinct condition_id, liquidity_usd
        from markets
        where status = 'open'
          and liquidity_usd >= $1
          and condition_id is not null
        order by liquidity_usd desc
        limit $2
    """
    logger.debug("Fetching condition IDs")
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(query, MIN_LIQUIDITY_USD, MAX_MARKETS)

    condition_ids = [r["condition_id"] for r in rows]
    logger.info("Returning %d condition IDs for backfill", len(condition_ids))
    return condition_ids

async def run_backfill(pool, queue, hours_back=1):
    """
    Backfill trades from the last `hours_back` hours.
    This function should be called periodically by a wrapper.
    """
    t_now = datetime.now(timezone.utc)
    t_start = t_now - timedelta(hours=hours_back)  # FIX: Add timedelta()
    cutoff_unix = int(t_start.timestamp())
    
    condition_ids = await load_condition_ids_for_backfill(pool)
    if not condition_ids:
        logger.info("No condition_ids available for backfill")
        return
    
    total_trades = 0
    
    # FIX: Process each condition properly
    for cond in condition_ids:
        logger.info("Backfilling trades for condition_id=%s", cond)
        
        try:
            resp = clob.get_trades(
                TradeParams(
                    market=cond,
                    after=str(cutoff_unix)
                )
            )
            
            # FIX: This loop must be INSIDE the condition loop
            for trade in resp:
                mt = parse_iso_z(trade["match_time"])
                if mt < t_start:
                    continue
                
                norm = {
                    "kind": "trade",
                    "id": trade.get("id"),
                    "market_id": trade.get("market_id"),
                    "price": float(trade.get("price", 0)),
                    "ts": mt,  # FIX: Keep as datetime object, not string
                    "asset_id": trade.get("asset_id"),
                    "match_time": mt,        
                    "market_order_id": trade.get("market_order_id"),
                    "size_usd": float(trade.get("size", 0)),
                    "side": trade.get("side"),
                    "maker_wallet": trade.get("maker_address"),
                    "taker_wallet": None,
                    "tx_hash": trade.get("transaction_hash"),
                    "status": trade.get("status"),
                    "bucket_index": trade.get("bucket_index"),
                    "raw": json.dumps(trade)  # FIX: Convert to JSON string
                }
                
                # Enqueue immediately
                try:
                    queue.put_nowait(norm)
                    total_trades += 1
                except asyncio.QueueFull:
                    _ = queue.get_nowait()
                    queue.put_nowait(norm)
                    
        except Exception as e:
            logger.error("Error backfilling condition %s: %s", cond, e, exc_info=True)
            continue
    
    logger.info("Backfill completed: %d trades from %d conditions", 
                total_trades, len(condition_ids))
    
    if total_trades > 0:
        logger.info("✅ Recent trades exist!")
    else:
        logger.warning("⚠️ No trades in last %d hour(s) for selected markets", hours_back)
        
async def run_backfill_loop(pool, queue, interval=3600, hours_back=1):
    """
    Periodically run backfill to catch any missed trades.
    Runs every `interval` seconds.
    
    Note: This does NOT run an initial backfill - that should be done
    in main() before starting this task.
    """
    logger.info("Starting backfill loop every %s seconds (looking back %s hours)", 
                interval, hours_back)
    
    while True:
        await asyncio.sleep(interval)
        
        try:
            logger.info("Running periodic backfill (last %s hour(s))...", hours_back)
            await run_backfill(pool, queue, hours_back=hours_back)
            logger.info("Periodic backfill completed")
        except Exception as e:
            logger.exception("Periodic backfill failed: %s", e)    

     
async def main():


    pool = await asyncpg.create_pool(uri, min_size=1, max_size=5)
    async with pool:
        markets_raw = gamma_client.fetch_markets()      # or await if async
        markets = gamma_client.parse_json(markets_raw)

        await upsert_markets(pool, markets)

        asset_ids = await load_asset_ids(pool)
        asset_map = await load_asset_maps(pool)

        # later: await insert_trades(pool, trade_events)
        #        await ob_snapshot(pool, book_events)

if __name__ == "__main__":
    asyncio.run(main())
