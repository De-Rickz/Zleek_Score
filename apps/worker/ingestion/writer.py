from dotenv import load_dotenv
import asyncpg, asyncio
import logging
import os
import json
import gamma_client
from py_clob_client.clob_types import TradeParams
from clob_client import client
from datetime import datetime, timezone,timedelta


logger = logging.getLogger(__name__)

load_dotenv(".env.local")
uri = os.getenv("DATABASE_URL")
MIN_LIQUIDITY_USD = 20000
MAX_MARKETS = 10
BATCH=500
SNAPSHOT_THROTTLE_SEC=5


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
    trades_query = """
    insert into trades(id, market_id, asset_id, ts, price, size_usd, side, maker_wallet,
                        taker_wallet, tx_hash, raw, status, asset_id, market_order_id, match_time)
    values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
    on conflict (tx_hash) do update
      set id=$1, market_id=$2, ts=$3, price=$4, size_usd=$5, side=$6,
          maker_wallet=$7, taker_wallet=$8, tx_hash=$9, 
          raw=$10, status=$11, asset_id=$12, market_order_id=$13, match_time=$14 ;
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
    while True:
        batch = []
        # drain quickly up to BATCH
        
        
        try:
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
            batch.append(item)
        except asyncio.TimeoutError:
         
            pass
        while len(batch) < BATCH:
            try:
                batch.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                logger.debug("Queue empty while building batch")
                break
        if not batch:
            continue
        
        trades = []
        snaps = []
        now = datetime.now(timezone.utc)
        try:   
            
            for e in batch:
                logger.debug("Processing %s event", e["kind"])
                kind = e.get("kind")
                if kind == "trade":
                    logger.info("Writing trade %s", e.get("tx_hash"))
                    row = (
                            e.get("market_id",""),
                            e.get("asset_id",""),
                            e.get("ts", ""),
                            e.get("price", 0),
                            e.get("size_usd", 0),
                            e.get("side",""),
                            e.get("maker_wallet", ""),
                            e.get("taker_wallet",""),
                            e.get("tx_hash",""),
                            e.get("status",""),
                            e.get("raw",""),
                            e.get("bucket_index",-1),
                            e.get("match_time", ""),
                            e.get("id",""),
                            e.get("market_order_id","")
                        ) 
                    trades.append(row)
                    
            
                            

                elif kind == "book":
                    key = (e["market_id"])
                    #throttle snapshots per market
                    ts_prev = last_snapshot.get(key)
                    
                    logger.info("Writing book snapshot for %s", e.get("market_id"))
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
                else:
                    
                    pass
            async with pool.acquire() as conn:
                if trades:
                    async with conn.transaction():
                        await conn.executemany(trades_query, trades)
                        logger.info("Trades stored")    
                if snaps:
                    async with conn.transaction():
                        await conn.executemany(ob_snapshot_query, snaps)
                        logger.info("Books stored")    
                
                        # unknown kind: drop or log
                    pass
        except Exception as e:
            logger.error("DB write error: %s", e)
        finally:
            for _ in batch:
                queue.task_done()
                logger.info("Marked %d tasks done", len(batch))

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
    logger.info("Returning asset %d IDs",len(list_ids))
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
        select distinct condition_id
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

async def run_backfill(pool,queue):
    t_now = datetime.now(timezone.utc)
    t_start = t_now - timedelta(hours=1)
    cutoff_unix = int(t_start.timestamp())
    condition_ids = await load_condition_ids_for_backfill(pool)
    if not condition_ids:
        logger.info("No condition_ids available for backfill")
        return
    
    for cond in condition_ids:
        logger.info("Backfilling trades for condition_id=%s", cond) 
        resp = client.get_trades(
        TradeParams(
            market=cond,
            after=str(cutoff_unix)
        ),
    )
    backfill_norm_trades = []

    
    for trade in resp:
        mt = parse_iso_z(trade["match_time"])
        if mt < t_start:
            # defensive, although 'after' should already filter these out
            continue
        
        norm = {
                "kind": "trade",
                "id": trade.get("id"),
                "market_id": trade.get("market_id"),
                "price": float(trade.get("price", 0)),
                "ts": mt.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
                "asset_id": trade.get("asset_id"),
                "match_time": mt,        
                "market_order_id": trade.get("market_order_id"),
                "size_usd": float(trade.get("size",0)),
                "side": trade.get("side"),    # "buy"/"sell" for YES
                "maker_wallet": trade.get("maker_address"),
                "taker_wallet": None,
                "tx_hash": trade.get("transaction_hash"),
                "status": trade.get("status"),
                "bucket_index" : trade.get("bucket_index"),
                "raw": trade
        }
        backfill_norm_trades.append(norm)
    logger.info("Backfill produced %d trades", len(backfill_norm_trades))
    for t in backfill_norm_trades:
        try:
            queue.put_nowait(t)
        except asyncio.QueueFull:
            _ = queue.get_nowait()
            queue.put_nowait(t)
    
        
       
    
     
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
