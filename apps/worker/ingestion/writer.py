import asyncio
import logging
import json
import statistics
from datetime import datetime, timezone, timedelta
from collections import deque
from clob_client import get_client
from py_clob_client.clob_types import TradeParams
# 📦 High-performance async drivers


logger = logging.getLogger("Ingestion.Writer")

def coerce_dt(value):
    """
    💱 The Currency Exchange: Translates string timestamps into 
    native Python datetime objects for the database.
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None

class EliteZleekWriter:
    """
    🏗️ The Stateful Machine: Buffers data in Redis and flushes 
    batches to TimescaleDB.
    """
    def __init__(self, db_pool, redis_client, throttle_sec=5):
        self.pool = db_pool
        self.redis = redis_client
        self.throttle_sec = throttle_sec
        
        # 🔑 Redis queue keys
        self.REDIS_REST = "buffer:trades:rest"
        self.REDIS_WS = "buffer:trades:ws"
        self.REDIS_SNAPS = "buffer:snapshots"
        
        # 🛡️ Bouncer state
        self.last_snapshot_time = {}
        
        # 📊 Telemetry
        self.write_lags = deque(maxlen=200)
        self.write_lag_counter = 0

        # 🏛️ SQL Queries (Your original schema)
        self.REST_QUERY = """
            INSERT INTO trades(id, market_id, asset_id, ts, price, size_usd, side, 
                               maker_wallet, taker_wallet, tx_hash, raw, status, 
                               market_order_id, match_time)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT (tx_hash) DO UPDATE SET price=EXCLUDED.price;
        """
        
        self.WS_QUERY = """
            INSERT INTO trades(market_id, asset_id, ts, price, size_usd, side, raw)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
        """
        
        self.OB_QUERY = """
            INSERT INTO ob_snapshots(market_id, ts, best_ask, best_bid, bid_depth_usd, ask_depth_usd, bids, asks)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (market_id,ts) DO NOTHING;
        """

    async def add_item(self, item: dict):
        """
        👨‍🍳 The Chef: Prepares incoming data and pushes to Redis.
        """
        kind = item.get("kind")
        now = datetime.now(timezone.utc)
        
        # 1. Telemetry
        ingest_ts = coerce_dt(item.get("ingest_ts"))
        if ingest_ts:
            lag_ms = (now - ingest_ts).total_seconds() * 1000
            self.write_lags.append(lag_ms)
            self._log_lag()

        # 2. Routing & Throttling
        if kind == "trade":
            key = self.REDIS_REST if item.get("tx_hash") else self.REDIS_WS
        elif kind == "book":
            market_id = item.get("market_id")
            ts_prev = self.last_snapshot_time.get(market_id)
            if ts_prev and (now - ts_prev).total_seconds() < self.throttle_sec:
                return # 🛡️ Bouncer drops the spam
            self.last_snapshot_time[market_id] = now
            key = self.REDIS_SNAPS
        else:
            return

        # 3. Queueing
        await self.redis.lpush(key, json.dumps(item, default=str))

    def _log_lag(self):
        self.write_lag_counter += 1
        if self.write_lag_counter % 200 == 0 and self.write_lags:
            p50 = statistics.median(self.write_lags)
            logger.info(f"📊 Write lag p50={p50:.0f}ms")

    async def flush(self):
        """
        🚚 The Express Truck: Moves data from Redis to Postgres.
        """
        pipe = self.redis.pipeline()
        for key in [self.REDIS_REST, self.REDIS_WS, self.REDIS_SNAPS]:
            pipe.lrange(key, 0, -1)
            pipe.delete(key)
        
        results = await pipe.execute()
        raw_rest, _, raw_ws, _, raw_snaps, _ = results

        if not any([raw_rest, raw_ws, raw_snaps]):
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                if raw_rest:
                    rows = [self._map_rest(json.loads(r)) for r in reversed(raw_rest)]
                    await conn.executemany(self.REST_QUERY, rows)
                if raw_ws:
                    rows = [self._map_ws(json.loads(r)) for r in reversed(raw_ws)]
                    await conn.executemany(self.WS_QUERY, rows)
                if raw_snaps:
                    rows = [self._map_snap(json.loads(r)) for r in reversed(raw_snaps)]
                    await conn.executemany(self.OB_QUERY, rows)
        
        logger.info("✅ Database batch flush complete.")

    # --- 🗺️ Mapping Helpers ---
    def _map_rest(self, e):
        return (e.get("id"), e.get("market_id"), e.get("asset_id"), coerce_dt(e.get("ts")),
                float(e.get("price", 0)), float(e.get("size_usd", 0)), e.get("side"),
                e.get("maker_wallet"), e.get("taker_wallet"), e.get("tx_hash"),
                json.dumps(e.get("raw")), e.get("status"), e.get("market_order_id"),
                coerce_dt(e.get("match_time")))

    def _map_ws(self, e):
        return (e.get("market_id"), e.get("asset_id"), coerce_dt(e.get("ts")),
                float(e.get("price", 0)), float(e.get("size_usd", 0)), e.get("side"),
                json.dumps(e.get("raw")))

    def _map_snap(self, e):
        return (e.get("market_id"), coerce_dt(e.get("ts")), float(e.get("best_ask", 0)),
                float(e.get("best_bid", 0)), float(e.get("bid_depth_usd", 0)),
                float(e.get("ask_depth_usd", 0)), json.dumps(e.get("bids")),
                json.dumps(e.get("asks")))

    async def flush_loop(self, interval=5):
        while True:
            await asyncio.sleep(interval)
            try:
                await self.flush()
            except Exception as e:
                logger.error(f"Flush error: {e}")

# --- 🏛️ YOUR ORIGINAL HELPER FUNCTIONS ---

async def upsert_markets(pool, data):
    query = """
    insert into markets(id, title, event_id, condition_id, event_title, category, status, liquidity_usd,
                        token_yes_id, token_no_id, created_at, updated_at, raw)
    values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
    on conflict (id) do update
      set title=$2, event_id=$3, condition_id=$4, event_title=$5, category=$6, status=$7,
          liquidity_usd=$8, token_yes_id=$9, token_no_id=$10, created_at=$11,
          updated_at=$12, raw=$13;
    """
    rows = [(m.get("id"), m.get("title"), m.get("event_id"), m.get("condition_id"),
             m.get("event_title"), m.get("category"), m.get("status", "open"),
             m.get("liquidity_usd", 0), m.get("token_yes_id"), m.get("token_no_id"),
             m.get("created_at"), m.get("updated_at"), m.get("raw")) for m in data]
    async with pool.acquire() as conn:
        await conn.executemany(query, rows)

async def load_asset_ids(pool, min_liq=10000, limit=20):
    query = "SELECT token_yes_id, token_no_id FROM markets WHERE status = 'open' AND liquidity_usd >= $1 ORDER BY liquidity_usd DESC LIMIT $2"
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, min_liq, limit)
    ids = set()
    for r in rows:
        if r["token_yes_id"]: ids.add(r["token_yes_id"])
        if r["token_no_id"]: ids.add(r["token_no_id"])
    return list(ids)

async def load_asset_maps(pool):
    query = "SELECT id as market_id, token_yes_id, token_no_id FROM markets"
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)
    m = {}
    for r in rows:
        if r["token_yes_id"]: m[r["token_yes_id"]] = (r["market_id"], "YES")
        if r["token_no_id"]: m[r["token_no_id"]] = (r["market_id"], "NO")
    return m

async def load_condition_ids_for_backfill(pool, min_liq=10000, limit=20):
    query = """
        SELECT condition_id 
        FROM markets 
        WHERE status = 'open' AND liquidity_usd >= $1 AND condition_id IS NOT NULL 
        GROUP BY condition_id 
        ORDER BY MAX(liquidity_usd) DESC 
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, min_liq, limit)
    return [r["condition_id"] for r in rows]

def parse_iso_z(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

async def run_backfill(pool, writer, hours_back=1):
    """
    🔄 The Producer: Fills the Redis buffer with historical data.
    """
    
    clob = get_client()
    t_start = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    cutoff_unix = int(t_start.timestamp())
    
    conds = await load_condition_ids_for_backfill(pool)
    for cond in conds:
        try:
            resp = clob.get_trades(TradeParams(market=cond, after=str(cutoff_unix)))
            for trade in resp:
                mt = parse_iso_z(trade["match_time"])
                if mt < t_start: continue
                
                norm = {
                    "kind": "trade", "id": trade.get("id"), "market_id": trade.get("market_id"),
                    "price": trade.get("price"), "ts": mt.isoformat(), "asset_id": trade.get("asset_id"),
                    "tx_hash": trade.get("transaction_hash"), "raw": trade, "ingest_ts": datetime.now(timezone.utc).isoformat()
                }
                # 🚀 THE BIG CHANGE: Direct call to our new writer
                await writer.add_item(norm)
        except Exception as e:
            logger.error(f"Backfill error for {cond}: {e}")
            
async def run_backfill_loop(pool, writer, interval=3600, hours_back=1):
    """Periodically triggers the backfill process in the background."""
    logger.info(f"Starting backfill loop every {interval}s")
    while True:
        await asyncio.sleep(interval)
        try:
            logger.info(f"Running periodic backfill (last {hours_back} hours)...")
            await run_backfill(pool, writer, hours_back=hours_back)
            logger.info("Periodic backfill completed")
        except Exception as e:
            logger.exception(f"Periodic backfill failed: {e}")