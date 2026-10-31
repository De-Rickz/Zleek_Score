import asyncio
import logging
import json
import statistics
from datetime import datetime, timezone, timedelta
from collections import deque
from decimal import Decimal
import asyncpg
import httpx
# 📦 High-performance async drivers


logger = logging.getLogger("Ingestion.Writer")

# Flush safety (ROADMAP phase 2): after this many consecutive failed batches,
# insert row by row and move rows that can never be written to DEAD_LETTER_KEY,
# so one bad row can't block every later flush.
FLUSH_MAX_BATCH_FAILURES = 3
DEAD_LETTER_KEY = "buffer:dead"
# Errors that mean the row itself is bad (retrying can't help). Anything else
# (connection lost, database down) keeps the row queued for the next flush.
ROW_DATA_ERRORS = (asyncpg.exceptions.DataError,
                   asyncpg.exceptions.IntegrityConstraintViolationError,
                   ValueError, TypeError, KeyError, AttributeError)

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

def to_yes_terms(price, side, side_label):
    """
    Express a trade in outcome-0 ("YES") terms so both tokens of a market share
    one price series. A NO trade at p is a YES trade at 1 - p, and buying NO is
    selling YES. size_usd is NOT converted: callers compute it from the raw
    token price, which is the USD actually paid.
    """
    if side_label == "NO":
        return 1.0 - price, {"BUY": "SELL", "SELL": "BUY"}.get(side, side)
    return price, side

def book_to_yes_terms(bids, asks, side_label):
    """
    Express an order book in outcome-0 ("YES") terms. Someone selling NO at q
    is offering YES at 1 - q, so the NO book's asks become YES bids and its
    bids become YES asks, each repriced to 1 - q (sizes in shares are
    unchanged). Decimal keeps the repriced levels exact ("0.06", not
    "0.06000000000000005").
    """
    if side_label != "NO":
        return bids, asks
    def flip(levels):
        return [{"price": str(Decimal(1) - Decimal(str(lvl.get("price", 0)))), "size": lvl.get("size", 0)}
                for lvl in levels]
    return flip(asks), flip(bids)

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

        # Flush state: one flush at a time; consecutive whole-batch failures
        self._flush_lock = asyncio.Lock()
        self._batch_failures = 0
        
        # 📊 Telemetry
        self.write_lags = deque(maxlen=200)
        self.write_lag_counter = 0

        # 🏛️ SQL Queries (Your original schema)
        # trades.id is a bigserial; it is left to the default (an explicit NULL
        # would violate the primary key and abort the whole flush transaction).
        #
        # KNOWN LIMITATION: trades.tx_hash is UNIQUE, so if one on-chain
        # transaction ever carries several fills, only the first fill's row
        # (and size_usd) is kept; later fills only overwrite price. Applies to
        # both live websocket and backfill trades. Observed 0 times as of
        # 2026-10-28 (60k+ live events, 2.5k-fill Data API sample). Summing
        # size_usd on conflict is NOT a safe fix: the backfill re-fetches the
        # same trades every run and the websocket can redeliver, so sizes would
        # inflate. A real fix needs a per-fill unique key (schema change).
        # Monitor with scripts/check_multifill_collisions.py.
        self.REST_QUERY = """
            INSERT INTO trades(market_id, asset_id, ts, price, size_usd, side,
                               maker_wallet, taker_wallet, tx_hash, raw, status,
                               market_order_id, match_time)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
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
            # Any trade with a tx_hash (backfill or live websocket) goes through
            # REST_QUERY, whose ON CONFLICT (tx_hash) makes a fill seen by both
            # paths, or redelivered by the websocket, update the existing row
            # (same price) instead of raising a UNIQUE violation.
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

        Items leave Redis only AFTER the Postgres transaction commits: each queue
        is read with LRANGE, written, and then exactly those items are trimmed
        from the tail (add_item pushes new items onto the head, so anything
        queued during the flush stays). If the insert fails or the worker dies
        mid-flush, everything is still queued and the next flush retries it;
        REST_QUERY (ON CONFLICT tx_hash) and OB_QUERY (ON CONFLICT DO NOTHING on
        the (market_id, ts) key) make the retry harmless. WS_QUERY has no unique
        key, so trades without tx_hash could be written twice; a warning is
        logged whenever any reach it (0 since 2026-10-28).
        """
        async with self._flush_lock:
            queues = [(self.REDIS_REST, self.REST_QUERY, self._map_rest),
                      (self.REDIS_WS, self.WS_QUERY, self._map_ws),
                      (self.REDIS_SNAPS, self.OB_QUERY, self._map_snap)]
            pipe = self.redis.pipeline()
            for key, _, _ in queues:
                pipe.lrange(key, 0, -1)
            batches = await pipe.execute()
            if not any(batches):
                return
            if batches[1]:
                logger.warning("%d trade(s) without a transaction hash reached WS_QUERY "
                               "(no dedupe key: a retried flush can store them twice)", len(batches[1]))

            try:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        for (_, query, mapper), raw in zip(queues, batches):
                            if raw:
                                rows = [mapper(json.loads(r)) for r in reversed(raw)]
                                await conn.executemany(query, rows)
                self._batch_failures = 0
            except Exception as e:
                self._batch_failures += 1
                if self._batch_failures < FLUSH_MAX_BATCH_FAILURES:
                    raise  # nothing trimmed: flush_loop logs it and the next flush retries
                logger.error("Flush failed %d times in a row (%s: %s); retrying row by row",
                             self._batch_failures, type(e).__name__, e)
                await self._flush_row_by_row(queues, batches)  # raises if the database is unreachable
                self._batch_failures = 0

            pipe = self.redis.pipeline()
            for (key, _, _), raw in zip(queues, batches):
                if raw:
                    pipe.ltrim(key, 0, -(len(raw) + 1))  # drop exactly the items just written
            await pipe.execute()
            logger.info("✅ Database batch flush complete.")

    async def _flush_row_by_row(self, queues, batches):
        """Insert each row on its own; rows that fail with a data error go to
        DEAD_LETTER_KEY with the error. A connection-level error stops here and
        propagates, so nothing is trimmed or dead-lettered and the batch is
        retried on the next flush."""
        dead = []
        async with self.pool.acquire() as conn:
            for (key, query, mapper), raw in zip(queues, batches):
                for r in reversed(raw):
                    text = r.decode() if isinstance(r, bytes) else r
                    try:
                        await conn.execute(query, *mapper(json.loads(text)))
                    except ROW_DATA_ERRORS as e:
                        dead.append(json.dumps({"queue": key, "error": f"{type(e).__name__}: {e}",
                                                "item": text}))
        if dead:
            await self.redis.lpush(DEAD_LETTER_KEY, *dead)
            logger.error("Moved %d unwritable row(s) to %s", len(dead), DEAD_LETTER_KEY)

    # --- 🗺️ Mapping Helpers ---
    def _map_rest(self, e):
        return (e.get("market_id"), e.get("asset_id"), coerce_dt(e.get("ts")),
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

    flush_interval_sec = 5

    async def flush_loop(self, interval=5):
        self.flush_interval_sec = interval
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

async def load_asset_ids(pool, min_liq=10000, limit=20, fresh_within=timedelta(hours=2)):
    # Top markets by 24h volume (liquidity goes stale once a market quiets down).
    # Gamma bumps updatedAt on live markets within minutes, so rows it has not
    # refreshed recently have dropped out of the fetch (closed, delisted, or
    # past the ~2,100-row pagination cap) and are skipped. If the Gamma refresh
    # has been failing, fall back to any open market rather than subscribing to
    # nothing.
    query = """
        SELECT token_yes_id, token_no_id FROM markets
        WHERE status = 'open' AND liquidity_usd >= $1
          AND ($3::interval IS NULL OR updated_at > now() - $3::interval)
        ORDER BY (raw->>'volume24hr')::float8 DESC NULLS LAST
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, min_liq, limit, fresh_within)
        if not rows:
            logger.warning("No markets refreshed in the last %s; using stale rows", fresh_within)
            rows = await conn.fetch(query, min_liq, limit, None)
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

async def load_condition_ids_for_backfill(pool, exclude_asset_ids=(), min_liq=10000, limit=200):
    # Markets already on the websocket are excluded: WS trades are stored without
    # a tx_hash, so backfilling them too would store every trade twice.
    query = """
        SELECT condition_id
        FROM markets
        WHERE status = 'open' AND liquidity_usd >= $1 AND condition_id IS NOT NULL
          AND NOT (token_yes_id = ANY($3::text[]) OR token_no_id = ANY($3::text[]))
        GROUP BY condition_id
        ORDER BY MAX(liquidity_usd) DESC
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, min_liq, limit, list(exclude_asset_ids))
    return [r["condition_id"] for r in rows]

DATA_API_URL = "https://data-api.polymarket.com"
BACKFILL_PAGE_SIZE = 500
BACKFILL_REQUEST_GAP_SEC = 0.5  # the Data API returns 429 after a few rapid requests

def _trade_key(t):
    return (t.get("transactionHash"), t.get("asset"), t.get("timestamp"), str(t.get("size")))

async def fetch_public_trades(client, condition_id, cutoff_unix, end_unix=None, page_size=BACKFILL_PAGE_SIZE):
    """
    Public trades for one market with timestamp in [cutoff_unix, end_unix]
    (epoch seconds; end defaults to now), newest first.

    Pages by moving the Data API's `end` filter back to the oldest trade seen,
    always at offset 0: the API rejects offsets above 10,000 ("max historical
    trades offset of 10000 exceeded"), and busy markets pass that within a few
    hours (10,518 trades in 3h on one market, 2026-10-31). Trades on the
    boundary second come back twice and are dropped by key.
    """
    end = end_unix if end_unix is not None else int(datetime.now(timezone.utc).timestamp())
    trades, seen = [], set()
    while True:
        for attempt in range(3):
            resp = await client.get("/trades", params={
                "market": condition_id, "limit": page_size, "offset": 0,
                "start": cutoff_unix, "end": end,
            })
            if resp.status_code != 429:
                break
            await asyncio.sleep(5 * (attempt + 1))
        resp.raise_for_status()
        page = resp.json()
        await asyncio.sleep(BACKFILL_REQUEST_GAP_SEC)

        new = [t for t in page if _trade_key(t) not in seen and t.get("timestamp", 0) >= cutoff_unix]
        seen.update(_trade_key(t) for t in new)
        trades.extend(new)
        if len(page) < page_size:
            return trades
        oldest = page[-1].get("timestamp", 0)
        if new and oldest < end:
            end = oldest  # normal case: the next page ends at the oldest second seen
        else:
            # A full page within one second: the cursor can't advance, so step
            # past it. Only the rest of that one second is skipped.
            logger.warning("Backfill %s: more than %d trades at %s; rest of that second skipped",
                           condition_id, page_size, oldest)
            end = oldest - 1
        if end < cutoff_unix:
            return trades

async def queue_public_trades(writer, trades, asset_maps):
    """Map Data API trades to YES terms and queue them for the writer. Returns
    how many were queued (trades for tokens we don't know are skipped)."""
    queued = 0
    for trade in trades:
        info = asset_maps.get(trade.get("asset"))
        if info is None:
            continue
        raw_price = float(trade.get("price", 0))
        price, side = to_yes_terms(raw_price, trade.get("side"), info[1])
        ts = datetime.fromtimestamp(trade["timestamp"], tz=timezone.utc)
        norm = {
            "kind": "trade", "market_id": info[0], "asset_id": trade.get("asset"),
            "ts": ts.isoformat(), "match_time": ts.isoformat(), "price": price,
            # Data API size is in shares; the raw token price is USD per share
            "size_usd": float(trade.get("size", 0)) * raw_price,
            "side": side, "tx_hash": trade.get("transactionHash"), "raw": trade,
            "ingest_ts": datetime.now(timezone.utc).isoformat(),
        }
        await writer.add_item(norm)
        queued += 1
    return queued

async def run_backfill(pool, writer, hours_back=1):
    """
    🔄 The Producer: Fills the Redis buffer with recent public trades from the
    Data API. (The CLOB /data/trades endpoint only returns the authenticated
    wallet's own trades, which is why the old backfill wrote nothing.)
    """
    t_start = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    cutoff_unix = int(t_start.timestamp())

    asset_maps = await load_asset_maps(pool)
    ws_asset_ids = await load_asset_ids(pool)
    conds = await load_condition_ids_for_backfill(pool, exclude_asset_ids=ws_asset_ids)

    queued = 0
    async with httpx.AsyncClient(base_url=DATA_API_URL, timeout=15.0) as client:
        for cond in conds:
            try:
                queued += await queue_public_trades(
                    writer, await fetch_public_trades(client, cond, cutoff_unix), asset_maps)
            except Exception as e:
                logger.error(f"Backfill error for {cond}: {e}")
    logger.info("Backfill queued %d trades from %d markets", queued, len(conds))
            
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


# --- Startup gap backfill (ROADMAP phase 2) ---
GAP_BACKFILL_CAP = timedelta(hours=48)   # never fetch more than this after an outage
GAP_OVERLAP = timedelta(minutes=5)       # re-fetch a little before the newest stored trade
GAP_PAGE_SIZE = 1000                     # the Data API accepts up to 10,000
GAP_FLUSH_WAIT_SEC = 300                 # how long to wait for the writer to drain the queue

def compute_gap_window(newest_trade_ts, now, cap=GAP_BACKFILL_CAP, overlap=GAP_OVERLAP):
    """
    Start of the startup backfill, and whether the cap cut it short.
    None if there are no stored trades (fresh database: the periodic 1-hour
    backfill covers it).
    """
    if newest_trade_ts is None:
        return None
    start = newest_trade_ts - overlap
    if start < now - cap:
        return now - cap, True
    return start, False

async def wait_for_writer_drain(writer, timeout_sec=GAP_FLUSH_WAIT_SEC, interval_sec=2):
    """Wait until the flush loop has emptied the trade queue, then one more flush
    interval so the last batch is committed. Returns False on timeout."""
    deadline = asyncio.get_running_loop().time() + timeout_sec
    while asyncio.get_running_loop().time() < deadline:
        if await writer.redis.llen(writer.REDIS_REST) == 0:
            await asyncio.sleep(writer.flush_interval_sec)
            return True
        await asyncio.sleep(interval_sec)
    return False

async def newest_stored_trade(pool):
    """Read BEFORE the websocket and flush loop start: once they run, fresh live
    trades make the gap look empty."""
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT max(ts) FROM trades")

async def run_gap_backfill(pool, writer, newest_trade_ts):
    """
    Once at startup: fetch every market's public trades since newest_trade_ts
    (from newest_stored_trade, minus GAP_OVERLAP, capped at GAP_BACKFILL_CAP),
    then rebuild bars for that range. Runs in the background so the websocket
    starts immediately.

    - Websocket markets are NOT excluded (unlike the periodic backfill): the
      websocket was down for the same gap. Live and backfilled trades both carry
      tx_hash, so REST_QUERY's ON CONFLICT (tx_hash) dedupes any overlap.
    - Bars must be rebuilt explicitly: the bars job's first run after a restart
      happens before these trades are flushed, and later runs only look back
      BARS_LOOKBACK (65 min).
    """
    from bars_jobs import rebuild_bars  # local import: bars_jobs is a sibling module
    now = datetime.now(timezone.utc)
    try:
        newest = newest_trade_ts
        window = compute_gap_window(newest, now)
        if window is None:
            logger.info("Gap backfill: no stored trades, nothing to fill")
            return
        start, capped = window
        if capped:
            logger.warning("Gap backfill capped at %s: trades between %s and %s are not recovered",
                           GAP_BACKFILL_CAP, newest, start)
        logger.info("Gap backfill: fetching trades from %s to %s", start, now)

        asset_maps = await load_asset_maps(pool)
        conds = await load_condition_ids_for_backfill(pool)  # no websocket exclusion, see above
        start_unix, end_unix = int(start.timestamp()), int(now.timestamp())
        queued = failed = 0
        async with httpx.AsyncClient(base_url=DATA_API_URL, timeout=30.0) as client:
            for cond in conds:
                try:
                    trades = await fetch_public_trades(client, cond, start_unix, end_unix=end_unix,
                                                       page_size=GAP_PAGE_SIZE)
                    queued += await queue_public_trades(writer, trades, asset_maps)
                except Exception as e:
                    failed += 1
                    logger.error(f"Gap backfill error for {cond}: {e}")
        logger.info("Gap backfill queued %d trades from %d markets (%d failed)", queued, len(conds), failed)

        if not await wait_for_writer_drain(writer):
            logger.error("Gap backfill: writer queue did not drain in %ss; bars for the gap not rebuilt",
                         GAP_FLUSH_WAIT_SEC)
            return
        async with pool.acquire() as conn:
            await rebuild_bars(conn, start, datetime.now(timezone.utc))
        logger.info("Gap backfill: rebuilt bars from %s", start)
    except Exception as e:
        logger.exception(f"Gap backfill failed: {e}")
