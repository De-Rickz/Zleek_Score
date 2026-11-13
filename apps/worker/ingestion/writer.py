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
            ON CONFLICT (tx_hash) DO UPDATE SET price=EXCLUDED.price
            WHERE trades.price IS DISTINCT FROM EXCLUDED.price;
        """
        # The WHERE (ROADMAP phase 4 step 2a): the periodic backfill re-fetches
        # the last hour every 5 minutes, so each trade arrives ~12 times. An
        # unconditional DO UPDATE writes a new row version every time, even
        # with the same price (dead rows, WAL, autovacuum); with the WHERE,
        # Postgres skips the write unless the price actually changed.
        
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

async def load_condition_ids_for_backfill(pool, exclude_asset_ids=(), min_liq=10000, limit=200,
                                          fresh_within=timedelta(hours=2)):
    # Ranked by 24h volume, with the same filters as load_asset_ids (ROADMAP
    # phase 4 step 2): ranked by liquidity, the 200 were mostly quiet markets
    # (on 2026-11-07 only 51 traded in 5 hours), and busy markets went
    # unfollowed until they reached the websocket's top 20 (42.7% of the
    # Data API's trades for these markets not stored that day). So the
    # periodic backfill covers the markets ranked 21-220, the ones about to
    # enter the websocket.
    # Websocket markets are excluded only because the websocket already stores
    # their trades; an overlap would be harmless (both paths carry tx_hash, and
    # REST_QUERY's ON CONFLICT (tx_hash) dedupes). The gap backfill passes no
    # exclusion and gets the top 200 including the websocket's.
    query = """
        SELECT condition_id
        FROM markets
        WHERE status = 'open' AND liquidity_usd >= $1 AND condition_id IS NOT NULL
          AND ($4::interval IS NULL OR updated_at > now() - $4::interval)
          AND NOT (token_yes_id = ANY($3::text[]) OR token_no_id = ANY($3::text[]))
        GROUP BY condition_id
        ORDER BY MAX((raw->>'volume24hr')::float8) DESC NULLS LAST
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, min_liq, limit, list(exclude_asset_ids), fresh_within)
        if not rows:
            logger.warning("Backfill: no markets refreshed in the last %s; using stale rows", fresh_within)
            rows = await conn.fetch(query, min_liq, limit, list(exclude_asset_ids), None)
    return [r["condition_id"] for r in rows]

DATA_API_URL = "https://data-api.polymarket.com"
# Data API v2 (v1's /trades was retired on 2026-11-02 and answers HTTP 410).
# Spec: https://data-api.polymarket.com/v2/openapi.json. For a market feed:
#   - condition=<ids>: up to 20 condition ids per request, one newest-first feed;
#   - no time filter (start/end are honoured only for a single wallet), so we
#     page back until a page's oldest trade is older than the window start;
#   - cursor-only paging (next_cursor until null); the same filters must be
#     re-sent with every cursor; limit is at most 1000;
#   - snake_case fields (token_id, transaction_hash, outcome_index); size in shares;
#   - 300 requests / 10 s for /v2/trades; 429 (rate) and 503 (server busy) carry
#     Retry-After.
DATA_API_TRADES = "/v2/trades"
DATA_API_MAX_CONDITIONS = 20
BACKFILL_PAGE_SIZE = 1000
BACKFILL_REQUEST_GAP_SEC = 0.2   # well under 300 req / 10 s
DATA_API_MAX_ATTEMPTS = 4

def _trade_key(t):
    return (t.get("transaction_hash"), t.get("token_id"), t.get("timestamp"), str(t.get("size")))

def condition_batches(condition_ids, size=DATA_API_MAX_CONDITIONS):
    """Split condition ids into groups the v2 API accepts in one request."""
    return [condition_ids[i:i + size] for i in range(0, len(condition_ids), size)]

async def _get_trades_page(client, params):
    """One /v2/trades page: (trades, next_cursor). Waits out 429/503 using
    Retry-After (else 5, 10, 15 s), then raises on any other error."""
    for attempt in range(DATA_API_MAX_ATTEMPTS):
        resp = await client.get(DATA_API_TRADES, params=params)
        if resp.status_code not in (429, 503) or attempt == DATA_API_MAX_ATTEMPTS - 1:
            break
        try:
            wait = float(resp.headers.get("Retry-After", ""))
        except ValueError:
            wait = 5.0 * (attempt + 1)
        await asyncio.sleep(min(max(wait, 1.0), 60.0))
    resp.raise_for_status()
    body = resp.json()
    return body.get("data") or [], (body.get("pagination") or {}).get("next_cursor")

async def fetch_public_trades(client, condition_ids, cutoff_unix, end_unix=None, page_size=BACKFILL_PAGE_SIZE):
    """
    Public trades for up to 20 markets (condition ids; one id as a string is
    fine) with timestamp in [cutoff_unix, end_unix] (epoch seconds; end
    defaults to no upper bound). The feed is newest first, so paging stops at
    the first page whose oldest trade is before cutoff_unix, or when the
    cursor runs out. Duplicates (by transaction hash, token, time and size)
    are dropped.
    """
    if isinstance(condition_ids, str):
        condition_ids = [condition_ids]
    if not condition_ids:
        return []
    if len(condition_ids) > DATA_API_MAX_CONDITIONS:
        raise ValueError(f"at most {DATA_API_MAX_CONDITIONS} condition ids per request, got {len(condition_ids)}")
    filters = {"condition": ",".join(condition_ids), "limit": page_size}
    trades, seen, cursor = [], set(), None
    while True:
        params = filters if cursor is None else {**filters, "cursor": cursor}
        page, cursor = await _get_trades_page(client, params)
        await asyncio.sleep(BACKFILL_REQUEST_GAP_SEC)
        for t in page:
            ts = t.get("timestamp", 0)
            if ts < cutoff_unix or (end_unix is not None and ts > end_unix):
                continue
            key = _trade_key(t)
            if key not in seen:
                seen.add(key)
                trades.append(t)
        if not page or not cursor or page[-1].get("timestamp", 0) < cutoff_unix:
            return trades

async def queue_public_trades(writer, trades, asset_maps):
    """Map Data API trades to YES terms and queue them for the writer. Returns
    how many were queued (trades for tokens we don't know are skipped)."""
    queued = 0
    for trade in trades:
        asset = trade.get("token_id") or trade.get("asset")   # v2, else v1 name
        info = asset_maps.get(asset)
        if info is None:
            continue
        raw_price = float(trade.get("price", 0))
        price, side = to_yes_terms(raw_price, trade.get("side"), info[1])
        ts = datetime.fromtimestamp(trade["timestamp"], tz=timezone.utc)
        norm = {
            "kind": "trade", "market_id": info[0], "asset_id": asset,
            "ts": ts.isoformat(), "match_time": ts.isoformat(), "price": price,
            # Data API size is in shares; the raw token price is USD per share
            "size_usd": float(trade.get("size", 0)) * raw_price,
            "side": side, "tx_hash": trade.get("transaction_hash") or trade.get("transactionHash"), "raw": trade,
            "ingest_ts": datetime.now(timezone.utc).isoformat(),
        }
        await writer.add_item(norm)
        queued += 1
    return queued

# --- Catch-up on join (ROADMAP phase 4 step 3) ---
# When the websocket subscribes to a market it wasn't subscribed to, the
# market's trades from before that moment are otherwise never fetched: the
# periodic backfill drops it once it enters the top 20, and its last run may
# be up to 5 minutes old. So fetch the market's last hour once, a little after
# subscribing (the Data API shows trades up to ~80 s late). The overlap with
# what the websocket and the backfill already stored dedupes on tx_hash
# (REST_QUERY, which skips unchanged rows).
CATCH_UP_DELAY_SEC = 120
CATCH_UP_LOOKBACK = timedelta(hours=1)


async def load_condition_ids_for_assets(pool, asset_ids):
    """Condition ids of the markets that own these token ids."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT DISTINCT condition_id FROM markets
               WHERE condition_id IS NOT NULL
                 AND (token_yes_id = ANY($1::text[]) OR token_no_id = ANY($1::text[]))""",
            list(asset_ids))
    return sorted(r["condition_id"] for r in rows)


async def catch_up_on_join(pool, writer, asset_ids, joined_at,
                           delay_sec=CATCH_UP_DELAY_SEC, lookback=CATCH_UP_LOOKBACK):
    """Fetch and queue the last `lookback` (before joined_at) of trades for
    newly subscribed tokens, after waiting delay_sec. Never raises (other than
    cancellation): a failure is logged and the websocket is unaffected.
    Returns how many trades were queued."""
    try:
        await asyncio.sleep(delay_sec)
        conds = await load_condition_ids_for_assets(pool, asset_ids)
        if not conds:
            return 0
        asset_maps = await load_asset_maps(pool)
        cutoff_unix = int((joined_at - lookback).timestamp())
        queued = failed = 0
        batches = condition_batches(conds)
        async with httpx.AsyncClient(base_url=DATA_API_URL, timeout=15.0) as client:
            for batch in batches:
                try:
                    queued += await queue_public_trades(
                        writer, await fetch_public_trades(client, batch, cutoff_unix), asset_maps)
                except Exception as e:
                    failed += 1
                    logger.warning("Catch-up on join error for %d markets (first %s): %s",
                                   len(batch), batch[0], e)
        if failed:
            logger.warning("Catch-up on join queued %d trades for %d new markets; %d of %d requests failed",
                           queued, len(conds), failed, len(batches))
        else:
            logger.info("Catch-up on join queued %d trades from the last %s for %d new markets",
                        queued, lookback, len(conds))
        return queued
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning("Catch-up on join failed: %s", e)
        return 0


# Runs in a row in which every Data API request failed (reset by any run that
# got through). Logged with each failed run so a broken API is obvious in the
# logs; the heartbeat's "backfill" check alerts on the missing trades.
backfill_failed_runs = 0


def summarize_backfill(queued, n_markets, n_requests, failed, first_error):
    """Log one line per periodic backfill run, at a level that matches what
    happened, and keep the count of fully failed runs in a row."""
    global backfill_failed_runs
    if n_requests and failed == n_requests:
        backfill_failed_runs += 1
        logger.error("Backfill run FAILED: all %d Data API requests failed for %d markets "
                     "(%d failed runs in a row); first error: %s",
                     n_requests, n_markets, backfill_failed_runs, first_error)
        return
    backfill_failed_runs = 0
    if failed:
        logger.warning("Backfill queued %d trades from %d markets; %d of %d Data API requests failed",
                       queued, n_markets, failed, n_requests)
    else:
        logger.info("Backfill queued %d trades from %d markets (%d requests)", queued, n_markets, n_requests)


# --- Catch-up on entry to the backfill (ROADMAP phase 4 step 2c) ---
# The backfill fetches only the last hour, so a market that has just entered
# its top 200 (its 24h volume passed the 220th market's, which can take hours
# for a new market) or come back after dropping out loses everything it traded
# before that hour. A market new to the set is fetched over the last 24 hours
# once (the window its volume was ranked on); the rest stay at the last hour.
# "New" = not fetched successfully by the previous run in this process, so the
# first run after a start, and markets whose batch failed, get 24 hours too.
# REST_QUERY's ON CONFLICT (tx_hash) dedupes the overlap with what is stored.
ENTRY_LOOKBACK = timedelta(hours=24)
backfill_prev_conds = None   # condition ids the previous run fetched successfully


def split_new_entries(conds, prev):
    """(new to the set, already fetched by the previous run), in the given
    order; with no previous run every market is new."""
    if prev is None:
        return list(conds), []
    return [c for c in conds if c not in prev], [c for c in conds if c in prev]


async def run_backfill(pool, writer, hours_back=1, entry_lookback=ENTRY_LOOKBACK):
    """
    🔄 The Producer: Fills the Redis buffer with recent public trades from the
    Data API. (The CLOB /data/trades endpoint only returns the authenticated
    wallet's own trades, which is why the old backfill wrote nothing.)
    Markets new to the set get the last `entry_lookback` instead of
    `hours_back` (step 2c, above).
    """
    global backfill_prev_conds
    now = datetime.now(timezone.utc)
    cutoff_unix = int((now - timedelta(hours=hours_back)).timestamp())
    entry_cutoff_unix = int((now - entry_lookback).timestamp())

    asset_maps = await load_asset_maps(pool)
    ws_asset_ids = await load_asset_ids(pool)
    conds = await load_condition_ids_for_backfill(pool, exclude_asset_ids=ws_asset_ids)
    new, known = split_new_entries(conds, backfill_prev_conds)

    queued = queued_entry = failed = 0
    first_error = None
    failed_conds = set()
    plan = ([(b, entry_cutoff_unix, True) for b in condition_batches(new)]
            + [(b, cutoff_unix, False) for b in condition_batches(known)])
    async with httpx.AsyncClient(base_url=DATA_API_URL, timeout=15.0) as client:
        for batch, cutoff, is_entry in plan:
            try:
                n = await queue_public_trades(
                    writer, await fetch_public_trades(client, batch, cutoff), asset_maps)
                queued += n
                queued_entry += n if is_entry else 0
            except Exception as e:
                failed += 1
                failed_conds.update(batch)
                first_error = first_error or f"{type(e).__name__}: {e}"
                logger.error("Backfill error for %d markets (first %s): %s", len(batch), batch[0], e)
    backfill_prev_conds = set(conds) - failed_conds
    if new:
        logger.info("Backfill catch-up on entry: %d markets new to the set, %d trades queued from the last %s",
                    len(new), queued_entry, entry_lookback)
    summarize_backfill(queued, len(conds), len(plan), failed, first_error)


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
GAP_PAGE_SIZE = 1000                     # the v2 maximum
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
            for batch in condition_batches(conds):
                try:
                    trades = await fetch_public_trades(client, batch, start_unix, end_unix=end_unix,
                                                       page_size=GAP_PAGE_SIZE)
                    queued += await queue_public_trades(writer, trades, asset_maps)
                except Exception as e:
                    failed += len(batch)
                    logger.error("Gap backfill error for %d markets (first %s): %s", len(batch), batch[0], e)
        if conds and failed == len(conds):
            logger.error("Gap backfill FAILED: every Data API request failed (%d markets); "
                         "the gap since %s is not recovered", len(conds), start)
        else:
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
