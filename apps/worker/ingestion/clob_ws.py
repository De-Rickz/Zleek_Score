import asyncio
import json
import websockets
from websockets.exceptions import ConnectionClosed
from writer import load_asset_maps, load_asset_ids
from datetime import datetime, timezone
import logging
from collections import Counter
from collections import deque
import statistics

lags = deque(maxlen=20)
lag_samples = deque(maxlen=20)

event_counter = Counter()

logger = logging.getLogger(__name__)

url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def iter_events(data):
    """Handle both single dict and list of dicts"""
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


async def subscribe(queue: asyncio.Queue, pool):
    """
    Subscribe to Polymarket CLOB websocket with automatic reconnection
    and asset map reloading on each connection attempt.
    """
    backoff = 1
    max_backoff = 60
    
    while True:
        try:
            # 1. Load fresh asset data on each connection attempt
            logger.info("Loading asset IDs and maps...")
            
            asset_ids = await load_asset_ids(pool)

            if not asset_ids:
                logger.warning(
                    "No asset IDs available. Retrying in %s seconds...", backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue

            asset_maps = await load_asset_maps(pool)
            logger.info(
                "Loaded %d asset IDs and %d mappings", len(asset_ids), len(asset_maps)
            )

            # Reset backoff on successful load
            backoff = 1

            # 2. Establish websocket connection with automatic ping/pong
            async with websockets.connect(
                url,
                ping_interval=20,      # Automatic pings every 20 seconds
                ping_timeout=20,       # Wait 20s for pong response
                max_size=8 * 1024 * 1024,
                close_timeout=10,
                compression=None
            ) as ws:
                logger.info("Connected to CLOB websocket")

                # 3. Subscribe to assets
                await ws.send(json.dumps({"assets_ids": asset_ids, "type": "MARKET"}))
                logger.info("Subscribed to %d assets", len(asset_ids))

                # 4. Process messages (no manual heartbeat needed!)
                while True:
                    raw = await ws.recv()

                    # Handle bytes
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", "ignore")
                    
                    if not isinstance(raw, str):
                        continue

                    # Skip non-JSON
                    raw_strip = raw.lstrip()
                    if not raw_strip.startswith(("{", "[")):
                        logger.debug("Ignoring non-JSON message: %s", raw[:100])
                        continue

                    try:
                        data = json.loads(raw_strip)
                    except json.JSONDecodeError:
                        logger.warning("Invalid JSON frame (skipped): %s", raw[:100])
                        continue

                    # Process events
                    for event in iter_events(data):
                        try:
                            await process_event(event, asset_maps, queue)
                        except Exception as e:
                            logger.error(
                                "Error processing event: %s", e, exc_info=True
                            )
                            continue

        except ConnectionClosed as e:
            logger.warning(
                "WebSocket connection closed: %s. Reconnecting in %s seconds...",
                e,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

        except Exception as e:
            logger.error("Unexpected error in subscribe loop: %s", e, exc_info=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


async def process_event(event, asset_maps, queue):
    """Extract and normalize event data, then enqueue for writing"""
    if not isinstance(event, dict):
        logger.debug("Skipping non-dict event: %s", event)
        return

    t = event.get("event_type")
    event_counter[t] = event_counter.get(t, 0) + 1

    # Log summary every 100 events
    if sum(event_counter.values()) % 100 == 0:
        logger.info("📊 Event stats: %s", dict(event_counter))

    if t not in ("last_trade_price", "book", "price_change", "tick_size_change"):
        logger.debug("Skipping unknown event type: %s", t)
        return

    event_asset_id = event.get("asset_id")
    if event_asset_id is None:
        logger.debug("Skipping event with no asset_id")
        return

    info = asset_maps.get(event_asset_id)
    if info is None:
        # Asset not in our map - silently skip (normal for assets we're not tracking)
        return

    market_id, side_label = info
    ts = event.get("timestamp")

    # Parse timestamp
    dt = parse_timestamp(ts)
    
    if dt is None:
        logger.warning("Failed to parse timestamp, skipping event")
        return
    
    arrival_ts = datetime.now(timezone.utc)
    lag_ms = (arrival_ts - dt).total_seconds() * 1000

    if lag_ms < -50:
        logger.warning(
            "NEGATIVE LAG raw_ts=%r parsed=%s arrival=%s lag_ms=%.0f event_type=%s",
            ts, dt.isoformat(), arrival_ts.isoformat(), lag_ms, t
    )
    # Sample timestamps so we can confirm unit/clock issues without spamming logs.
    lag_samples.append(
        {
            "raw_ts": ts,
            "parsed_ts": dt.isoformat(),
            "arrival_ts": arrival_ts.isoformat(),
            "lag_ms": int(lag_ms),
        }
    )
    if len(lag_samples) == lag_samples.maxlen:
        logger.info("WS lag samples (raw/parsed/arrival/lag_ms): %s", list(lag_samples))
        lag_samples.clear()

    lags.append(lag_ms)

    if len(lags) == 200:
        p50 = statistics.median(lags)
        p95 = sorted(lags)[int(0.95 * len(lags))]
        logger.info("lag p50=%.0fms p95=%.0fms", p50, p95)
        
        
    

    if t == "last_trade_price":
        price = float(event.get("price", 0.0))
        size = float(event.get("size", 0.0))

        norm = {
            "kind": "trade",
            "market_id": market_id,
            "asset_id": event_asset_id,
            "ts": dt,
            "ingest_ts": arrival_ts,
            "price": price,
            "size_usd": price * size,
            "side": event.get("side"),
            "maker_wallet": None,
            "taker_wallet": None,
            "tx_hash": None,
            "status": None,
            "raw": json.dumps(event),  # Store as JSON string
        }
        logger.info(
            "TRADE asset=%s price=%.4f size=%.2f side=%s ts=%s",
            event_asset_id,
            norm["price"],
            norm["size_usd"],
            norm["side"],
            int(ts) if isinstance(ts, (int, float)) else ts
        )
        await enqueue_with_backpressure(queue, norm)

    elif t == "book":
        # Handle bid/ask inversion for NO side
        if side_label == "YES":
            bids = event.get("bids") or []
            asks = event.get("asks") or []
        else:
            bids = event.get("asks") or []
            asks = event.get("bids") or []

        best_bid = max((float(b.get("price", 0)) for b in bids), default=0.0)
        depth_bid = sum(
            float(b.get("price", 0)) * float(b.get("size", 0)) for b in bids
        )

        best_ask = min((float(a.get("price", 0)) for a in asks), default=0.0)
        depth_ask = sum(
            float(a.get("price", 0)) * float(a.get("size", 0)) for a in asks
        )

        norm = {
            "kind": "book",
            "market_id": market_id,
            "ts": dt,
            "ingest_ts": arrival_ts,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid_depth_usd": depth_bid,
            "ask_depth_usd": depth_ask,
            "bids": json.dumps(bids),
            "asks": json.dumps(asks),
        }
        logger.debug(
            "Book event: market=%s, bid=%.4f, ask=%.4f", market_id, best_bid, best_ask
        )
        await enqueue_with_backpressure(queue, norm)


def parse_timestamp(ts):
    if ts is None:
        return None

    try:
        # Numeric epochs
        if isinstance(ts, (int, float)):
            x = int(ts)
            return _parse_epoch(x)

        # String timestamps: try ISO first, then numeric epoch
        if isinstance(ts, str):
            s = ts.strip()
            # ISO
            if "T" in s:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))

            # numeric
            if s.isdigit():
                return _parse_epoch(int(s))

            # numeric with decimals
            try:
                return _parse_epoch(int(float(s)))
            except ValueError:
                return None

    except Exception as e:
        logger.warning("Failed to parse timestamp '%s': %s", ts, e)
        return None

    return None

def _parse_epoch(x: int) -> datetime:
    # seconds epochs are ~1e9, ms epochs are ~1e12
    if x >= 1_000_000_000_000:  # ms
        return datetime.fromtimestamp(x / 1000, tz=timezone.utc)
    else:  # seconds
        return datetime.fromtimestamp(x, tz=timezone.utc)

async def enqueue_with_backpressure(queue, item):
    """Enqueue item with backpressure handling (drop oldest if full)"""
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        logger.warning("Queue full (%d items), dropping oldest item", queue.maxsize)
        try:
            _ = queue.get_nowait()
            queue.put_nowait(item)
        except asyncio.QueueEmpty:
            # Race condition - queue was emptied, try again
            queue.put_nowait(item)
