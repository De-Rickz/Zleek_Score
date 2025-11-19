import asyncio
import json
import websockets
from websockets.exceptions import ConnectionClosed
from writer import load_asset_maps, load_asset_ids
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


async def heartbeat(ws):
    """Send PING every 20 seconds to keep the connection alive"""
    try:
        while True:
            await ws.send("PING")
            await asyncio.sleep(20)
    except asyncio.CancelledError:
        logger.debug("Heartbeat cancelled")
        raise


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
                logger.warning("No asset IDs available. Retrying in %s seconds...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue
            
            asset_maps = await load_asset_maps(pool)
            logger.info("Loaded %d asset IDs and %d mappings", len(asset_ids), len(asset_maps))
            
            # Reset backoff on successful load
            backoff = 1
            
            # 2. Establish websocket connection
            async with websockets.connect(
                url,
                ping_interval=None,
                max_size=8 * 1024 * 1024,
                close_timeout=10  # Don't hang on close
            ) as ws:
                logger.info("Connected to CLOB websocket")
                
                # 3. Subscribe to assets
                await ws.send(json.dumps({"assets_ids": asset_ids}))
                logger.info("Subscribed to %d assets", len(asset_ids))
                
                # 4. Start heartbeat
                hb = asyncio.create_task(heartbeat(ws))
                
                try:
                    # 5. Process messages
                    while True:
                        raw = await ws.recv()
                        
                        # Ignore heartbeat frames
                        if raw in ("PING", "PONG"):
                            continue
                        
                        if not raw.startswith("{") and not raw.startswith("["):
                            logger.debug("Ignoring non-JSON message: %s", raw)
                            continue
                        
                        try:
                            data = json.loads(raw)
                            logger.debug("Received event batch of size %d", len(data))
                        except json.JSONDecodeError:
                            logger.warning("Invalid JSON frame (skipped): %s", raw[:100])
                            continue
                        
                        if not isinstance(data, list):
                            logger.debug("Non-event message (skipped): %s", data)
                            continue
                        
                        # Process events
                        for event in data:
                            try:
                                await process_event(event, asset_maps, queue, raw)
                            except Exception as e:
                                logger.error("Error processing event: %s", e, exc_info=True)
                                # Continue processing other events
                                continue
                
                finally:
                    # 6. Cleanup heartbeat
                    hb.cancel()
                    try:
                        await hb
                    except asyncio.CancelledError:
                        pass
                    logger.info("Heartbeat task cancelled")
        
        except ConnectionClosed as e:
            logger.warning("WebSocket connection closed: %s. Reconnecting in %s seconds...", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        
        except Exception as e:
            logger.error("Unexpected error in subscribe loop: %s", e, exc_info=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


async def process_event(event, asset_maps, queue, raw):
    """Extract and normalize event data, then enqueue for writing"""
    if not isinstance(event, dict):
        logger.debug("Skipping non-dict event: %s", event)
        return
    
    t = event.get("event_type")
    if t not in ("trade", "book"):
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
    
    if t == "trade":
        norm = {
            "kind": "trade",
            "id": event.get("id"),
            "market_id": market_id,
            "market_order_id": event.get("taker_order_id"),
            "asset_id": event_asset_id,
            "ts": dt,
            "price": float(event.get("price", 0)),
            "size_usd": float(event.get("notionalUsd", 0)),
            "side": event.get("side"),
            "maker_wallet": event.get("maker"),
            "taker_wallet": event.get("taker"),
            "tx_hash": event.get("txHash"),
            "status": event.get("status"),
            "raw": raw
        }
        logger.debug("Trade event: market=%s, price=%.4f, size=%.2f", 
                    market_id, norm["price"], norm["size_usd"])
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
        depth_bid = sum(float(b.get("price", 0)) * float(b.get("size", 0)) for b in bids)
        
        best_ask = min((float(a.get("price", 0)) for a in asks), default=0.0)
        depth_ask = sum(float(a.get("price", 0)) * float(a.get("size", 0)) for a in asks)
        
        norm = {
            "kind": "book",
            "market_id": market_id,
            "ts": dt,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid_depth_usd": depth_bid,
            "ask_depth_usd": depth_ask,
            "bids": json.dumps(bids),
            "asks": json.dumps(asks),
        }
        logger.debug("Book event: market=%s, bid=%.4f, ask=%.4f", 
                    market_id, best_bid, best_ask)
        await enqueue_with_backpressure(queue, norm)


def parse_timestamp(ts):
    """Parse timestamp from various formats (epoch ms or ISO string)"""
    if ts is None:
        return None
    
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        
        elif isinstance(ts, str):
            # Try numeric first (common case for Polymarket)
            try:
                ts_num = float(ts)
                return datetime.fromtimestamp(ts_num / 1000, tz=timezone.utc)
            except ValueError:
                # Try ISO format
                clean = ts.replace("Z", "+00:00")
                return datetime.fromisoformat(clean)
    except (ValueError, OSError) as e:
        logger.warning("Failed to parse timestamp '%s': %s", ts, e)
        return None
    
    logger.debug("Unsupported timestamp format: %s", ts)
    return None


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