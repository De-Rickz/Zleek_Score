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

# 🔌 1. Signature updated to expect 'writer' instead of 'queue'
async def subscribe(writer, pool):
    """
    Subscribe to Polymarket CLOB websocket with automatic reconnection
    and asset map reloading on each connection attempt.
    """
    backoff = 1
    max_backoff = 60
    
    while True:
        try:
            logger.info("Loading asset IDs and maps...")
            asset_ids = await load_asset_ids(pool)
            
            if not asset_ids:
                logger.warning("No asset IDs available. Retrying in %s seconds...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue
            
            asset_maps = await load_asset_maps(pool)
            logger.info("Loaded %d asset IDs and %d mappings", len(asset_ids), len(asset_maps))
            
            backoff = 1
            
            async with websockets.connect(
                url,
                ping_interval=None,
                max_size=8 * 1024 * 1024,
                close_timeout=10 
            ) as ws:
                logger.info("Connected to CLOB websocket")
                
                await ws.send(json.dumps({"assets_ids": asset_ids}))
                logger.info("Subscribed to %d assets", len(asset_ids))
                
                hb = asyncio.create_task(heartbeat(ws))
                
                try:
                    while True:
                        raw = await ws.recv()
                        
                        if raw in ("PING", "PONG"):
                            continue
                        
                        if not raw.startswith("{") and not raw.startswith("["):
                            continue
                        
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        
                        if isinstance(data, dict):
                            data = [data]
                            
                        if not isinstance(data, list):
                            continue
                        
                        for event in data:
                            try:
                                # 🔌 2. Pass the writer down to the event processor
                                await process_event(event, asset_maps, writer, raw)
                            except Exception as e:
                                logger.error("Error processing event: %s", e, exc_info=True)
                                continue
                
                finally:
                    hb.cancel()
                    try:
                        await hb
                    except asyncio.CancelledError:
                        pass
        
        except ConnectionClosed as e:
            logger.warning("WebSocket closed: %s. Reconnecting in %s sec...", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        
        except Exception as e:
            logger.error("Unexpected error in subscribe loop: %s", e, exc_info=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

# 🔌 3. Signature updated to expect 'writer'
async def process_event(event, asset_maps, writer, raw):
    """Extract and normalize event data, then send directly to our elite writer."""
    if not isinstance(event, dict):
        return
    
    t = event.get("event_type")
    if t not in ("trade", "book"):
        return
    
    event_asset_id = event.get("asset_id")
    if event_asset_id is None:
        return
    
    info = asset_maps.get(event_asset_id)
    if info is None:
        return
    
    market_id, side_label = info
    ts = event.get("timestamp")
    
    dt = parse_timestamp(ts)
    if dt is None:
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
            "raw": raw,
            # ⏱️ Telemetry timestamp added!
            "ingest_ts": datetime.now(timezone.utc).isoformat() 
        }
        
        # 🏎️ 4. Direct push to the Redis Waiting Room
        await writer.add_item(norm)
    
    elif t == "book":
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
            # 🧹 Left as raw lists to avoid double JSON encoding
            "bids": bids,
            "asks": asks,
            # ⏱️ Telemetry timestamp added!
            "ingest_ts": datetime.now(timezone.utc).isoformat()
        }
        
        # 🏎️ 5. Direct push to the Redis Waiting Room (Bouncer checks for spam)
        await writer.add_item(norm)


def parse_timestamp(ts):
    """Parse timestamp from various formats (epoch ms or ISO string)"""
    if ts is None:
        return None
    
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        
        elif isinstance(ts, str):
            try:
                ts_num = float(ts)
                return datetime.fromtimestamp(ts_num / 1000, tz=timezone.utc)
            except ValueError:
                clean = ts.replace("Z", "+00:00")
                return datetime.fromisoformat(clean)
    except (ValueError, OSError) as e:
        logger.warning("Failed to parse timestamp '%s': %s", ts, e)
        return None
    return None

# 🗑️ Deleted: enqueue_with_backpressure (No longer needed!)