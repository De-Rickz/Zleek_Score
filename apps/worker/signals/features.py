import asyncio
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("Signals.Features")

# Scoring calibration (ROADMAP phase 1, decided 2026-10-30):
Z_WINDOW = timedelta(minutes=240)   # z uses only bars this recent...
Z_MAX_BARS = 20                     # ...at most the 20 newest of them...
Z_MIN_BARS = 10                     # ...and needs at least 10, else z is NULL (quiet)
BOOK_MAX_AGE = timedelta(minutes=15)  # imbalance only from a book this fresh and two-sided
ELIGIBLE_REFRESH = timedelta(hours=2)   # Gamma refreshed it recently...
ELIGIBLE_TRADED = timedelta(hours=24)   # ...or it traded recently


async def eligible_market_ids(conn):
    """
    Markets that get features and scores (and so appear on the dashboard): open
    (status covers closed, resolved and not-accepting-orders), with bars, and
    either refreshed by Gamma within ELIGIBLE_REFRESH or traded within
    ELIGIBLE_TRADED. The traded test keeps active markets that fall outside
    Gamma's ~2,100-market pagination window.
    """
    rows = await conn.fetch("""
        SELECT m.id FROM markets m
        WHERE m.id IN (SELECT DISTINCT market_id FROM bars_1m)
          AND m.status = 'open'
          AND (m.updated_at > now() - $1::interval
               OR EXISTS (SELECT 1 FROM trades t WHERE t.market_id = m.id AND t.ts > now() - $2::interval))
    """, ELIGIBLE_REFRESH, ELIGIBLE_TRADED)
    return [r["id"] for r in rows]

async def calculate_features_for_market(conn, market_id: str):
    """
    Calculates the Price Z-Score and OB Imbalance for a single market
    and saves it to the features_5m table.
    """
    # ---------------------------------------------------------
    # 1. THE Z-SCORE (Rubber Band Metric)
    # ---------------------------------------------------------
    # Only recent bars count: a z built from bars of any age kept a market's last
    # big move (often its resolution) scoring forever after trading stopped.
    bars_query = """
        SELECT bucket, close
        FROM bars_1m
        WHERE market_id = $1 AND bucket > now() - $2::interval
        ORDER BY bucket DESC
        LIMIT $3
    """
    bars = await conn.fetch(bars_query, market_id, Z_WINDOW, Z_MAX_BARS)

    if len(bars) < Z_MIN_BARS:
        z_score = None  # quiet market: scored 0 by tremor
    else:
        closes = pd.Series([float(b["close"]) for b in bars][::-1])  # oldest first
        current_std = closes.std()  # sample stddev, as before
        # Prevent dividing by zero if the price hasn't moved at all
        if current_std == 0 or pd.isna(current_std):
            z_score = 0.0
        else:
            z_score = (closes.iloc[-1] - closes.mean()) / current_std

    # ---------------------------------------------------------
    # 2. THE ORDER BOOK IMBALANCE (Tug-of-War Metric)
    # ---------------------------------------------------------
    # Only the newest snapshot, and only if it is fresh and has both sides. Books
    # exist only for websocket-subscribed markets, so a stale one (market rotated
    # out) or a one-sided one (empty side = exactly +/-1) is shown as n/a instead.
    # Not part of the tremor score until books cover every market (ROADMAP phase 4).
    ob_query = """
        SELECT bid_depth_usd, ask_depth_usd
        FROM ob_snapshots
        WHERE market_id = $1 AND ts > now() - $2::interval
          AND jsonb_array_length(bids) > 0 AND jsonb_array_length(asks) > 0
          AND ts = (SELECT max(ts) FROM ob_snapshots WHERE market_id = $1)
    """
    ob = await conn.fetchrow(ob_query, market_id, BOOK_MAX_AGE)

    ob_imbalance = None
    if ob:
        bids = float(ob['bid_depth_usd'])
        asks = float(ob['ask_depth_usd'])
        total_depth = bids + asks
        
        if total_depth > 0:
            # The Formula: (Bids - Asks) / Total
            # Positive = more buyers. Negative = more sellers.
            ob_imbalance = (bids - asks) / total_depth

    # ---------------------------------------------------------
    # 3. SAVE TO THE VAULT
    # ---------------------------------------------------------
    # We use the current time truncated to the nearest 5-minute mark
    now = datetime.now(timezone.utc)
    # Example: 12:07 becomes 12:05
    minute_bucket = now.minute - (now.minute % 5)
    bucket = now.replace(minute=minute_bucket, second=0, microsecond=0)

    insert_query = """
        INSERT INTO features_5m (market_id, bucket, price_z_score, ob_imbalance)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (market_id, bucket) 
        DO UPDATE SET 
            price_z_score = EXCLUDED.price_z_score,
            ob_imbalance = EXCLUDED.ob_imbalance
    """
    await conn.execute(insert_query, market_id, bucket,
                       None if z_score is None else float(z_score),
                       None if ob_imbalance is None else float(ob_imbalance))
    logger.debug(f"[{market_id}] Z-Score: {z_score} | Imbalance: {ob_imbalance}")


async def run_features_job(pool):
    """The infinite loop that wakes up every 5 minutes to run the math."""
    logger.info("Starting 5-minute Features Engine...")
    
    while True:
        try:
            async with pool.acquire() as conn:
                markets = await eligible_market_ids(conn)

                for market_id in markets:
                    await calculate_features_for_market(conn, market_id)
                    
            logger.info("Calculated features for %d markets.", len(markets))
            
        except Exception as e:
            logger.error("Error in features job: %s", e, exc_info=True)
            
        # Sleep for 5 minutes (300 seconds)
        await asyncio.sleep(300)