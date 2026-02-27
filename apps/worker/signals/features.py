import asyncio
import logging
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger("Signals.Features")

async def calculate_features_for_market(conn, market_id: str):
    """
    Calculates the Price Z-Score and OB Imbalance for a single market
    and saves it to the features_5m table.
    """
    # ---------------------------------------------------------
    # 1. THE Z-SCORE (Rubber Band Metric)
    # ---------------------------------------------------------
    # Fetch the last 20 minutes of price bars for this market
    bars_query = """
        SELECT bucket, close 
        FROM bars_1m 
        WHERE market_id = $1 
        ORDER BY bucket DESC 
        LIMIT 20
    """
    bars = await conn.fetch(bars_query, market_id)
    
    if len(bars) < 20:
        # Not enough data to calculate a reliable 20-period moving average yet!
        return

    # Convert the SQL rows into a pandas DataFrame (our virtual spreadsheet)
    # We reverse it [::-1] so the oldest data is at the top, newest at the bottom
    df = pd.DataFrame([dict(b) for b in bars][::-1])
    
    # 🪄 PANDAS MAGIC: Calculate the rolling average and standard deviation in 2 lines!
    rolling_mean = df['close'].rolling(window=20).mean()
    rolling_std = df['close'].rolling(window=20).std()
    
    # Calculate the Z-Score: (Current - Mean) / StdDev
    # We grab the very last value (.iloc[-1]) because that's the "current" z-score
    current_close = df['close'].iloc[-1]
    current_mean = rolling_mean.iloc[-1]
    current_std = rolling_std.iloc[-1]
    
    # Prevent dividing by zero if the price hasn't moved at all in 20 minutes
    if current_std == 0 or pd.isna(current_std):
        z_score = 0.0
    else:
        z_score = (current_close - current_mean) / current_std

    # ---------------------------------------------------------
    # 2. THE ORDER BOOK IMBALANCE (Tug-of-War Metric)
    # ---------------------------------------------------------
    # Fetch the single most recent snapshot of the order book
    ob_query = """
        SELECT bid_depth_usd, ask_depth_usd 
        FROM ob_snapshots 
        WHERE market_id = $1 
        ORDER BY ts DESC 
        LIMIT 1
    """
    ob = await conn.fetchrow(ob_query, market_id)
    
    ob_imbalance = 0.0
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
    await conn.execute(insert_query, market_id, bucket, float(z_score), float(ob_imbalance))
    logger.debug(f"[{market_id}] Z-Score: {z_score:.2f} | Imbalance: {ob_imbalance:.2f}")


async def run_features_job(pool):
    """The infinite loop that wakes up every 5 minutes to run the math."""
    logger.info("Starting 5-minute Features Engine...")
    
    while True:
        try:
            async with pool.acquire() as conn:
                # Find all markets that actually have recent trades
                active_markets_query = "SELECT DISTINCT market_id FROM bars_1m"
                markets = await conn.fetch(active_markets_query)
                
                for m in markets:
                    await calculate_features_for_market(conn, m["market_id"])
                    
            logger.info("Calculated features for %d markets.", len(markets))
            
        except Exception as e:
            logger.error("Error in features job: %s", e, exc_info=True)
            
        # Sleep for 5 minutes (300 seconds)
        await asyncio.sleep(300)