import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger("Signals.Tremor")

async def calculate_tremor_for_market(conn, market_id: str):
    """
    Fetches the latest scientific features and converts them 
    into a user-friendly 0-100 Tremor Score.
    """
    # 1. Get the most recent features for this market
    query = """
        SELECT bucket, price_z_score, ob_imbalance 
        FROM features_5m 
        WHERE market_id = $1 
        ORDER BY bucket DESC 
        LIMIT 1
    """
    latest = await conn.fetchrow(query, market_id)
    if not latest:
        return

    z_score = float(latest["price_z_score"] or 0)
    imbalance = float(latest["ob_imbalance"] or 0)
    
    # ---------------------------------------------------------
    # 🌍 THE TREMOR ALGORITHM
    # ---------------------------------------------------------
    # A Z-score of 3.0 is a massive move. Let's make that worth 50 points.
    # We use abs() because a massive crash (-3.0) is just as big of a tremor as a massive pump (+3.0).
    z_points = min(abs(z_score) * (50 / 3.0), 50) 
    
    # An imbalance of 1.0 (or -1.0) is a totally one-sided order book. Worth 50 points.
    imb_points = min(abs(imbalance) * 50, 50)
    
    # Combine them for the final 0-100 score!
    tremor_score = round(z_points + imb_points, 2)
    
    # Clamp it between 0 and 100 just to be mathematically safe
    tremor_score = max(0.0, min(100.0, tremor_score))

    # ---------------------------------------------------------
    # 💾 SAVE THE DATA (History + Latest)
    # ---------------------------------------------------------
    now = datetime.now(timezone.utc)
    
    # Insert into the Historical Vault
    await conn.execute("""
        INSERT INTO signals_history (market_id, ts, tremor_score)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
    """, market_id, now, tremor_score)

    # Upsert into the "Dashboard Snapshot" table (signals_latest)
    await conn.execute("""
        INSERT INTO signals_latest (market_id, ts, tremor_score)
        VALUES ($1, $2, $3)
        ON CONFLICT (market_id) 
        DO UPDATE SET 
            ts = EXCLUDED.ts,
            tremor_score = EXCLUDED.tremor_score
    """, market_id, now, tremor_score)

    if tremor_score > 50:
        logger.warning(f"🚨 [TREMOR DETECTED] {market_id} | Score: {tremor_score}/100")


async def run_tremor_job(pool):
    """Wakes up every 5 minutes (offset slightly) to calculate Tremor scores."""
    logger.info("Starting 5-minute Tremor Engine...")
    
    # Sleep for 30 seconds at boot so features.py has a head-start to write its data first
    await asyncio.sleep(30)
    
    while True:
        try:
            async with pool.acquire() as conn:
                # Find markets that have features calculated
                markets = await conn.fetch("SELECT DISTINCT market_id FROM features_5m")
                for m in markets:
                    await calculate_tremor_for_market(conn, m["market_id"])
                    
            logger.info("Calculated Tremor scores for %d markets.", len(markets))
        except Exception as e:
            logger.error("Error in tremor job: %s", e, exc_info=True)
            
        await asyncio.sleep(300)