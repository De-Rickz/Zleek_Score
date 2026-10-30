import asyncio
import logging
from datetime import datetime, timezone

from signals.features import eligible_market_ids

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

    # NULL z = fewer than Z_MIN_BARS recent bars: a quiet market, scored 0
    z_score = float(latest["price_z_score"] or 0)

    # ---------------------------------------------------------
    # 🌍 THE TREMOR ALGORITHM
    # ---------------------------------------------------------
    # Price move only, 0-100: |z| of 3.0 or more is a massive move = 100.
    # abs() because a crash (-3.0) is as big a tremor as a pump (+3.0).
    # Order-book imbalance is no longer part of the score: books only exist for
    # websocket-subscribed markets, so it is shown separately (when fresh) and
    # can rejoin once books cover every market (ROADMAP phase 4).
    tremor_score = round(min(abs(z_score) / 3.0, 1.0) * 100, 2)

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
                # Same rule as the features job: ended, stale or untraded
                # markets are not scored and drop off the dashboard.
                markets = await eligible_market_ids(conn)
                for market_id in markets:
                    await calculate_tremor_for_market(conn, market_id)
                if markets:  # never empty the dashboard on a bad eligibility read
                    removed = await conn.execute(
                        "DELETE FROM signals_latest WHERE market_id <> ALL($1::text[])", markets)
                    logger.info("Removed no-longer-eligible markets from signals_latest: %s", removed)

            logger.info("Calculated Tremor scores for %d markets.", len(markets))
        except Exception as e:
            logger.error("Error in tremor job: %s", e, exc_info=True)
            
        await asyncio.sleep(300)