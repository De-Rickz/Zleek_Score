from dotenv import load_dotenv
import asyncpg
import asyncio
import os
from writer import EliteZleekWriter, upsert_markets, run_backfill_loop, run_backfill
from clob_ws import subscribe
import gamma_client
import logging
import sys
from bars_jobs import run_bars_job
import redis.asyncio as redis
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Put this somewhere near your other imports at the top
from signals.features import run_features_job
from signals.tremor import run_tremor_job

load_dotenv(".env.local")
uri = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
LOG_LEVEL = logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)
logging.getLogger().info("Boot sanity-check")

logger = logging.getLogger(__name__)


async def refresh_markets_periodically(pool, interval=600):
    """
    Periodically fetch markets from Gamma API and upsert into database.
    Runs every `interval` seconds (default 10 minutes).
    """
    logger.info("Starting Gamma refresher every %s seconds", interval)
    
    while True:
        try:
            # 1) Fetch + parse markets from Gamma
            raw = await gamma_client.fetch_markets()  # sync HTTP call
            logger.info("Fetched %d raw markets from Gamma", len(raw))
            
            markets = gamma_client.parse_markets(raw)
            logger.info("Parsed %d markets from Gamma response", len(markets))
            
            # 2) Upsert into DB using the same pool
            await upsert_markets(pool, markets)
            logger.info("Successfully upserted markets into database")
            
        except Exception as e:
            logger.exception("Gamma refresh failed: %s", e)
        
        # 3) Sleep before next refresh
        await asyncio.sleep(interval)
        
async def run_bars_loop(pool, interval=60):
    while True:
        try:
            await run_bars_job(pool)
        except Exception as e:
            logger.exception("Bars job failed: %s", e)
        await asyncio.sleep(interval)



async def main():
    """
    Main entry point: 
    - Creates database connection pool
    - Starts three concurrent tasks:
      1. Writer: Consumes from queue and writes to DB
      2. CLOB subscriber: Receives WebSocket events and enqueues them
      3. Gamma refresher: Periodically updates market metadata
    """
    logger.info("Starting main application...")
    
    # Create database connection pool
    pool = await asyncpg.create_pool(
        uri,
        min_size=2,  # Increased from 1 for better concurrency
        max_size=10,  # Increased from 5 to handle more concurrent operations
        command_timeout=60
    )
    
    # Create queue for passing events from subscriber to writer 
    
    async with pool:
        logger.info("Database pool created successfully")
        redis_client = redis.from_url(REDIS_URL)
        writer = EliteZleekWriter(pool, redis_client)
          # 1. Run large initial backfill (24 hours)
        try:
            logger.info("Running initial 24-hour backfill...")
            await run_backfill(pool, writer)
            logger.info("Initial backfill completed")
        except Exception as e:
            logger.error("Initial backfill failed: %s", e, exc_info=True)
        
        # Create all tasks
        writer_task = asyncio.create_task(
            writer.flush_loop(interval=5), 
            name="writer_flush"
        )
        clob_task = asyncio.create_task(
            subscribe(writer, pool), # We pass 'writer' instead of 'queue'
            name="clob_subscriber"
        )
        
        bars_task = asyncio.create_task(
            run_bars_loop(pool),
            name="bars_job"
        )
        gamma_task = asyncio.create_task(
            refresh_markets_periodically(pool, interval=600),
            name="gamma_refresher"
        )
        
        # Periodic small backfills every hour (just last hour)
        backfill_task = asyncio.create_task(
            run_backfill_loop(pool, writer, interval=300, hours_back=1),
            name="backfill_loop"
        )
        
        features_task = asyncio.create_task(
            run_features_job(pool),
            name="features_loop"
        )
        
        tremor_task = asyncio.create_task(
            run_tremor_job(pool),
            name="tremor_loop"
        )
        
        logger.info("All tasks started. Running indefinitely...")
        
        # Wait for any task to complete (or crash)
        # In practice, these tasks run forever unless there's an unhandled error
        done, pending = await asyncio.wait(
            [writer_task, clob_task,bars_task, gamma_task, backfill_task, features_task, tremor_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # If we get here, something crashed
        for task in done:
            task_name = task.get_name()
            try:
                task.result()  # This will raise the exception if task failed
            except Exception as e:
                logger.critical("Task %s crashed: %s", task_name, e, exc_info=True)
        
        # Cancel remaining tasks
        logger.warning("Cancelling remaining tasks...")
        for task in pending:
            task.cancel()
        
        # Wait for cancellation to complete
        await asyncio.gather(*pending, return_exceptions=True)
        3
        logger.error("Application shutting down due to task failure")
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user (Ctrl+C)")
    except Exception as e:
        logger.critical("Fatal error in main: %s", e, exc_info=True)
        sys.exit(1) 