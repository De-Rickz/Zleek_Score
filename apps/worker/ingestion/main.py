from dotenv import load_dotenv
import asyncpg
import asyncio
import os
from writer import insert_trades_and_books, upsert_markets
from clob_ws import subscribe
import gamma_client
import logging
import sys

load_dotenv(".env.local")
uri = os.getenv("DATABASE_URL")
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
            raw = gamma_client.fetch_markets()  # sync HTTP call
            logger.info("Fetched %d raw markets from Gamma", len(raw))
            
            markets = gamma_client.parse_json(raw)
            logger.info("Parsed %d markets from Gamma response", len(markets))
            
            # 2) Upsert into DB using the same pool
            await upsert_markets(pool, markets)
            logger.info("Successfully upserted markets into database")
            
        except Exception as e:
            logger.exception("Gamma refresh failed: %s", e)
        
        # 3) Sleep before next refresh
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
    queue = asyncio.Queue(maxsize=5000)
    
    async with pool:
        logger.info("Database pool created successfully")
        
        # Create all tasks
        writer_task = asyncio.create_task(
            insert_trades_and_books(pool, queue),
            name="writer"
        )
        clob_task = asyncio.create_task(
            subscribe(queue, pool),
            name="clob_subscriber"
        )
        gamma_task = asyncio.create_task(
            refresh_markets_periodically(pool, interval=600),
            name="gamma_refresher"
        )
        
        logger.info("All tasks started. Running indefinitely...")
        
        # Wait for any task to complete (or crash)
        # In practice, these tasks run forever unless there's an unhandled error
        done, pending = await asyncio.wait(
            [writer_task, clob_task, gamma_task],
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