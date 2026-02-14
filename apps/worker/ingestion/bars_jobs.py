from dotenv import load_dotenv
import asyncpg
import asyncio
import logging
import os
import json
import gamma_client
from py_clob_client.clob_types import TradeParams
from clob_client import client
from datetime import datetime, timezone,timedelta
import pandas as pd
import sys

load_dotenv(".env.local")
logger = logging.getLogger(__name__)
MIN_LIQUIDITY_USD = 20000
MAX_MARKETS = 10
uri = os.getenv("DATABASE_URL")


async def run_bars_job(pool):
    logger.info("Bars Job started")
    async with pool.acquire() as conn:
        logger.info("Database pool created successfully")
         # 1) read last processed ts
        row = await conn.fetchrow("select last_ts from bars_state where id = 1")
        last_ts = row["last_ts"]
        t_now = datetime.now(timezone.utc)
        t_start = t_now - timedelta(minutes=1)

        if t_start <= last_ts:
            return  # nothing to do
        
        logger.info("Load asset IDs started")
        query = """ 
            insert into bars_1m as b (
                market_id,
                bucket,
                open,
                high,
                low,
                close,
                volume_usd,
                trades_cnt
            )
            select
                t.market_id,                                                                    
                date_trunc('minute', t.ts) as bucket,
                (array_agg(t.price order by t.ts asc))[1] as open,
                max(t.price) as high,
                min(t.price) as low,
                (array_agg(t.price order by t.ts desc))[1] as close,
                sum(t.size_usd) as volume_usd,
                count(*) as trades_cnt
            from trades t
            where t.ts >= $1 and t.ts < $2
            group by t.market_id, date_trunc('minute', t.ts)
            on conflict (market_id, bucket) do update
            set
                open       = excluded.open,
                high       = excluded.high,
                low        = excluded.low,
                close      = excluded.close,
                volume_usd = excluded.volume_usd,
                trades_cnt = excluded.trades_cnt;
            """
            
        await conn.execute(query, last_ts,t_start)
        
        await conn.execute(
            
            """ 
            update bars_state last_ts = $1 where id=1
            """,
            t_start
        )
    

        logger.info("Bars State updated")
    
async def main():
    logger.info("Starting main application...")
    pool = await asyncpg.create_pool(
        uri,
        min_size=2,  # Increased from 1 for better concurrency
        max_size=10,  # Increased from 5 to handle more concurrent operations
        command_timeout=60
    )
    
    await run_bars_job(pool)
        

        # later: await insert_trades(pool, trade_events)
        #        await ob_snapshot(pool, book_events)

if  __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user (Ctrl+C)")
    except Exception as e:
        logger.critical("Fatal error in main: %s", e, exc_info=True)
        sys.exit(1)        