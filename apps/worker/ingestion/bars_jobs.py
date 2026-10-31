from dotenv import load_dotenv
import asyncpg
import asyncio
import logging
import os
from datetime import datetime, timezone,timedelta
import sys

load_dotenv(".env.local")
logger = logging.getLogger(__name__)

uri = os.getenv("DATABASE_URL")

# Rebuild a trailing window each run so trades that arrive late (the REST
# backfill fetches the last hour) still get bars. Must exceed the backfill's
# hours_back.
BARS_LOOKBACK = timedelta(minutes=65)


BARS_QUERY = """
insert into bars_1m (market_id, bucket, open, high, low, close, volume_usd, trades_cnt)
select
    t.market_id,
    date_trunc('minute', t.ts) as bucket,
    (array_agg(t.price order by t.ts asc, t.id asc))[1] as open,
    max(t.price) as high,
    min(t.price) as low,
    (array_agg(t.price order by t.ts desc, t.id desc))[1] as close,
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
    trades_cnt = excluded.trades_cnt
where (bars_1m.trades_cnt, bars_1m.volume_usd, bars_1m.close)
      is distinct from (excluded.trades_cnt, excluded.volume_usd, excluded.close);
"""


async def rebuild_bars(conn, start, end):
    """(Re)build 1-minute bars from trades with ts in [start, end). Idempotent;
    only bars whose values change are rewritten. t.id breaks timestamp ties so
    open/close are deterministic."""
    await conn.execute(BARS_QUERY, start, end)


async def run_bars_job(pool):
    logger.info("Bars Job started")
    async with pool.acquire() as conn:
         # 1) read last processed ts
        row = await conn.fetchrow("select last_ts from bars_state where id = 1")
        last_ts = row["last_ts"]
        t_now = datetime.now(timezone.utc)
        t_start = t_now - timedelta(minutes=1)

        if t_start <= last_ts:
            return  # nothing to do

        # Older last_ts (worker was down) still catches up the whole gap
        window_start = min(last_ts, t_now - BARS_LOOKBACK)
        await rebuild_bars(conn, window_start, t_start)

        await conn.execute(
            
            """
            update bars_state SET last_ts = $1 where id=1
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
