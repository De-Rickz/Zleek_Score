"""
Tests for the in-memory fallback buffer in EliteZleekWriter (T1): events are
kept while Redis is down and reach the queue, in order, once it answers again.
In-memory fake only (no Redis, no Postgres).

    cd apps/worker && python tests/test_redis_fallback.py
"""
import asyncio
import json
import logging
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import writer as w


class FlakyRedis:
    def __init__(self):
        self.lists = defaultdict(list)  # index 0 = head
        self.down = False

    async def lpush(self, key, *values):
        if self.down:
            raise ConnectionError("redis is down")
        for v in values:
            self.lists[key].insert(0, v)


def trade(i):
    return {"kind": "trade", "tx_hash": f"0x{i}", "market_id": "m", "price": 0.5}


def queued_ids(redis, key):
    """Item numbers in processing order (flush reads the tail first)."""
    return [json.loads(x)["tx_hash"] for x in reversed(redis.lists[key])]


async def main():
    REST = "buffer:trades:rest"

    # (c) normal operation unchanged: straight LPUSH, nothing buffered
    redis = FlakyRedis()
    wr = w.EliteZleekWriter(None, redis)
    for i in range(3):
        await wr.add_item(trade(i))
    assert queued_ids(redis, REST) == ["0x0", "0x1", "0x2"] and not wr._fallback
    await wr.add_item({"kind": "other"})
    assert not wr._fallback
    print("ok  1 normal operation: items pushed in order, buffer empty")

    # (a) outage: items buffered, nothing raised, order kept after recovery
    redis = FlakyRedis()
    wr = w.EliteZleekWriter(None, redis)
    await wr.add_item(trade(0))
    redis.down = True
    for i in range(1, 4):
        await wr.add_item(trade(i))
    assert len(wr._fallback) == 3 and queued_ids(redis, REST) == ["0x0"]
    assert await wr.drain_fallback() == 0 and len(wr._fallback) == 3  # still down
    redis.down = False
    await wr.add_item(trade(4))  # a new item must not jump ahead of the buffered ones
    assert not wr._fallback
    assert queued_ids(redis, REST) == [f"0x{i}" for i in range(5)], queued_ids(redis, REST)
    print("ok  2 outage: 3 events kept, drained in order on recovery, none lost")

    # drain by the flush loop's tick (no new item arrives)
    redis.down = True
    await wr.add_item(trade(5))
    redis.down = False
    assert await wr.drain_fallback() == 1
    assert queued_ids(redis, REST)[-1] == "0x5" and not wr._fallback
    print("ok  3 drain_fallback (flush loop tick) empties the buffer")

    # (b) bound holds, oldest dropped first, count logged
    redis = FlakyRedis()
    wr = w.EliteZleekWriter(None, redis)
    wr.fallback_max = 5
    redis.down = True
    records = []

    class Capture(logging.Handler):
        def emit(self, r):
            records.append(r)
    handler = Capture(level=logging.ERROR)
    logging.getLogger("Ingestion.Writer").addHandler(handler)
    try:
        for i in range(8):
            await wr.add_item(trade(i))
    finally:
        logging.getLogger("Ingestion.Writer").removeHandler(handler)
    assert len(wr._fallback) == 5 and wr.fallback_dropped == 3
    msgs = [r.getMessage() for r in records]
    assert msgs and "dropped 1 oldest" in msgs[0], msgs
    redis.down = False
    await wr.drain_fallback()
    assert queued_ids(redis, REST) == [f"0x{i}" for i in range(3, 8)]
    print(f"ok  4 bound holds: 8 events into 5 slots, 3 oldest dropped and counted: \"{msgs[0]}\"")

    # recovery that fails midway keeps the rest buffered, in order
    redis = FlakyRedis()
    wr = w.EliteZleekWriter(None, redis)
    redis.down = True
    for i in range(3):
        await wr.add_item(trade(i))
    real = redis.lpush
    calls = {"n": 0}

    async def flaky(key, *values):
        calls["n"] += 1
        if calls["n"] == 2:
            raise ConnectionError("blip")
        return await real(key, *values)
    redis.lpush = flaky
    redis.down = False
    assert await wr.drain_fallback() == 1 and len(wr._fallback) == 2
    assert await wr.drain_fallback() == 2
    assert queued_ids(redis, REST) == ["0x0", "0x1", "0x2"]
    print("ok  5 Redis fails again mid-drain: remaining items stay buffered, order kept")

    print("all redis fallback tests passed")


if __name__ == "__main__":
    asyncio.run(main())
