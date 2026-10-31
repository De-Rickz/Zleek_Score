"""
Tests for EliteZleekWriter.flush (ROADMAP phase 2, item 3): items leave Redis
only after the Postgres commit, a bad row can't block later flushes, and a
warning is logged when a trade without tx_hash reaches WS_QUERY.

In-memory fakes only (no Redis, no Postgres):
  - FakeRedis follows Redis list semantics: LPUSH onto the head, LRANGE,
    LTRIM with negative indexes (start > end empties the list).
  - FakeDB applies a batch only if its transaction commits, and dedupes the
    way the real constraints do: trades by tx_hash (REST_QUERY ON CONFLICT),
    ob_snapshots by (market_id, ts) (OB_QUERY ON CONFLICT DO NOTHING).

    cd apps/worker && .venv/bin/python tests/test_flush.py
"""
import asyncio
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import asyncpg

import writer as w


class FakeRedis:
    def __init__(self):
        self.lists = defaultdict(list)  # index 0 = head
        self.fail_trim = False

    async def lpush(self, key, *values):
        for v in values:
            self.lists[key].insert(0, v.encode() if isinstance(v, str) else v)

    async def llen(self, key):
        return len(self.lists[key])

    def pipeline(self):
        return FakePipeline(self)

    def _lrange(self, key, start, stop):
        lst = self.lists[key]
        n = len(lst)
        start, stop = (start if start >= 0 else n + start), (stop if stop >= 0 else n + stop)
        return list(lst[start:stop + 1])

    def _ltrim(self, key, start, stop):
        if self.fail_trim:
            raise ConnectionError("redis went away before the trim")
        lst = self.lists[key]
        n = len(lst)
        start, stop = (start if start >= 0 else n + start), (stop if stop >= 0 else n + stop)
        self.lists[key] = lst[max(start, 0):stop + 1] if start <= stop else []


class FakePipeline:
    def __init__(self, redis):
        self.redis, self.ops = redis, []

    def lrange(self, key, start, stop):
        self.ops.append(("lrange", key, start, stop))

    def ltrim(self, key, start, stop):
        self.ops.append(("ltrim", key, start, stop))

    async def execute(self):
        out = []
        for op, key, a, b in self.ops:
            out.append(self.redis._lrange(key, a, b) if op == "lrange" else self.redis._ltrim(key, a, b))
        return out


class FakeDB:
    def __init__(self):
        self.trades = {}          # tx_hash -> row     (REST_QUERY, ON CONFLICT tx_hash)
        self.ws_trades = []       # rows                (WS_QUERY, no unique key)
        self.snapshots = {}       # (market_id, ts) -> row (OB_QUERY, ON CONFLICT DO NOTHING)
        self.down = False         # connection-level failure on every call
        self.fail_batches = 0     # fail the next N batch inserts with a connection error
        self.during_batch = None  # async hook run in the middle of a batch insert

    def apply(self, query, row):
        if "market_id" in query and row[0] == "BAD":
            raise asyncpg.exceptions.NotNullViolationError("null value in column \"ts\" violates not-null constraint")
        if "INSERT INTO trades(market_id, asset_id, ts, price, size_usd, side,\n" in query:
            self.trades.setdefault(row[8], row)
        elif "INSERT INTO trades" in query:
            self.ws_trades.append(row)
        else:
            self.snapshots.setdefault((row[0], row[1]), row)


class FakeConn:
    def __init__(self, db):
        self.db, self.pending = db, None

    def transaction(self):
        conn = self

        class Tx:
            async def __aenter__(self):
                conn.pending = []

            async def __aexit__(self, exc_type, exc, tb):
                if exc_type is None:
                    for q, r in conn.pending:
                        conn.db.apply(q, r)
                conn.pending = None
                return False
        return Tx()

    async def executemany(self, query, rows):
        if self.db.down or self.db.fail_batches > 0:
            self.db.fail_batches = max(0, self.db.fail_batches - 1)
            raise asyncpg.exceptions.ConnectionDoesNotExistError("connection was closed")
        if any(r[0] == "BAD" for r in rows):
            raise asyncpg.exceptions.NotNullViolationError("null value in column \"ts\"")
        if self.db.during_batch:
            hook, self.db.during_batch = self.db.during_batch, None
            await hook()
        self.pending.extend((query, r) for r in rows)

    async def execute(self, query, *row):  # autocommit, used by the row-by-row fallback
        if self.db.down:
            raise asyncpg.exceptions.ConnectionDoesNotExistError("connection was closed")
        self.db.apply(query, row)


class FakePool:
    def __init__(self, db):
        self.db = db

    def acquire(self):
        pool = self

        class Ctx:
            async def __aenter__(self):
                return FakeConn(pool.db)

            async def __aexit__(self, *a):
                return False
        return Ctx()


def trade(i, market="m1", tx=True):
    now = datetime.now(timezone.utc).isoformat()
    return {"kind": "trade", "market_id": market, "asset_id": "a", "ts": now, "price": 0.5,
            "size_usd": 1.0, "side": "BUY", "tx_hash": f"0x{i}" if tx else None, "raw": {}, "ingest_ts": now}


def book(market):
    now = datetime.now(timezone.utc).isoformat()
    return {"kind": "book", "market_id": market, "ts": now, "best_bid": 0.4, "best_ask": 0.6,
            "bid_depth_usd": 1, "ask_depth_usd": 1, "bids": [], "asks": [], "ingest_ts": now}


def fresh():
    db, redis = FakeDB(), FakeRedis()
    return db, redis, w.EliteZleekWriter(FakePool(db), redis)


def queued(redis, key):
    return len(redis.lists[key])


async def main():
    REST, SNAPS = "buffer:trades:rest", "buffer:snapshots"

    # 1) normal flush: everything written, queues emptied
    db, redis, wr = fresh()
    for i in range(3):
        await wr.add_item(trade(i))
    await wr.add_item(book("m1")); await wr.add_item(book("m2"))
    await wr.flush()
    assert len(db.trades) == 3 and len(db.snapshots) == 2 and queued(redis, REST) == queued(redis, SNAPS) == 0
    print("ok  1 normal flush: 3 trades + 2 snapshots written, queues empty")

    # 2) insert fails: nothing removed from Redis; the next flush writes each row once
    db, redis, wr = fresh()
    for i in range(3):
        await wr.add_item(trade(i))
    db.fail_batches = 1
    try:
        await wr.flush()
        raise AssertionError("flush should have raised")
    except asyncpg.exceptions.ConnectionDoesNotExistError:
        pass
    assert len(db.trades) == 0 and queued(redis, REST) == 3, "items lost on a failed insert"
    await wr.flush()
    assert len(db.trades) == 3 and queued(redis, REST) == 0
    print("ok  2 failed insert: 0 written, 3 still queued; retry writes 3, queue empty")

    # 3) items queued while a flush is running survive the trim
    db, redis, wr = fresh()
    for i in range(3):
        await wr.add_item(trade(i))

    async def push_two_more():
        await wr.add_item(trade(100)); await wr.add_item(trade(101))
    db.during_batch = push_two_more
    await wr.flush()
    assert set(db.trades) == {"0x0", "0x1", "0x2"} and queued(redis, REST) == 2
    await wr.flush()
    assert set(db.trades) == {"0x0", "0x1", "0x2", "0x100", "0x101"} and queued(redis, REST) == 0
    print("ok  3 items queued mid-flush: kept (2 left), written on the next flush")

    # 4) commit succeeds but the trim fails (worker dies / Redis drops): retry is harmless
    db, redis, wr = fresh()
    for i in range(3):
        await wr.add_item(trade(i))
    await wr.add_item(book("m1"))
    redis.fail_trim = True
    try:
        await wr.flush()
    except ConnectionError:
        pass
    redis.fail_trim = False
    assert len(db.trades) == 3 and queued(redis, REST) == 3  # written AND still queued
    await wr.flush()
    assert len(db.trades) == 3 and len(db.snapshots) == 1 and queued(redis, REST) == 0
    print("ok  4 commit then failed trim: rows re-sent once, deduped by tx_hash / (market_id, ts); no duplicates")

    # 5) one bad row: fails 3 batches, then good rows are written and the bad one is dead-lettered
    db, redis, wr = fresh()
    for i in range(3):
        await wr.add_item(trade(i))
    await wr.add_item(trade(99, market="BAD"))
    for attempt in range(1, w.FLUSH_MAX_BATCH_FAILURES):
        try:
            await wr.flush()
            raise AssertionError("expected failure")
        except asyncpg.exceptions.NotNullViolationError:
            assert queued(redis, REST) == 4 and len(db.trades) == 0
    await wr.flush()  # attempt FLUSH_MAX_BATCH_FAILURES: row by row
    dead = redis.lists[w.DEAD_LETTER_KEY]
    assert len(db.trades) == 3 and queued(redis, REST) == 0 and len(dead) == 1
    entry = json.loads(dead[0])
    assert entry["queue"] == REST and json.loads(entry["item"])["market_id"] == "BAD"
    assert entry["error"].startswith("NotNullViolationError")
    await wr.add_item(trade(5)); await wr.flush()
    assert len(db.trades) == 4
    print(f"ok  5 bad row: {w.FLUSH_MAX_BATCH_FAILURES - 1} failed batches keep everything; then 3 good rows "
          f"written, 1 bad row in {w.DEAD_LETTER_KEY} with its error; later flushes unblocked")

    # 6) database down for longer: the fallback must not dead-letter good rows
    db, redis, wr = fresh()
    for i in range(3):
        await wr.add_item(trade(i))
    db.down = True
    for attempt in range(w.FLUSH_MAX_BATCH_FAILURES + 2):
        try:
            await wr.flush()
        except asyncpg.exceptions.ConnectionDoesNotExistError:
            pass
    assert queued(redis, REST) == 3 and not redis.lists[w.DEAD_LETTER_KEY], "good rows dead-lettered"
    db.down = False
    await wr.flush()
    assert len(db.trades) == 3 and queued(redis, REST) == 0
    print(f"ok  6 database down for {w.FLUSH_MAX_BATCH_FAILURES + 2} flushes: nothing dead-lettered, "
          "nothing lost; all 3 written once it's back")

    # 7) a trade without tx_hash reaching WS_QUERY logs a warning
    db, redis, wr = fresh()
    records = []

    class Capture(logging.Handler):
        def emit(self, r):
            records.append(r)
    handler = Capture(level=logging.WARNING)
    logging.getLogger("Ingestion.Writer").addHandler(handler)
    try:
        await wr.add_item(trade(1, tx=False))
        await wr.flush()
    finally:
        logging.getLogger("Ingestion.Writer").removeHandler(handler)
    msgs = [r.getMessage() for r in records if r.levelno == logging.WARNING]
    assert len(db.ws_trades) == 1 and any("without a transaction hash reached WS_QUERY" in m for m in msgs), msgs
    print(f"ok  7 trade without tx_hash: written via WS_QUERY and warned: \"{msgs[0]}\"")

    print("all flush tests passed")


if __name__ == "__main__":
    asyncio.run(main())
