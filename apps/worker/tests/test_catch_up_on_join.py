"""
Tests for catch-up on join (ROADMAP phase 4 step 3): when the websocket
subscribes to tokens it wasn't subscribed to, fetch those markets' last hour
from the Data API about 2 minutes later, in the background. Offline: the
recorded v2 pages (tests/fixtures/data_api_v2_trades.json), a fake pool and a
fake HTTP client; no real sleeping. What it does to the onset-loss share is
measured on production with scripts/recover_backfill_gap_2026-11.py
--population volume --diagnose.

    cd apps/worker && .venv/bin/python tests/test_catch_up_on_join.py
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import clob_ws
import writer as w

FX = json.load(open(os.path.join(os.path.dirname(__file__), "fixtures", "data_api_v2_trades.json")))
P1, P2 = FX["page1"], FX["page2"]
ALL = P1["data"] + P2["data"]
LAST = {"data": [], "pagination": {"next_cursor": None}}


class FakeResp:
    def __init__(self, status, body):
        self.status_code, self._body, self.headers = status, body, {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self): return self._body


class FakeClient:
    calls = []
    status = 200

    def __init__(self, *a, **kw): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *exc): return False

    async def get(self, path, params):
        FakeClient.calls.append(dict(params))
        if FakeClient.status != 200:
            return FakeResp(FakeClient.status, {})
        c = params.get("cursor")
        return FakeResp(200, P1 if c is None else P2 if c == P1["pagination"]["next_cursor"] else LAST)


class FakeConn:
    def __init__(self, conds): self.conds, self.queries = conds, []

    async def fetch(self, query, *args):
        self.queries.append((" ".join(query.split()), args))
        return [{"condition_id": c} for c in self.conds]


class FakePool:
    def __init__(self, conds): self.conn = FakeConn(conds)

    def acquire(self):
        conn = self.conn

        class Ctx:
            async def __aenter__(self_): return conn
            async def __aexit__(self_, *exc): return False
        return Ctx()


class Writer:
    def __init__(self): self.items = []
    async def add_item(self, item): self.items.append(item)


def harness(fn):
    """Fake HTTP client, asset maps and sleep; capture log records."""
    def wrapper():
        slept, records = [], []

        async def fake_sleep(sec): slept.append(sec)

        async def maps(pool): return {t["token_id"]: ("m", "YES") for t in ALL}

        class Grab(logging.Handler):
            def emit(self, record): records.append(record)
        saved = (w.httpx.AsyncClient, w.asyncio.sleep, w.load_asset_maps)
        w.httpx.AsyncClient, w.asyncio.sleep, w.load_asset_maps = FakeClient, fake_sleep, maps
        FakeClient.calls, FakeClient.status = [], 200
        h = Grab()
        w.logger.addHandler(h)
        level = w.logger.level
        w.logger.setLevel(logging.DEBUG)
        try:
            fn(slept, records)
        finally:
            w.httpx.AsyncClient, w.asyncio.sleep, w.load_asset_maps = saved
            w.logger.removeHandler(h)
            w.logger.setLevel(level)
    wrapper.__name__ = fn.__name__
    return wrapper


def test_newly_subscribed():
    assert clob_ws.newly_subscribed(set(), ["a", "b"]) == {"a", "b"}          # first connect: all
    assert clob_ws.newly_subscribed({"a", "b"}, ["b", "a"]) == set()          # reconnect, same set
    assert clob_ws.newly_subscribed({"a", "b"}, ["b", "c"]) == {"c"}          # resubscribe: only joiners


@harness
def test_waits_then_fetches_the_last_hour_and_queues(slept, records):
    joined_at = datetime.fromtimestamp(max(t["timestamp"] for t in ALL), timezone.utc)
    pool, wr = FakePool(FX["conditions"]), Writer()
    n = asyncio.run(w.catch_up_on_join(pool, wr, {"tokA"}, joined_at, lookback=timedelta(days=3650)))
    assert slept[0] == w.CATCH_UP_DELAY_SEC == 120                           # waits for the Data API first
    assert n == len(ALL) == len(wr.items) and all(i["tx_hash"] for i in wr.items)
    q, args = pool.conn.queries[0]
    assert "token_yes_id = ANY($1::text[]) OR token_no_id = ANY($1::text[])" in q and args == (["tokA"],)
    assert FakeClient.calls[0]["condition"] == ",".join(FX["conditions"])
    msg = [r for r in records if r.getMessage().startswith("Catch-up on join queued")]
    assert len(msg) == 1 and msg[0].levelname == "INFO", [r.getMessage() for r in records]


@harness
def test_cutoff_is_one_hour_before_joining(slept, records):
    newest = max(t["timestamp"] for t in ALL)
    oldest_p1 = min(t["timestamp"] for t in P1["data"])
    joined_at = datetime.fromtimestamp(newest, timezone.utc)
    lookback = timedelta(seconds=newest - oldest_p1)                         # reaches only page 1
    wr = Writer()
    asyncio.run(w.catch_up_on_join(FakePool(FX["conditions"]), wr, {"tokA"}, joined_at, lookback=lookback))
    assert {i["tx_hash"] for i in wr.items} == {t["transaction_hash"] for t in P1["data"]
                                                 if t["timestamp"] >= oldest_p1}
    assert w.CATCH_UP_LOOKBACK == timedelta(hours=1)


@harness
def test_api_error_is_a_warning_not_a_crash(slept, records):
    FakeClient.status = 500
    n = asyncio.run(w.catch_up_on_join(FakePool(FX["conditions"]), Writer(), {"tokA"},
                                       datetime.now(timezone.utc)))
    assert n == 0
    warns = [r for r in records if r.levelname == "WARNING"]
    assert any("1 of 1 requests failed" in r.getMessage() for r in warns), [r.getMessage() for r in records]
    assert not any(r.levelname == "ERROR" for r in records)


@harness
def test_database_error_is_a_warning_not_a_crash(slept, records):
    class BrokenPool:
        def acquire(self): raise RuntimeError("db down")
    n = asyncio.run(w.catch_up_on_join(BrokenPool(), Writer(), {"tokA"}, datetime.now(timezone.utc)))
    assert n == 0 and any("Catch-up on join failed: db down" in r.getMessage() for r in records)


@harness
def test_unknown_tokens_do_nothing(slept, records):
    n = asyncio.run(w.catch_up_on_join(FakePool([]), Writer(), {"tokX"}, datetime.now(timezone.utc)))
    assert n == 0 and FakeClient.calls == []


def test_scheduled_task_survives_the_connection_and_is_released():
    calls = []

    async def fake_catch_up(pool, writer, asset_ids, joined_at):
        calls.append(set(asset_ids))
        return 0
    saved = clob_ws.catch_up_on_join
    clob_ws.catch_up_on_join = fake_catch_up

    async def go():
        t = clob_ws.schedule_catch_up(None, None, {"a"})
        assert t in clob_ws.catch_up_tasks                                  # referenced while running
        await t
        await asyncio.sleep(0)
        assert t not in clob_ws.catch_up_tasks                              # released when done
    try:
        asyncio.run(go())
    finally:
        clob_ws.catch_up_on_join = saved
    assert calls == [{"a"}]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
