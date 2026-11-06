"""
Tests for the Data API v2 backfill client (ROADMAP phase 4 step 2b): v1's
/trades was retired on 2026-11-02 (HTTP 410), so fetch_public_trades now uses
/v2/trades: condition ids in batches of up to 20, newest-first cursor paging
with the same filters re-sent, no server-side time filter (the client stops at
the window start), snake_case fields, Retry-After on 429/503.

Offline: a fake client serves two real pages recorded on 2026-11-06
(tests/fixtures/data_api_v2_trades.json, trader names and profiles removed).

    cd apps/worker && .venv/bin/python tests/test_data_api_v2.py
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import writer as w

FX = json.load(open(os.path.join(os.path.dirname(__file__), "fixtures", "data_api_v2_trades.json")))
P1, P2 = FX["page1"], FX["page2"]
ALL = P1["data"] + P2["data"]
LAST = {"data": [], "pagination": {"limit": 4, "offset": 0, "has_more": False, "next_cursor": None}}


class FakeResp:
    def __init__(self, status, body, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._body


class FakeClient:
    """Serves page1, page2 (via page1's cursor), then an empty last page."""
    def __init__(self, before=()):
        self.calls, self.before = [], list(before)

    async def get(self, path, params):
        self.calls.append((path, dict(params)))
        if self.before:                      # queued error responses first
            return self.before.pop(0)
        cursor = params.get("cursor")
        if cursor is None:
            return FakeResp(200, P1)
        if cursor == P1["pagination"]["next_cursor"]:
            return FakeResp(200, P2)
        return FakeResp(200, LAST)


def run(coro):
    return asyncio.run(coro)


def no_sleep():
    """Record sleeps instead of sleeping."""
    slept = []
    async def fake(sec):
        slept.append(sec)
    return slept, fake


def with_fake_sleep(fn):
    def wrapper():
        slept, fake = no_sleep()
        orig = w.asyncio.sleep
        w.asyncio.sleep = fake
        try:
            fn(slept)
        finally:
            w.asyncio.sleep = orig
    wrapper.__name__ = fn.__name__
    return wrapper


@with_fake_sleep
def test_pages_to_the_end_with_same_filters(slept):
    c = FakeClient()
    got = run(w.fetch_public_trades(c, FX["conditions"], cutoff_unix=0))
    assert [t["transaction_hash"] for t in got] == [t["transaction_hash"] for t in ALL]
    assert len(c.calls) == 3, c.calls                               # page1, page2, empty last page
    assert all(path == "/v2/trades" for path, _ in c.calls)
    first, second = c.calls[0][1], c.calls[1][1]
    assert first == {"condition": ",".join(FX["conditions"]), "limit": w.BACKFILL_PAGE_SIZE}, first
    assert second == {**first, "cursor": P1["pagination"]["next_cursor"]}, second   # same filters re-sent
    assert "offset" not in first and "start" not in first and "market" not in first


@with_fake_sleep
def test_stops_at_the_window_start(slept):
    oldest_p1 = min(t["timestamp"] for t in P1["data"])
    newest_p2 = max(t["timestamp"] for t in P2["data"])
    cutoff = newest_p2 + 1                    # between the pages
    assert oldest_p1 >= cutoff
    c = FakeClient()
    got = run(w.fetch_public_trades(c, FX["conditions"], cutoff_unix=cutoff))
    assert got == P1["data"], got                                    # page2 is all older: dropped
    assert len(c.calls) == 2, c.calls                                # and paging stops there


@with_fake_sleep
def test_end_bound_and_single_condition_string(slept):
    newest = max(t["timestamp"] for t in ALL)
    got = run(w.fetch_public_trades(FakeClient(), FX["conditions"][0], cutoff_unix=0, end_unix=newest - 1))
    assert got and all(t["timestamp"] <= newest - 1 for t in got)
    assert len(got) == len([t for t in ALL if t["timestamp"] <= newest - 1])


@with_fake_sleep
def test_duplicates_dropped(slept):
    class Dup(FakeClient):
        async def get(self, path, params):
            r = await super().get(path, params)
            if params.get("cursor") == P1["pagination"]["next_cursor"]:
                return FakeResp(200, {"data": P1["data"][-1:] + P2["data"], "pagination": P2["pagination"]})
            return r
    got = run(w.fetch_public_trades(Dup(), FX["conditions"], cutoff_unix=0))
    keys = [w._trade_key(t) for t in got]
    assert len(keys) == len(set(keys)) == len(ALL)


@with_fake_sleep
def test_retry_after_on_429_and_503(slept):
    c = FakeClient(before=[FakeResp(429, {}, {"Retry-After": "7"}), FakeResp(503, {}, {"Retry-After": "2"})])
    got = run(w.fetch_public_trades(c, FX["conditions"], cutoff_unix=0))
    assert len(got) == len(ALL)
    assert slept[:2] == [7.0, 2.0], slept                             # waited what the server asked


@with_fake_sleep
def test_other_errors_raise(slept):
    c = FakeClient(before=[FakeResp(410, {"error": "Data API v1 has been retired"})])
    try:
        run(w.fetch_public_trades(c, FX["conditions"], cutoff_unix=0))
    except RuntimeError as e:
        assert "410" in str(e)
    else:
        raise AssertionError("a 410 must raise, not be treated as an empty page")


def test_batches_of_twenty():
    ids = [f"0x{i:02d}" for i in range(45)]
    batches = w.condition_batches(ids)
    assert [len(b) for b in batches] == [20, 20, 5] and sum(batches, []) == ids
    try:
        run(w.fetch_public_trades(FakeClient(), ids[:21], cutoff_unix=0))
    except ValueError:
        pass
    else:
        raise AssertionError("more than 20 condition ids must be refused")


def test_queue_maps_v2_fields():
    class Writer:
        def __init__(self): self.items = []
        async def add_item(self, item): self.items.append(item)
    yes, no = P1["data"][0], next(t for t in ALL if t["token_id"] != P1["data"][0]["token_id"])
    maps = {yes["token_id"]: ("m-yes", "YES"), no["token_id"]: ("m-no", "NO")}
    wr = Writer()
    queued = run(w.queue_public_trades(wr, [yes, no], maps))
    assert queued == 2
    a, b = wr.items
    assert a["asset_id"] == yes["token_id"] and a["tx_hash"] == yes["transaction_hash"]
    assert a["price"] == yes["price"] and a["side"] == yes["side"]
    assert abs(a["size_usd"] - yes["size"] * yes["price"]) < 1e-9          # size is shares
    assert b["price"] == 1.0 - no["price"]                                   # NO token: YES terms
    assert b["side"] == {"BUY": "SELL", "SELL": "BUY"}[no["side"]]
    assert abs(b["size_usd"] - no["size"] * no["price"]) < 1e-9             # USD at the raw token price
    assert a["raw"] is yes                                                   # raw stores the v2 object


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
