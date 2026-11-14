"""
Tests for catch-up on entry to the backfill (ROADMAP phase 4 step 2c): a
market new to the backfill's top 200 (first run, newly ranked, back after
dropping out, or its batch failed last run) is fetched over the last 24
hours; the rest over the last hour. Offline: database lookups and the HTTP
client faked, no real sleeping. Measured on production with
scripts/recover_backfill_gap_2026-11.py --population day --diagnose.

    cd apps/worker && .venv/bin/python tests/test_catch_up_on_entry.py
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import writer as w


def run(coro):
    return asyncio.run(coro)


def backfill(conds, fail=(), entry_hours=24):
    """One run_backfill with fakes. Returns ({condition: cutoff hours back}, log lines)."""
    fetched, lines = {}, []
    now = datetime.now(timezone.utc).timestamp()

    async def asset_maps(pool): return {}
    async def asset_ids(pool): return []
    async def condition_ids(pool, **kw): return list(conds)

    async def fetch(client, batch, cutoff_unix, *a, **kw):
        if any(c in fail for c in batch):
            raise RuntimeError("HTTP 503")
        for c in batch:
            fetched[c] = round((now - cutoff_unix) / 3600)
        return []

    class Client:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *e): return False

    class Grab(logging.Handler):
        def emit(self, rec): lines.append(rec.getMessage())

    saved = (w.load_asset_maps, w.load_asset_ids, w.load_condition_ids_for_backfill,
             w.fetch_public_trades, w.httpx.AsyncClient)
    h = Grab()
    w.logger.addHandler(h)
    level = w.logger.level
    w.logger.setLevel(logging.DEBUG)
    w.load_asset_maps, w.load_asset_ids, w.load_condition_ids_for_backfill = asset_maps, asset_ids, condition_ids
    w.fetch_public_trades, w.httpx.AsyncClient = fetch, Client
    try:
        run(w.run_backfill(None, None))
    finally:
        (w.load_asset_maps, w.load_asset_ids, w.load_condition_ids_for_backfill,
         w.fetch_public_trades, w.httpx.AsyncClient) = saved
        w.logger.removeHandler(h)
        w.logger.setLevel(level)
    return fetched, lines


def test_split_new_entries():
    assert w.split_new_entries(["a", "b"], None) == (["a", "b"], [])
    assert w.split_new_entries(["a", "b", "c"], {"b", "x"}) == (["a", "c"], ["b"])


def test_first_run_fetches_24h_for_all_then_1h():
    w.backfill_prev_conds = None
    got, lines = backfill(["a", "b"])
    assert got == {"a": 24, "b": 24}
    assert any("catch-up on entry: 2 markets new" in l for l in lines)
    got, lines = backfill(["a", "b"])
    assert got == {"a": 1, "b": 1}
    assert not any("catch-up on entry" in l for l in lines)


def test_newly_ranked_and_returning_markets_get_24h():
    w.backfill_prev_conds = None
    backfill(["a", "b"])
    got, _ = backfill(["a", "c"])            # c enters, b drops out
    assert got == {"a": 1, "c": 24}
    got, _ = backfill(["a", "b", "c"])       # b comes back
    assert got == {"a": 1, "b": 24, "c": 1}


def test_failed_batch_is_retried_with_24h_next_run():
    w.backfill_prev_conds = None
    backfill(["a"])
    got, lines = backfill(["a"], fail={"a"})
    assert got == {} and any("Backfill error" in l for l in lines)
    got, _ = backfill(["a"])                 # the hour that failed is covered by the 24h fetch
    assert got == {"a": 24}


def test_new_markets_are_batched_apart_from_known_ones():
    w.backfill_prev_conds = None
    known = [f"k{i}" for i in range(25)]
    backfill(known)
    batches = []
    orig = w.condition_batches
    w.condition_batches = lambda conds, size=w.DATA_API_MAX_CONDITIONS: batches.append(list(conds)) or orig(conds, size)
    try:
        got, _ = backfill(known + ["n1", "n2"])
    finally:
        w.condition_batches = orig
    assert batches == [["n1", "n2"], known]  # entry markets fetched in their own batches
    assert got["n1"] == 24 and got["k0"] == 1 and got["k24"] == 1


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
