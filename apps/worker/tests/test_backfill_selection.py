"""
Tests for the periodic backfill's market selection (ROADMAP phase 4 step 2):
ranked by 24h volume among markets Gamma refreshed in the last 2 h, like the
websocket's, instead of by liquidity. Fake pool; what the ranking does to the
onset-loss share is measured on production with
scripts/recover_backfill_gap_2026-11.py --population volume --diagnose.

    cd apps/worker && .venv/bin/python tests/test_backfill_selection.py
"""
import asyncio
import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import writer as w


class FakeConn:
    def __init__(self, answers):
        self.answers, self.calls = list(answers), []

    async def fetch(self, query, *args):
        self.calls.append((" ".join(query.split()), args))
        return self.answers.pop(0)


class FakePool:
    def __init__(self, conn): self.conn = conn

    def acquire(self):
        conn = self.conn

        class Ctx:
            async def __aenter__(self_): return conn
            async def __aexit__(self_, *exc): return False
        return Ctx()


def run(coro):
    return asyncio.run(coro)


def test_ranked_by_volume_fresh_and_excluding_websocket():
    conn = FakeConn([[{"condition_id": "0xa"}, {"condition_id": "0xb"}]])
    got = run(w.load_condition_ids_for_backfill(FakePool(conn), exclude_asset_ids={"tokA"}))
    assert got == ["0xa", "0xb"]
    q, args = conn.calls[0]
    assert "ORDER BY MAX((raw->>'volume24hr')::float8) DESC NULLS LAST" in q, q
    assert "liquidity_usd) DESC" not in q                                  # no longer by liquidity
    assert "updated_at > now() - $4::interval" in q and "status = 'open'" in q
    assert "NOT (token_yes_id = ANY($3::text[]) OR token_no_id = ANY($3::text[]))" in q
    assert args == (10000, 200, ["tokA"], timedelta(hours=2)), args


def test_same_filters_as_the_websocket_selection():
    # the backfill must rank the same way as load_asset_ids, so that it covers
    # exactly the markets after the websocket's top 20
    conn = FakeConn([[{"token_yes_id": "y", "token_no_id": "n"}]])
    run(w.load_asset_ids(FakePool(conn)))
    ws_q = conn.calls[0][0]
    assert "ORDER BY (raw->>'volume24hr')::float8 DESC NULLS LAST" in ws_q
    assert "updated_at > now() - $3::interval" in ws_q


def test_falls_back_to_stale_rows_when_gamma_is_failing():
    conn = FakeConn([[], [{"condition_id": "0xold"}]])
    got = run(w.load_condition_ids_for_backfill(FakePool(conn)))
    assert got == ["0xold"]
    assert [c[1][3] for c in conn.calls] == [timedelta(hours=2), None]       # retried without freshness


def test_gap_backfill_call_has_no_exclusion():
    conn = FakeConn([[{"condition_id": "0xa"}]])
    run(w.load_condition_ids_for_backfill(FakePool(conn)))
    assert conn.calls[0][1][2] == []


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
