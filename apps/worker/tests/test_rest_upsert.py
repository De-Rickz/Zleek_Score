"""
Tests for REST_QUERY's conflict handling (ROADMAP phase 4 step 2a).

The periodic backfill re-fetches the last hour every 5 minutes, so each trade
reaches REST_QUERY ~12 times. The upsert must only write when the price
actually changed: an unconditional ON CONFLICT ... DO UPDATE writes a new row
version every time (dead rows, WAL, autovacuum), which step 2's larger
backfill would turn into millions of useless writes a day.

What Postgres does with the WHERE (skips the update, no new row version) can't
be shown with in-memory fakes; production's before/after check is
apps/worker/scripts/phase4_step2a_check.sql (pg_stat_user_tables counters).
These tests pin the query so the guard can't be dropped by accident.

    cd apps/worker && .venv/bin/python tests/test_rest_upsert.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import writer as w


def rest_query():
    q = w.EliteZleekWriter(db_pool=None, redis_client=None).REST_QUERY
    return re.sub(r"\s+", " ", q).strip()


def test_conflict_update_only_when_price_changes():
    q = rest_query()
    assert ("ON CONFLICT (tx_hash) DO UPDATE SET price=EXCLUDED.price "
            "WHERE trades.price IS DISTINCT FROM EXCLUDED.price;") in q, q


def test_conflict_updates_only_the_price():
    # Only price is refreshed from a re-fetch (the Data API's fill price);
    # raw, size_usd and the rest keep the first-seen values, as before 2a.
    q = rest_query()
    set_clause = q.split("DO UPDATE SET", 1)[1].split("WHERE", 1)[0].strip()
    assert set_clause == "price=EXCLUDED.price", set_clause


def test_still_dedupes_on_tx_hash():
    assert "ON CONFLICT (tx_hash)" in rest_query()


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
