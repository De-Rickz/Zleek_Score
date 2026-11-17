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
These tests pin the query so the guard can't be dropped by accident. Since
the trades_archive step the upsert lives in zleek_store_trade
(infra/sql/migrations/trades_archive.sql), which REST_QUERY calls; its
behaviour on a real database is tested by
infra/sql/migrations/test_trades_archive.sh.

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


MIGRATION = os.path.join(os.path.dirname(__file__), "..", "..", "..", "infra", "sql", "migrations",
                         "trades_archive.sql")


def store_function():
    """zleek_store_trade's body, from the migration (since the trades_archive
    step, REST_QUERY calls it; the upsert itself lives there)."""
    s = open(MIGRATION).read()
    body = s.split("CREATE OR REPLACE FUNCTION zleek_store_trade(", 1)[1].split("END $$;", 1)[0]
    return re.sub(r"\s+", " ", body)


def test_rest_query_calls_the_store_function_with_all_13_columns():
    assert rest_query() == ("SELECT zleek_store_trade($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13);"), rest_query()
    m = w.EliteZleekWriter(db_pool=None, redis_client=None)._map_rest({"ts": "2026-11-16T00:00:00+00:00"})
    assert len(m) == 13


def test_conflict_update_only_when_price_changes():
    f = store_function()
    assert ("ON CONFLICT (tx_hash) DO UPDATE SET price = EXCLUDED.price "
            "WHERE trades.price IS DISTINCT FROM EXCLUDED.price;") in f, f


def test_conflict_updates_only_the_price():
    # Only price is refreshed from a re-fetch (the Data API's fill price);
    # raw, size_usd and the rest keep the first-seen values, as before 2a.
    f = store_function()
    set_clause = f.split("DO UPDATE SET", 1)[1].split("WHERE", 1)[0].strip()
    assert set_clause == "price = EXCLUDED.price", set_clause


def test_still_dedupes_on_tx_hash():
    f = store_function()
    assert "ON CONFLICT (tx_hash)" in f
    # older than the cutoff: refused if either table already holds the tx_hash
    assert "EXISTS (SELECT 1 FROM trades WHERE tx_hash = p_tx_hash)" in f
    assert "FROM trades_archive a" in f and "a.tx_hash = p_tx_hash" in f


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
