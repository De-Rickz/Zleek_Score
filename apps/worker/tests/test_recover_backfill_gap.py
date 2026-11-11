"""
Tests for scripts/recover_backfill_gap_2026-11.py (the one-off recovery of
the 2026-11-02..06 backfill gap). Offline: the recorded v2 pages from
tests/fixtures/data_api_v2_trades.json and fake database connections.

    cd apps/worker && .venv/bin/python tests/test_recover_backfill_gap.py
"""
import asyncio
import importlib.util
import json
import os
import sys

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, "..", "ingestion"))
spec = importlib.util.spec_from_file_location(
    "recover", os.path.join(HERE, "..", "scripts", "recover_backfill_gap_2026-11.py"))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
w = r.w

FX = json.load(open(os.path.join(HERE, "fixtures", "data_api_v2_trades.json")))
P1, P2 = FX["page1"], FX["page2"]
ALL = P1["data"] + P2["data"]
LAST = {"data": [], "pagination": {"next_cursor": None}}


class FakeResp:
    def __init__(self, body, status=200, headers=None):
        self._b, self.status_code, self.headers = body, status, headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self): return self._b


class FakeClient:
    """page1 -> page2 (via cursor) -> empty last page; `before` responses
    (or exceptions) are served first."""
    def __init__(self, before=()):
        self.before, self.calls = list(before), []

    async def get(self, path, params, timeout=None):
        self.calls.append(dict(params))
        assert timeout == r.REQUEST_TIMEOUT_SEC, timeout          # every request has a timeout
        if self.before:
            x = self.before.pop(0)
            if isinstance(x, Exception):
                raise x
            return x
        c = params.get("cursor")
        return FakeResp(P1 if c is None else P2 if c == P1["pagination"]["next_cursor"] else LAST)


def run(coro):
    return asyncio.run(coro)


def quiet(fn):
    """No real sleeping; fresh Stats; returns the recorded sleeps."""
    def wrapper():
        slept = []

        async def fake_sleep(sec):
            slept.append(sec)
        orig = r.asyncio.sleep
        r.asyncio.sleep = fake_sleep
        r.Stats.requests = r.Stats.retries = r.Stats.slow = 0
        r.Stats.slowest = 0.0
        try:
            fn(slept)
        finally:
            r.asyncio.sleep = orig
    wrapper.__name__ = fn.__name__
    return wrapper


def item(h, ts="2026-11-03T10:00:00+00:00"):
    return {"tx_hash": h, "ts": ts}


def test_split_new_dedupes_and_skips_stored():
    items = [item("a"), item("b"), item("a"), item("c"), {"tx_hash": None, "ts": "x"}]
    new, already = r.split_new(items, existing={"b"})
    assert [e["tx_hash"] for e in new] == ["a", "c"] and already == 1


def test_per_day():
    items = [item("a", "2026-11-03T01:00:00+00:00"), item("b", "2026-11-03T23:00:00+00:00"),
             item("c", "2026-11-04T00:00:00+00:00")]
    assert r.per_day(items) == {"2026-11-03": 2, "2026-11-04": 1}


@quiet
def test_fetch_group_maps_and_counts_unmapped(slept):
    known = ALL[0]["token_id"]
    maps = {known: ("m1", "YES")}                        # the other tokens are unknown
    fetched, unmapped, items, markets, infos = run(r.fetch_group(FakeClient(), FX["conditions"], 0, 2**40, maps))
    n_known = sum(1 for t in ALL if t["token_id"] == known)
    assert fetched == len(ALL) and len(items) == n_known and unmapped == len(ALL) - n_known
    assert markets == set(FX["conditions"]) & {t["condition_id"] for t in ALL}
    assert all(e["market_id"] == "m1" and e["tx_hash"] for e in items)
    assert infos == [{"pages": 3, "oldest": min(t["timestamp"] for t in ALL), "status": "feed ended (no older)",
                      "complete": True, "batch": 1, "markets": 2, "trades": len(ALL)}], infos


@quiet
def test_batch_reaching_the_window_start_is_complete(slept):
    start = max(t["timestamp"] for t in P2["data"]) + 1     # between page1 and page2
    trades, info = run(r.fetch_batch(FakeClient(), FX["conditions"], start, 2**40))
    assert trades == P1["data"] and info["status"] == "reached window start" and info["complete"]
    assert info["pages"] == 2 and info["oldest"] == min(t["timestamp"] for t in P2["data"])


@quiet
def test_failing_batch_is_incomplete_and_others_still_run(slept):
    class SecondBatchFails(FakeClient):
        async def get(self, path, params, timeout=None):
            if params["condition"].startswith("0xb") and "cursor" not in params:
                return FakeResp({"error": "gone"}, status=410)
            return await super().get(path, params, timeout)
    conds = [f"0xa{i:02d}" for i in range(20)] + [f"0xb{i:02d}" for i in range(5)]
    fetched, _, _, _, infos = run(r.fetch_group(SecondBatchFails(), conds, 0, 2**40, {}))
    assert [i["complete"] for i in infos] == [True, False], infos
    assert infos[1]["status"].startswith("INCOMPLETE: RuntimeError: HTTP 410") and infos[1]["trades"] == 0
    assert fetched == len(ALL)                               # batch 1's trades still counted


@quiet
def test_429_with_retry_after_then_success(slept):
    c = FakeClient(before=[FakeResp({}, 429, {"Retry-After": "7"})])
    trades, info = run(r.fetch_batch(c, FX["conditions"], 0, 2**40))
    assert info["complete"] and len(trades) == len(ALL)
    assert 7.0 in slept and r.Stats.retries == 1


@quiet
def test_timeout_is_retried_with_backoff(slept):
    c = FakeClient(before=[r.httpx.ReadTimeout("slow"), r.httpx.ReadTimeout("slow")])
    trades, info = run(r.fetch_batch(c, FX["conditions"], 0, 2**40))
    assert info["complete"] and r.Stats.retries == 2
    assert [x for x in slept if x != r.REQUEST_GAP_SEC][:2] == [2.0, 4.0], slept   # 2, 4 s backoff


@quiet
def test_retries_run_out_raise(slept):
    c = FakeClient(before=[FakeResp({}, 429)] * r.MAX_ATTEMPTS)
    try:
        run(r.fetch_batch(c, FX["conditions"], 0, 2**40))
    except RuntimeError as e:
        assert "429" in str(e)
    else:
        raise AssertionError("a 429 on every attempt must end the batch as an error")
    assert r.Stats.retries == r.MAX_ATTEMPTS - 1


@quiet
def test_slow_requests_are_logged(slept):
    import contextlib, io
    orig = r.SLOW_REQUEST_SEC
    r.SLOW_REQUEST_SEC = -1                                  # every request counts as slow
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            run(r.fetch_batch(FakeClient(), FX["conditions"], 0, 2**40))
    finally:
        r.SLOW_REQUEST_SEC = orig
    assert r.Stats.slow == r.Stats.requests == 3 and out.getvalue().count("slow request:") == 3


class FakeConn:
    def __init__(self):
        self.batches, self.in_tx = [], 0

    def transaction(self):
        conn = self

        class Tx:
            async def __aenter__(self_):
                conn.in_tx += 1

            async def __aexit__(self_, *exc):
                conn.in_tx -= 1
                return False
        return Tx()

    async def executemany(self, query, rows):
        assert self.in_tx == 1, "inserts must run inside a transaction"
        self.batches.append((query, rows))


def test_insert_new_uses_rest_query_in_batches():
    maps = {t["token_id"]: ("m", "YES") for t in ALL}
    col = r.Collector()
    run(w.queue_public_trades(col, ALL, maps))
    items = col.items * 300                                # 2,400 rows -> 1000 + 1000 + 400
    conn = FakeConn()
    run(r.insert_new(conn, items))
    assert [len(rows) for _, rows in conn.batches] == [1000, 1000, 400]
    q = w.EliteZleekWriter(db_pool=None, redis_client=None).REST_QUERY
    assert all(query == q for query, _ in conn.batches)    # the live upsert, with the 2a guard
    row = conn.batches[0][1][0]
    assert len(row) == 13 and row[8] == ALL[0]["transaction_hash"]   # _map_rest's shape


def test_stored_hashes_looks_up_in_chunks():
    class LookupConn:
        def __init__(self): self.calls = []

        async def fetch(self, query, arr):
            self.calls.append(len(arr))
            return [{"tx_hash": h} for h in arr if h.endswith("0")]
    conn = LookupConn()
    found = run(r.stored_hashes(conn, [f"h{i}" for i in range(12000)]))
    assert conn.calls == [5000, 5000, 2000] and len(found) == 1200


def test_default_window_and_flags():
    a = r.parse_args([])
    assert r.parse_utc(a.start).isoformat() == "2026-11-02T22:20:00+00:00"
    assert r.parse_utc(a.end).isoformat() == "2026-11-06T16:05:00+00:00"
    assert not a.apply and not a.include_websocket_markets          # dry run by default
    a = r.parse_args(["--apply", "--include-websocket-markets"])
    assert a.apply and a.include_websocket_markets


def test_classify_missing_near_vs_not_followed():
    base = 1793990000                                       # stored trades at +0 and +60 s
    stored = {"m1": [base, base + 60]}
    iso = lambda s: r.datetime.fromtimestamp(s, r.timezone.utc).isoformat()
    new = [{"market_id": "m1", "ts": iso(base + 30)},                    # between stored ones: dropped
           {"market_id": "m1", "ts": iso(base + 60 + r.NEAR_SEC)},       # exactly NEAR_SEC after: near
           {"market_id": "m1", "ts": iso(base + 60 + r.NEAR_SEC + 1)},   # just beyond: not followed
           {"market_id": "m2", "ts": iso(base)}]                         # market never stored
    assert r.classify_missing(new, stored) == {"near_stored": 2, "not_followed": 1, "never_stored": 1}


def test_timeline_per_ten_minutes():
    b = 1793990400                                          # a 10-minute boundary
    assert b % 600 == 0
    assert r.timeline(missing_ts=[b + 1, b + 1300], stored_ts=[b + 5, b + 599, b + 600]) == [
        (b, 2, 1), (b + 600, 1, 0), (b + 1200, 0, 1)]


def test_onset_share():
    c1 = {"near_stored": 5, "not_followed": 10, "never_stored": 5}
    c2 = {"near_stored": 0, "not_followed": 0, "never_stored": 0}     # a group with nothing missing
    total, lost, lost_pct, near, near_pct = r.onset_share([(100, c1), (100, c2)])
    assert (total, lost, near) == (200, 15, 5) and lost_pct == 7.5 and near_pct == 2.5
    assert r.onset_share([]) == (0, 0, 0.0, 0, 0.0)


def test_population_volume_is_dry_run_only():
    assert r.parse_args([]).population == "selection"
    assert r.parse_args(["--population", "volume", "--diagnose", "5"]).population == "volume"
    import contextlib, io
    with contextlib.redirect_stderr(io.StringIO()):
        try:
            r.parse_args(["--population", "volume", "--apply"])
        except SystemExit as e:
            assert e.code == 2
        else:
            raise AssertionError("--population volume with --apply must be refused")


def test_volume_condition_ids_query():
    class Conn:
        async def fetch(self, query, exclude, limit):
            self.q, self.args = " ".join(query.split()), (exclude, limit)
            return [{"condition_id": "0x1"}, {"condition_id": "0x2"}]
    c = Conn()
    assert run(r.volume_condition_ids(c, ["tokA"])) == ["0x1", "0x2"]
    assert c.args == (["tokA"], 200)
    assert "ORDER BY MAX((raw->>'volume24hr')::float8) DESC" in c.q          # by volume, not liquidity
    assert "updated_at > now() - interval '2 hours'" in c.q and "status = 'open'" in c.q
    assert "NOT (token_yes_id = ANY($1::text[])" in c.q                        # websocket markets excluded



def _refused(argv):
    import contextlib, io
    with contextlib.redirect_stderr(io.StringIO()):
        try:
            r.parse_args(argv)
        except SystemExit as e:
            return e.code == 2
    return False


def test_markets_file_round_trip_and_flags():
    import tempfile
    d = tempfile.mkdtemp()
    p = os.path.join(d, "set.txt")
    r.write_markets_file(p, {"websocket": ["0xa", "0xb"], "backfill": ["0xc"]}, note="saved now\nrule")
    text = open(p).read()
    assert text.startswith("# measured set") and "# saved now\n# rule\n" in text
    assert r.read_markets_file(p) == {"websocket": ["0xa", "0xb"], "backfill": ["0xc"]}
    for bad in ("websocket 0xa\nbackfill 0xa\n", "other 0xa\n", "websocket\n", "# only a comment\n"):
        with open(p, "w") as f:
            f.write(bad)
        try:
            r.read_markets_file(p)
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted {bad!r}")


def test_population_day_flags():
    day = ["--population", "day", "--start", "2026-11-09T00:00Z", "--end", "2026-11-10T00:00Z"]
    a = r.parse_args(day + ["--diagnose", "5", "--also-without", "2772194"])
    assert a.population == "day" and a.also_without == ["2772194"] and a.sets_dir.endswith("measured-sets")
    assert _refused(day + ["--apply"])
    assert _refused(["--population", "day", "--start", "2026-11-09T00:00Z", "--end", "2026-11-09T12:00Z"])
    assert _refused(["--population", "day", "--start", "2026-11-09T01:00Z", "--end", "2026-11-10T01:00Z"])
    from datetime import datetime, timezone
    assert r.day_set_path("/x", datetime(2026, 11, 9, tzinfo=timezone.utc)) == "/x/2026-11-09.txt"


def test_day_groups_rank_by_usd_not_trades():
    usd = {f"0x{i:03d}": 1000.0 - i for i in range(300)}
    usd["0xbot"] = 5.0          # many penny trades, little USD
    usd["0xzero"] = 0.0
    g = r.day_groups(usd)
    assert g["websocket"] == [f"0x{i:03d}" for i in range(20)]
    assert g["backfill"] == [f"0x{i:03d}" for i in range(20, 220)]
    assert "0xbot" not in g["websocket"] + g["backfill"]
    assert "0xzero" not in r.day_groups(usd, ws=5, nxt=500)["backfill"]       # no volume: left out


def test_rank_by_day_volume_sums_usd_and_refuses_a_failed_batch():
    calls = []
    async def fake_fetch(client, batch, s, e):
        calls.append(len(batch))
        if "0xfail" in batch:
            raise RuntimeError("boom")
        return [{"condition_id": "0xa", "size": 10, "price": 0.5},
                {"condition_id": "0xa", "size": 2, "price": 0.25},
                {"condition_id": "0xb", "size": 1000, "price": 0.01}], {}
    orig, r.fetch_batch = r.fetch_batch, fake_fetch
    try:
        import contextlib, io
        with contextlib.redirect_stdout(io.StringIO()):
            usd, n = run(r.rank_by_day_volume(None, [f"0x{i}" for i in range(25)], 0, 1))
            assert calls == [20, 5]
            assert usd["0xa"] == 2 * (5.0 + 0.5) and n["0xa"] == 4 and usd["0xb"] == 2 * 10.0
            try:
                run(r.rank_by_day_volume(None, ["0xfail"], 0, 1))
            except RuntimeError as e:
                assert "nothing saved" in str(e)
            else:
                raise AssertionError("a failed ranking batch must stop the run")
    finally:
        r.fetch_batch = orig


def test_missing_share_lines_with_and_without():
    items = [{"tx_hash": "h1", "market_id": "2772194"}, {"tx_hash": "h2", "market_id": "2772194"},
             {"tx_hash": "h3", "market_id": "9"}, {"tx_hash": "h4", "market_id": "9"},
             {"tx_hash": "h4", "market_id": "9"}, {"tx_hash": None, "market_id": "9"}]
    lines = r.missing_share_lines(items, existing={"h1", "h2", "h3"}, without=["2772194"])
    assert lines == ["   missing share: 1 of 4 (25.0%)", "   missing share without 2772194: 1 of 2 (50.0%)"]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
