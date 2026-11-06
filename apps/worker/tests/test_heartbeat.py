"""
Tests for heartbeat.py alerting (ntfy push + healthchecks.io dead-man's switch).

A local HTTP server stands in for both services and records every request.
The newest timestamps are substituted per case (no database needed), except
case 3, which really connects to an unreachable database. Nothing is written
anywhere.

    cd apps/worker && .venv/bin/python tests/test_heartbeat.py
"""
import contextlib
import io
import os
import subprocess
import sys
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import heartbeat as hb

REQUESTS = []


class Recorder(BaseHTTPRequestHandler):
    def _record(self):
        n = int(self.headers.get("Content-Length") or 0)
        REQUESTS.append({"method": self.command, "path": self.path, "headers": dict(self.headers),
                         "body": self.rfile.read(n).decode() if n else ""})
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    do_GET = do_POST = _record

    def log_message(self, *a):
        pass


NOW = datetime.now(timezone.utc)
FRESH = (NOW - timedelta(minutes=3), NOW, NOW - timedelta(minutes=10))     # signals, trades, backfill


def ago(**kw):
    return NOW - timedelta(**kw)


def run(env, stale_after=None, platform=None, ts=FRESH):
    """Run heartbeat.main() with the given env and newest timestamps (None for
    ts = really query DATABASE_URL); returns (stdout, stderr, requests, osascript)."""
    REQUESTS.clear()
    saved = {k: os.environ.get(k) for k in ("NTFY_URL", "NTFY_TOKEN", "HEALTHCHECK_URL", "DATABASE_URL")}
    os.environ.update({k: v for k, v in env.items() if v is not None})
    for k, v in env.items():
        if v is None:
            os.environ.pop(k, None)
    orig_stale, orig_bf, orig_platform, orig_run = hb.STALE_AFTER, hb.BACKFILL_STALE_AFTER, sys.platform, subprocess.run
    orig_newest = hb.newest_timestamps
    osascript_calls = []
    if stale_after is not None:
        hb.STALE_AFTER = hb.BACKFILL_STALE_AFTER = stale_after
    if ts is not None:
        async def fake_newest():
            return ts
        hb.newest_timestamps = fake_newest
    if platform:
        sys.platform = platform
    hb.subprocess.run = lambda args, **kw: osascript_calls.append(args[-1])
    out, err = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            hb.main()
    finally:
        hb.STALE_AFTER, hb.BACKFILL_STALE_AFTER, sys.platform, hb.subprocess.run = orig_stale, orig_bf, orig_platform, orig_run
        hb.newest_timestamps = orig_newest
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return out.getvalue(), err.getvalue(), list(REQUESTS), osascript_calls


def main():
    server = HTTPServer(("127.0.0.1", 0), Recorder)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}"
    hb.LOG_FILE = "-"   # log line to stdout for the test
    db = "postgresql://unused@127.0.0.1:1/unused"   # timestamps are substituted except in case 3
    NTFY, HC = f"{base}/zleek-test-topic", f"{base}/ping/0000-uuid"

    # 1) healthy: one OK ping to healthchecks, no push
    out, err, reqs, _ = run({"DATABASE_URL": db, "NTFY_URL": NTFY, "HEALTHCHECK_URL": HC})
    assert " OK signals=3m trades=0m backfill=10m" in out, out
    assert [(r["method"], r["path"]) for r in reqs] == [("POST", "/ping/0000-uuid")], reqs
    assert reqs[0]["body"].strip() == out.strip()
    print(f"ok  1 healthy: log '{out.strip()}'; 1 healthchecks ping (OK), 0 pushes")

    # 2) all three stale: three pushes to ntfy with headers, then a /fail ping
    out, err, reqs, _ = run({"DATABASE_URL": db, "NTFY_URL": NTFY, "NTFY_TOKEN": "tk_test",
                             "HEALTHCHECK_URL": HC}, stale_after=timedelta(0))
    pushes = [r for r in reqs if r["path"] == "/zleek-test-topic"]
    fails = [r for r in reqs if r["path"] == "/ping/0000-uuid/fail"]
    assert len(pushes) == 3 and len(fails) == 1 and len(reqs) == 4, reqs
    assert pushes[0]["headers"]["Title"] == "Zleek Score" and pushes[0]["headers"]["Authorization"] == "Bearer tk_test"
    assert "no fresh signals" in pushes[0]["body"] and "No fresh trade data" in pushes[1]["body"]
    assert "Backfill has stored no trades" in pushes[2]["body"]
    assert fails[0]["body"].strip().endswith("!") and " FAIL " in fails[0]["body"]
    print(f"ok  2 all stale: 3 ntfy pushes, token sent as Bearer, 1 /fail ping with the log line")

    # 2b) only the backfill is stale (the 2026-11-02..06 case: websocket fine, Data API failing)
    out, err, reqs, _ = run({"DATABASE_URL": db, "NTFY_URL": NTFY, "HEALTHCHECK_URL": HC},
                            ts=(FRESH[0], FRESH[1], ago(hours=3)))
    pushes = [r for r in reqs if r["path"] == "/zleek-test-topic"]
    assert " FAIL signals=3m trades=0m backfill=180m!" in out, out
    assert len(pushes) == 1 and pushes[0]["body"].startswith("Backfill has stored no trades in 180min"), pushes
    assert [r["path"] for r in reqs][-1] == "/ping/0000-uuid/fail"
    print(f"ok  2b backfill alone stale: '{out.strip()}', 1 push (\"{pushes[0]['body'][:45]}...\"), /fail ping")

    # 2c) no backfill trade in the last day at all
    out, err, reqs, _ = run({"DATABASE_URL": db, "NTFY_URL": NTFY, "HEALTHCHECK_URL": HC},
                            ts=(FRESH[0], FRESH[1], None))
    pushes = [r for r in reqs if r["path"] == "/zleek-test-topic"]
    assert "backfill=none" in out and " FAIL " in out, out
    assert len(pushes) == 1 and "over a day" in pushes[0]["body"], pushes
    print("ok  2c no backfill trade in a day: backfill=none, 1 push, /fail ping")

    # 2d) the backfill threshold is 2 h by default, separate from the 15 min of the others
    assert hb.BACKFILL_STALE_AFTER == timedelta(minutes=120) and hb.STALE_AFTER == timedelta(minutes=15)
    out, _, reqs, _ = run({"DATABASE_URL": db, "NTFY_URL": NTFY, "HEALTHCHECK_URL": HC},
                          ts=(FRESH[0], FRESH[1], ago(minutes=119)))
    assert " OK " in out and "backfill=119m" in out and not [r for r in reqs if r["path"] == "/zleek-test-topic"], out
    out, _, reqs, _ = run({"DATABASE_URL": db, "NTFY_URL": NTFY, "HEALTHCHECK_URL": HC},
                          ts=(FRESH[0], FRESH[1], ago(minutes=121)))
    assert " FAIL " in out and "backfill=121m!" in out, out
    print("ok  2d backfill threshold: 119 min OK, 121 min FAIL (others stay at 15 min)")

    # 3) database unreachable (a real connection attempt): push + /fail
    out, err, reqs, _ = run({"DATABASE_URL": "postgresql://x:y@127.0.0.1:1/none", "NTFY_URL": NTFY,
                             "HEALTHCHECK_URL": HC}, ts=None)
    assert "FAIL db_error=" in out and [r["path"] for r in reqs] == ["/zleek-test-topic", "/ping/0000-uuid/fail"], reqs
    print(f"ok  3 database unreachable: push \"{reqs[0]['body']}\" + /fail ping")

    # 4) ntfy unreachable: error on stderr, run completes, healthchecks still pinged
    out, err, reqs, _ = run({"DATABASE_URL": db, "NTFY_URL": "http://127.0.0.1:1/topic", "HEALTHCHECK_URL": HC},
                            stale_after=timedelta(0))
    assert "could not reach http://127.0.0.1:1/topic" in err and [r["path"] for r in reqs] == ["/ping/0000-uuid/fail"]
    print("ok  4 ntfy down: delivery error on stderr, run completes, /fail ping still sent")

    # 5) Mac without NTFY_URL: macOS notification per stale check, no HTTP at all
    out, err, reqs, osa = run({"DATABASE_URL": db, "NTFY_URL": None, "HEALTHCHECK_URL": None},
                              stale_after=timedelta(0), platform="darwin")
    assert len(osa) == 3 and not reqs, (osa, reqs)
    print("ok  5 Mac, no env vars: 3 macOS notifications, no HTTP requests")

    # 6) Linux without NTFY_URL: alert text to stderr instead of osascript
    out, err, reqs, osa = run({"DATABASE_URL": db, "NTFY_URL": None, "HEALTHCHECK_URL": None},
                              stale_after=timedelta(0), platform="linux")
    assert not osa and err.count("heartbeat ALERT (no NTFY_URL set)") == 3, err
    print("ok  6 Linux, no NTFY_URL: alerts go to stderr, osascript never called")
    server.shutdown()
    print("all heartbeat tests passed")


if __name__ == "__main__":
    main()
