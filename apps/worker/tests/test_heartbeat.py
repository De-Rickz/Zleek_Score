"""
Tests for heartbeat.py alerting (ntfy push + healthchecks.io dead-man's switch).

A local HTTP server stands in for both services and records every request.
The database checks run read-only against DATABASE_URL (a read-only role,
e.g. zleek_web); nothing is written anywhere.

    cd apps/worker && DATABASE_URL=... .venv/bin/python tests/test_heartbeat.py
"""
import contextlib
import io
import os
import subprocess
import sys
import threading
from datetime import timedelta
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


def run(env, stale_after=None, platform=None):
    """Run heartbeat.main() with the given env; returns (stdout, stderr, requests)."""
    REQUESTS.clear()
    saved = {k: os.environ.get(k) for k in ("NTFY_URL", "NTFY_TOKEN", "HEALTHCHECK_URL", "DATABASE_URL")}
    os.environ.update({k: v for k, v in env.items() if v is not None})
    for k, v in env.items():
        if v is None:
            os.environ.pop(k, None)
    orig_stale, orig_platform, orig_run = hb.STALE_AFTER, sys.platform, subprocess.run
    osascript_calls = []
    if stale_after is not None:
        hb.STALE_AFTER = stale_after
    if platform:
        sys.platform = platform
    hb.subprocess.run = lambda args, **kw: osascript_calls.append(args[-1])
    out, err = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            hb.main()
    finally:
        hb.STALE_AFTER, sys.platform, hb.subprocess.run = orig_stale, orig_platform, orig_run
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
    db = os.environ["DATABASE_URL"]
    NTFY, HC = f"{base}/zleek-test-topic", f"{base}/ping/0000-uuid"

    # 1) healthy: one OK ping to healthchecks, no push
    out, err, reqs, _ = run({"DATABASE_URL": db, "NTFY_URL": NTFY, "HEALTHCHECK_URL": HC})
    assert " OK signals=" in out, out
    assert [(r["method"], r["path"]) for r in reqs] == [("POST", "/ping/0000-uuid")], reqs
    assert reqs[0]["body"].strip() == out.strip()
    print(f"ok  1 healthy: log '{out.strip()}'; 1 healthchecks ping (OK), 0 pushes")

    # 2) both checks stale: two pushes to ntfy with headers, then a /fail ping
    out, err, reqs, _ = run({"DATABASE_URL": db, "NTFY_URL": NTFY, "NTFY_TOKEN": "tk_test",
                             "HEALTHCHECK_URL": HC}, stale_after=timedelta(0))
    pushes = [r for r in reqs if r["path"] == "/zleek-test-topic"]
    fails = [r for r in reqs if r["path"] == "/ping/0000-uuid/fail"]
    assert len(pushes) == 2 and len(fails) == 1 and len(reqs) == 3, reqs
    assert pushes[0]["headers"]["Title"] == "Zleek Score" and pushes[0]["headers"]["Authorization"] == "Bearer tk_test"
    assert "no fresh signals" in pushes[0]["body"] and "No fresh trade data" in pushes[1]["body"]
    assert fails[0]["body"].strip().endswith("!") and " FAIL " in fails[0]["body"]
    print(f"ok  2 stale: 2 ntfy pushes (\"{pushes[0]['body']}\" / \"{pushes[1]['body'][:40]}...\"), "
          f"token sent as Bearer, 1 /fail ping with the log line")

    # 3) database unreachable: push + /fail
    out, err, reqs, _ = run({"DATABASE_URL": "postgresql://x:y@127.0.0.1:1/none", "NTFY_URL": NTFY, "HEALTHCHECK_URL": HC})
    assert "FAIL db_error=" in out and [r["path"] for r in reqs] == ["/zleek-test-topic", "/ping/0000-uuid/fail"], reqs
    print(f"ok  3 database unreachable: push \"{reqs[0]['body']}\" + /fail ping")

    # 4) ntfy unreachable: error on stderr, run completes, healthchecks still pinged
    out, err, reqs, _ = run({"DATABASE_URL": db, "NTFY_URL": "http://127.0.0.1:1/topic", "HEALTHCHECK_URL": HC},
                            stale_after=timedelta(0))
    assert "could not reach http://127.0.0.1:1/topic" in err and [r["path"] for r in reqs] == ["/ping/0000-uuid/fail"]
    print("ok  4 ntfy down: delivery error on stderr, run completes, /fail ping still sent")

    # 5) Mac without NTFY_URL: unchanged behaviour (macOS notification), no HTTP at all
    out, err, reqs, osa = run({"DATABASE_URL": db, "NTFY_URL": None, "HEALTHCHECK_URL": None},
                              stale_after=timedelta(0), platform="darwin")
    assert len(osa) == 2 and not reqs, (osa, reqs)
    print("ok  5 Mac, no env vars: 2 macOS notifications, no HTTP requests (as before)")

    # 6) Linux without NTFY_URL: alert text to stderr instead of osascript
    out, err, reqs, osa = run({"DATABASE_URL": db, "NTFY_URL": None, "HEALTHCHECK_URL": None},
                              stale_after=timedelta(0), platform="linux")
    assert not osa and err.count("heartbeat ALERT (no NTFY_URL set)") == 2, err
    print("ok  6 Linux, no NTFY_URL: alerts go to stderr, osascript never called")
    server.shutdown()
    print("all heartbeat tests passed")


if __name__ == "__main__":
    main()
