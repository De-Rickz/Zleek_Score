"""
Worker heartbeat, run every 15 min: by launchd on the Mac (com.zleek.heartbeat),
by cron in a one-off container on the production server (docs/parallel-run.md).

Three checks, read-only via the zleek_web role (the frontend's DATABASE_URL in
apps/web/.env.local); each sends its own alert when stale:

  signals  newest signals_latest.ts: the scoring loop is alive.
  trades   newest LIVE websocket trade: data is actually flowing. Needed
           because the tremor job re-stamps every market each run even with
           no new data, so a dead websocket in an otherwise running worker
           never makes signals stale (see the clob_ws keepalive item in
           infra/TODO.md). Backfill rows are excluded because they keep
           arriving while the websocket is dead.
  backfill newest BACKFILLED trade (raw is the Data API object), within 2 h
           (HEARTBEAT_BACKFILL_STALE_MIN). Added 2026-11-06 after Data API v1
           was retired: every backfill request failed for days and nothing
           noticed, because the websocket kept "trades" fresh. Looks back
           at most a day (cheap with the trades(ts) index); no backfill trade
           in that day reports as none.

  archive  once trades_archive exists (ROADMAP phase 4 storage,
           docs/trades-archive-design.md): the nightly move keeps trades to
           the last 4-5 UTC days, so a row in trades older than 5 days means
           the move job isn't running; and no tx_hash may be in both tables
           near the cutoff, nor twice in the newest archived day (the
           duplicate guard: a mismatch means a write slipped past
           zleek_store_trade's dedupe). Not checked (nor logged) before the
           migration. Each query is indexed and bounded to ~1 day of rows.
After the Mac sleeps both checks fail together on wake; that is the worker
being suspended, not a websocket fault.

Every run writes one short line: to HEARTBEAT_LOG (a path, or "-" for stdout),
default apps/worker/ingestion/heartbeat.log.

Alerting, configured by environment (all optional):
  DATABASE_URL     read-only connection string. Default: apps/web/.env.local (Mac).
  NTFY_URL         e.g. https://ntfy.sh/<long-random-topic>. When set, each alert
                   is pushed there (phone). Without it: macOS notification on a
                   Mac, stderr elsewhere.
  NTFY_TOKEN       bearer token, for an access-controlled topic.
  HEALTHCHECK_URL  e.g. https://hc-ping.com/<uuid>. Every run pings it: <url>
                   when OK, <url>/fail when a check fails, with the log line as
                   the body. healthchecks.io then alerts on a FAIL ping AND
                   when no ping arrives at all (server down, cron stopped,
                   heartbeat crashed): the failures this script can't report
                   itself.
Alert delivery failures are written to stderr and never stop the run.
"""
import asyncio
import os
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

import asyncpg

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
ENV_FILE = os.path.join(ROOT, "apps", "web", ".env.local")
LOG_FILE = os.environ.get("HEARTBEAT_LOG") or os.path.join(ROOT, "apps", "worker", "ingestion", "heartbeat.log")
STALE_AFTER = timedelta(minutes=15)
BACKFILL_STALE_AFTER = timedelta(minutes=int(os.environ.get("HEARTBEAT_BACKFILL_STALE_MIN", "120")))
HTTP_TIMEOUT_SEC = 10


def database_url():
    if os.environ.get("DATABASE_URL"):
        return os.environ["DATABASE_URL"]
    with open(ENV_FILE) as f:
        for line in f:
            if line.startswith("DATABASE_URL="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError(f"DATABASE_URL not found in {ENV_FILE}")


async def newest_timestamps():
    conn = await asyncpg.connect(database_url(), timeout=10, server_settings={
        "default_transaction_read_only": "on", "statement_timeout": "5000"})
    try:
        signals = await conn.fetchval("SELECT MAX(ts) FROM signals_latest")
        # Live trades store the raw websocket message as a JSON string;
        # backfilled trades store the Data API object.
        trades = await conn.fetchval(
            "SELECT MAX(ts) FROM trades WHERE jsonb_typeof(raw) = 'string'")
        backfill = await conn.fetchval(
            "SELECT MAX(ts) FROM trades WHERE ts > now() - interval '1 day' AND jsonb_typeof(raw) = 'object'")
        return signals, trades, backfill, await archive_state(conn)
    finally:
        await conn.close()


async def archive_state(conn):
    """None before the migration; else (oldest ts in trades, tx_hashes in both
    tables near the cutoff, duplicate tx_hashes in the newest archived day)."""
    if not await conn.fetchval("SELECT to_regclass('public.trades_archive') IS NOT NULL"):
        return None
    oldest = await conn.fetchval("SELECT min(ts) FROM trades")
    # Where both can happen: the same trade on both sides of the cutoff (its
    # websocket and Data API timestamps differ by up to ~5 s), so an hour
    # either side is enough, and keeps the query small.
    both = await conn.fetchval(
        """SELECT count(*) FROM trades t
           WHERE t.ts >= archive_cutoff() - interval '1 hour' AND t.ts < archive_cutoff() + interval '1 hour'
             AND EXISTS (SELECT 1 FROM trades_archive a WHERE a.tx_hash = t.tx_hash
                         AND a.ts >= archive_cutoff() - interval '1 hour')""")
    dupes = await conn.fetchval(
        """SELECT count(*) - count(DISTINCT tx_hash) FROM trades_archive
           WHERE ts >= archive_cutoff() - interval '1 day' AND tx_hash IS NOT NULL""")
    return oldest, both, dupes



def _http(url, data=None, headers=None):
    """POST (with data) or GET; delivery problems go to stderr, never raise."""
    try:
        req = urllib.request.Request(url, data=data, headers=headers or {}, method="POST" if data is not None else "GET")
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as resp:
            resp.read()
    except Exception as e:
        print(f"heartbeat: could not reach {url.split('?')[0]}: {type(e).__name__}: {e}", file=sys.stderr)


def notify(message):
    ntfy = os.environ.get("NTFY_URL")
    if ntfy:
        headers = {"Title": "Zleek Score", "Priority": "high", "Tags": "warning"}
        if os.environ.get("NTFY_TOKEN"):
            headers["Authorization"] = f"Bearer {os.environ['NTFY_TOKEN']}"
        _http(ntfy, data=message.encode(), headers=headers)
    elif sys.platform == "darwin":
        # Message passed as an argument, not spliced into the AppleScript source.
        subprocess.run(["osascript",
                        "-e", "on run argv",
                        "-e", 'display notification (item 1 of argv) with title "Zleek Score"',
                        "-e", "end run", message], check=False, timeout=10)
    else:
        print(f"heartbeat ALERT (no NTFY_URL set): {message}", file=sys.stderr)


def ping_healthcheck(ok, line):
    """Dead-man's switch: <url> on OK, <url>/fail on FAIL, log line as body."""
    url = os.environ.get("HEALTHCHECK_URL")
    if url:
        _http(url.rstrip("/") + ("" if ok else "/fail"), data=line.encode())


def log(status, detail):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    line = f"{now} {status} {detail}"
    if LOG_FILE == "-":
        print(line, flush=True)
    else:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    return line


CHECKS = {
    # name: (message when stale, message when no rows at all)
    "signals": ("Zleek Score worker may be down — no fresh signals in {m}min",
                "Zleek Score worker may be down — signals_latest is empty"),
    "trades": ("No fresh trade data in {m}min (scoring job may be masking an upstream failure)",
               "No live trade data at all (scoring job may be masking an upstream failure)"),
    "backfill": ("Backfill has stored no trades in {m}min: Data API requests may be failing "
                 "(worker logs: 'Backfill error')",
                 "Backfill has stored no trades in over a day: Data API requests may be failing "
                 "(worker logs: 'Backfill error')"),
}


ARCHIVE_MAX_AGE = timedelta(days=5)   # trades holds 4-5 UTC days when the nightly move runs


def archive_problems(state, now):
    """Alert messages for the archive check (empty = fine), and its log part."""
    if state is None:
        return [], ""   # before the migration: nothing to check, nothing logged
    oldest, both, dupes = state
    problems = []
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if oldest is not None and oldest < day_start - ARCHIVE_MAX_AGE:
        problems.append(f"trades still holds rows from {oldest:%Y-%m-%d}: the nightly archive move "
                        "(archive_trades_job) may not be running")
    if both:
        problems.append(f"{both} trade(s) are in both trades and trades_archive near the cutoff")
    if dupes:
        problems.append(f"{dupes} duplicate tx_hash(es) in the newest archived day")
    age = "-" if oldest is None else f"{(now - oldest).days}d"
    return problems, f"archive={'!' if problems else ''}oldest{age},both{both},dupes{dupes}"


def stale_after(name):
    """Threshold per check; read at run time so tests can override it."""
    return BACKFILL_STALE_AFTER if name == "backfill" else STALE_AFTER


def main():
    try:
        values = asyncio.run(newest_timestamps())
        newest = dict(zip(CHECKS, values[:len(CHECKS)]))
        archive = values[len(CHECKS)] if len(values) > len(CHECKS) else None
    except Exception as e:
        line = log("FAIL", f"db_error={type(e).__name__}")
        notify(f"Zleek Score heartbeat can't reach the database ({type(e).__name__})")
        ping_healthcheck(False, line)
        return

    now = datetime.now(timezone.utc)
    parts, failed = [], False
    for name, (stale_msg, empty_msg) in CHECKS.items():
        ts = newest[name]
        if ts is None:
            parts.append(f"{name}=none")
            notify(empty_msg)
            failed = True
            continue
        gap = now - ts
        gap_min = int(gap.total_seconds() // 60)
        if gap > stale_after(name):
            parts.append(f"{name}={gap_min}m!")
            notify(stale_msg.format(m=gap_min))
            failed = True
        else:
            parts.append(f"{name}={gap_min}m")
    problems, part = archive_problems(archive, now)
    if part:
        parts.append(part)
    for p in problems:
        notify(f"Zleek Score archive: {p}")
        failed = True
    # e.g. "OK signals=3m trades=0m backfill=4m archive=oldest4d,both0,dupes0"
    line = log("FAIL" if failed else "OK", " ".join(parts))
    ping_healthcheck(not failed, line)


if __name__ == "__main__":
    main()
