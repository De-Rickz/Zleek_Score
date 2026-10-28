"""
Worker heartbeat, run every 15 min by launchd (com.zleek.heartbeat).

Two checks, read-only via the zleek_web role (the frontend's DATABASE_URL in
apps/web/.env.local); each shows its own macOS notification when stale:

  signals  newest signals_latest.ts: the scoring loop is alive.
  trades   newest LIVE websocket trade: data is actually flowing. Needed
           because the tremor job re-stamps every market each run even with
           no new data, so a dead websocket in an otherwise running worker
           never makes signals stale (see the clob_ws keepalive item in
           infra/TODO.md). Backfill rows are excluded because they keep
           arriving while the websocket is dead.

After the Mac sleeps both checks fail together on wake; that is the worker
being suspended, not a websocket fault.

Every run appends one short line to apps/worker/ingestion/heartbeat.log.
"""
import asyncio
import os
import subprocess
from datetime import datetime, timedelta, timezone

import asyncpg

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
ENV_FILE = os.path.join(ROOT, "apps", "web", ".env.local")
LOG_FILE = os.path.join(ROOT, "apps", "worker", "ingestion", "heartbeat.log")
STALE_AFTER = timedelta(minutes=15)


def database_url():
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
        return signals, trades
    finally:
        await conn.close()


def notify(message):
    # Message passed as an argument, not spliced into the AppleScript source.
    subprocess.run(["osascript",
                    "-e", "on run argv",
                    "-e", 'display notification (item 1 of argv) with title "Zleek Score"',
                    "-e", "end run", message], check=False, timeout=10)


def log(status, detail):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    with open(LOG_FILE, "a") as f:
        f.write(f"{now} {status} {detail}\n")


CHECKS = {
    # name: (message when stale, message when no rows at all)
    "signals": ("Zleek Score worker may be down — no fresh signals in {m}min",
                "Zleek Score worker may be down — signals_latest is empty"),
    "trades": ("No fresh trade data in {m}min (scoring job may be masking an upstream failure)",
               "No live trade data at all (scoring job may be masking an upstream failure)"),
}


def main():
    try:
        newest = dict(zip(CHECKS, asyncio.run(newest_timestamps())))
    except Exception as e:
        log("FAIL", f"db_error={type(e).__name__}")
        notify(f"Zleek Score heartbeat can't reach the database ({type(e).__name__})")
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
        if gap > STALE_AFTER:
            parts.append(f"{name}={gap_min}m!")
            notify(stale_msg.format(m=gap_min))
            failed = True
        else:
            parts.append(f"{name}={gap_min}m")
    # e.g. "OK signals=3m trades=0m" or "FAIL signals=3m trades=42m!"
    log("FAIL" if failed else "OK", " ".join(parts))


if __name__ == "__main__":
    main()
