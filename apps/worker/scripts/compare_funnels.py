"""
Compare the Mac's and production's funnels over the same window (parallel
run, ROADMAP phase 3). Standard library only: runs anywhere with Python 3.

Inputs are the CSV exports of funnel_export_trades.sql and
funnel_export_markets.sql, made with the SAME -v t0/t1 on both machines:

    python3 compare_funnels.py mac_trades.csv prod_trades.csv mac_markets.csv prod_markets.csv

The question it answers: did production miss trades the Mac caught?

Trades are matched by tx_hash (unique in trades). A trade only the Mac has is
classified by its market and its time:
  - MISSED: production has another trade in that market within
    --missed-window-min minutes (default 10) of this one, so it was following
    the market at that moment and still lacks this trade. This is the signal
    of silently dropped data.
  - NOT FOLLOWED AT THE TIME: production has trades in that market elsewhere
    in the window, but none that close. Each worker's websocket follows the
    top 20 markets by 24h volume and re-ranks every 5 min, so the two can
    cover a quiet market at different times (2026-11-03: market 630806,
    production's trades 10:06-10:28, the Mac's one trade at 11:00).
  - NOT TRACKED: production has no trade in that market at all. Expected to
    some degree: each worker picks its websocket markets (top 20 by 24h
    volume) and backfill markets (top 200 by liquidity) from its own markets
    table, so the two sets can differ, mostly in the tail.
Exit code 1 if MISSED exceeds --max-missed-pct of the Mac's trades that
production was following at the time (default 0.1%: data correctness comes
first, and any missed trade in a followed market should be explained), else 0.
With few such trades one miss moves the percentage a lot; prefer a long
window (e.g. the whole 24 h).
"""
import argparse
import bisect
import csv
import sys
from collections import Counter
from datetime import datetime, timezone


def load_trades(path):
    by_hash, no_hash = {}, 0
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if not r["tx_hash"]:
                no_hash += 1
                continue
            by_hash[r["tx_hash"]] = r
    return by_hash, no_hash


def load_markets(path):
    with open(path, newline="") as f:
        return {r["market_id"]: r for r in csv.DictReader(f)}


def has_trade_near(times, ts, window_sec):
    """True if the sorted list `times` has a value within window_sec of ts."""
    i = bisect.bisect_left(times, ts - window_sec)
    return i < len(times) and times[i] <= ts + window_sec


def pct(a, b):
    return f"{100.0 * a / b:.2f}%" if b else "n/a"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mac_trades"); ap.add_argument("prod_trades")
    ap.add_argument("mac_markets"); ap.add_argument("prod_markets")
    ap.add_argument("--max-missed-pct", type=float, default=0.1)
    ap.add_argument("--missed-window-min", type=float, default=10.0,
                    help="a Mac-only trade counts as MISSED only if production has a trade in "
                         "the same market within this many minutes of it (default 10)")
    ap.add_argument("--top", type=int, default=10)
    a = ap.parse_args()

    mac, mac_nohash = load_trades(a.mac_trades)
    prod, prod_nohash = load_trades(a.prod_trades)
    mac_m, prod_m = load_markets(a.mac_markets), load_markets(a.prod_markets)

    prod_trade_markets = {r["market_id"] for r in prod.values()}
    mac_trade_markets = {r["market_id"] for r in mac.values()}
    both = mac.keys() & prod.keys()
    mac_only = [mac[h] for h in mac.keys() - prod.keys()]
    prod_only = [prod[h] for h in prod.keys() - mac.keys()]
    window_sec = a.missed_window_min * 60
    prod_times = {}
    for r in prod.values():
        prod_times.setdefault(r["market_id"], []).append(int(r["ts_unix"]))
    for times in prod_times.values():
        times.sort()

    def followed(r):
        # production had a trade in this market within the window of r
        times = prod_times.get(r["market_id"])
        return times is not None and has_trade_near(times, int(r["ts_unix"]), window_sec)

    missed = [r for r in mac_only if followed(r)]
    not_followed = [r for r in mac_only if r["market_id"] in prod_trade_markets and not followed(r)]
    not_tracked = [r for r in mac_only if r["market_id"] not in prod_trade_markets]
    prod_extra_tracked = [r for r in prod_only if r["market_id"] in mac_trade_markets]
    # denominator: the Mac's trades production was following at the time (a
    # matched trade counts: production has that very trade)
    mac_followed = sum(1 for h, r in mac.items() if h in prod or followed(r))

    print("== Trades (matched by tx_hash)")
    print(f"   Mac:  {len(mac):>8} trades in {len(mac_trade_markets)} markets  (+{mac_nohash} without tx_hash, not matchable)")
    print(f"   Prod: {len(prod):>8} trades in {len(prod_trade_markets)} markets  (+{prod_nohash} without tx_hash, not matchable)")
    print(f"   in both: {len(both)}   Mac only: {len(mac_only)}   Prod only: {len(prod_only)}")
    print(f"   Mac-only, market NOT TRACKED by prod: {len(not_tracked)} trades in "
          f"{len({r['market_id'] for r in not_tracked})} markets  (market selection difference)")
    print(f"   Mac-only, prod NOT FOLLOWING the market at the time (no prod trade within "
          f"{a.missed_window_min:g} min): {len(not_followed)} trades in "
          f"{len({r['market_id'] for r in not_followed})} markets  (selection timing difference)")
    print(f"   Mac-only, MISSED while prod was following the market: {len(missed)} trades = "
          f"{pct(len(missed), mac_followed)} of the Mac's {mac_followed} trades prod was following")
    print(f"   Prod-only in markets the Mac tracked: {len(prod_extra_tracked)} (the Mac missed these)")

    if missed:
        by_market = Counter(r["market_id"] for r in missed)
        src = Counter(r["source"] for r in missed)
        print(f"\n   MISSED by source on the Mac: {dict(src)}")
        print(f"   MISSED, top {a.top} markets (market: missed / Mac trades / prod trades):")
        mac_count = Counter(r["market_id"] for r in mac.values())
        prod_count = Counter(r["market_id"] for r in prod.values())
        for m, n in by_market.most_common(a.top):
            print(f"      {m}: {n} / {mac_count[m]} / {prod_count[m]}")
        hours = Counter(datetime.fromtimestamp(int(r["ts_unix"]) // 3600 * 3600, timezone.utc).strftime("%m-%d %H:00")
                        for r in missed)
        print(f"   MISSED spread over {len(hours)} hour(s); busiest (UTC hour: count): "
              f"{hours.most_common(3)}  (clustered = an outage on prod; spread = a systematic gap)")

    print("\n== Markets (per-market funnel)")
    common = mac_m.keys() & prod_m.keys()
    print(f"   markets in Mac export: {len(mac_m)}   prod: {len(prod_m)}   both: {len(common)}")
    for col in ("bars", "features", "scores", "snapshots"):
        mac_has = {m for m, r in mac_m.items() if int(r[col]) > 0}
        prod_has = {m for m, r in prod_m.items() if int(r[col]) > 0}
        print(f"   {col:>9}: Mac {len(mac_has):>4} markets, prod {len(prod_has):>4}, both {len(mac_has & prod_has):>4}, "
              f"Mac-only {len(mac_has - prod_has):>4}, prod-only {len(prod_has - mac_has):>4}")
    bar_gaps = sorted(((int(mac_m[m]["bars"]) - int(prod_m[m]["bars"]), m) for m in common
                       if int(mac_m[m]["trades"]) > 0 and int(prod_m[m]["trades"]) > 0), reverse=True)
    worse = [(d, m) for d, m in bar_gaps if d > 0]
    print(f"   markets both traded where prod has FEWER bars than the Mac: {len(worse)}"
          + (f"; largest: {worse[:a.top]}" if worse else ""))

    limit_ok = mac_followed == 0 or 100.0 * len(missed) / mac_followed <= a.max_missed_pct
    print("\n== Verdict:", "PASS" if limit_ok else "FLAG",
          f"(MISSED {pct(len(missed), mac_followed)} vs limit {a.max_missed_pct}%)")
    if mac_followed and 100.0 / mac_followed > a.max_missed_pct:
        print(f"   note: one missed trade is {pct(1, mac_followed)} of {mac_followed}; "
              f"the sample is too small for a {a.max_missed_pct}% limit (use a longer window)")
    return 0 if limit_ok else 1


if __name__ == "__main__":
    sys.exit(main())
