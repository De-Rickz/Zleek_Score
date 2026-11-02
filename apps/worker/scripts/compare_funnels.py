"""
Compare the Mac's and production's funnels over the same window (parallel
run, ROADMAP phase 3). Standard library only: runs anywhere with Python 3.

Inputs are the CSV exports of funnel_export_trades.sql and
funnel_export_markets.sql, made with the SAME -v t0/t1 on both machines:

    python3 compare_funnels.py mac_trades.csv prod_trades.csv mac_markets.csv prod_markets.csv

The question it answers: did production miss trades the Mac caught?

Trades are matched by tx_hash (unique in trades). A trade only the Mac has is
classified by its market:
  - MISSED: production has other trades in that market in the window, so it
    was tracking the market and still lacks this trade. This is the signal of
    silently dropped data.
  - NOT TRACKED: production has no trade in that market at all. Expected to
    some degree: each worker picks its websocket markets (top 20 by 24h
    volume) and backfill markets (top 200 by liquidity) from its own markets
    table, so the two sets can differ, mostly in the tail.
Exit code 1 if MISSED exceeds --max-missed-pct of the Mac's trades in markets
production tracked (default 0.5%), else 0.
"""
import argparse
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


def pct(a, b):
    return f"{100.0 * a / b:.2f}%" if b else "n/a"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mac_trades"); ap.add_argument("prod_trades")
    ap.add_argument("mac_markets"); ap.add_argument("prod_markets")
    ap.add_argument("--max-missed-pct", type=float, default=0.5)
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
    missed = [r for r in mac_only if r["market_id"] in prod_trade_markets]
    not_tracked = [r for r in mac_only if r["market_id"] not in prod_trade_markets]
    prod_extra_tracked = [r for r in prod_only if r["market_id"] in mac_trade_markets]
    mac_in_prod_markets = sum(1 for r in mac.values() if r["market_id"] in prod_trade_markets)

    print("== Trades (matched by tx_hash)")
    print(f"   Mac:  {len(mac):>8} trades in {len(mac_trade_markets)} markets  (+{mac_nohash} without tx_hash, not matchable)")
    print(f"   Prod: {len(prod):>8} trades in {len(prod_trade_markets)} markets  (+{prod_nohash} without tx_hash, not matchable)")
    print(f"   in both: {len(both)}   Mac only: {len(mac_only)}   Prod only: {len(prod_only)}")
    print(f"   Mac-only, market NOT TRACKED by prod: {len(not_tracked)} trades in "
          f"{len({r['market_id'] for r in not_tracked})} markets  (market selection difference)")
    print(f"   Mac-only, MISSED in markets prod tracked: {len(missed)} trades = "
          f"{pct(len(missed), mac_in_prod_markets)} of the Mac's {mac_in_prod_markets} trades in those markets")
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

    limit_ok = mac_in_prod_markets == 0 or 100.0 * len(missed) / mac_in_prod_markets <= a.max_missed_pct
    print("\n== Verdict:", "PASS" if limit_ok else "FLAG",
          f"(MISSED {pct(len(missed), mac_in_prod_markets)} vs limit {a.max_missed_pct}%)")
    return 0 if limit_ok else 1


if __name__ == "__main__":
    sys.exit(main())
