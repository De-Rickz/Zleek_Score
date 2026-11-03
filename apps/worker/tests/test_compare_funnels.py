"""
Tests for compare_funnels.py's trade classification (MISSED / NOT FOLLOWED AT
THE TIME / NOT TRACKED) and its verdict. Synthetic CSVs in a temp directory;
no database.

    cd apps/worker && .venv/bin/python tests/test_compare_funnels.py
"""
import csv
import os
import subprocess
import sys
import tempfile

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "scripts", "compare_funnels.py")
T0 = 1793700000
TRADE_COLS = ["tx_hash", "market_id", "ts_unix", "source", "size_usd"]
MARKET_COLS = ["market_id", "trades", "bars", "features", "scores", "max_score", "snapshots"]


def write(path, cols, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)


def trade(h, market, offset):
    return [h, market, T0 + offset, "websocket", "1.00"]


def run(mac_trades, prod_trades, *args):
    with tempfile.TemporaryDirectory() as d:
        paths = [os.path.join(d, n) for n in ("mt.csv", "pt.csv", "mm.csv", "pm.csv")]
        write(paths[0], TRADE_COLS, mac_trades)
        write(paths[1], TRADE_COLS, prod_trades)
        write(paths[2], MARKET_COLS, [])
        write(paths[3], MARKET_COLS, [])
        p = subprocess.run([sys.executable, SCRIPT, *paths, *args], capture_output=True, text=True)
        return p.returncode, p.stdout


def line(out, prefix):
    return next(l for l in out.splitlines() if l.strip().startswith(prefix))


def test_classification():
    shared = [trade(f"s{i}", "A", 60 * i) for i in range(10)]   # A: both have 10 trades, 0-9 min
    mac = shared + [
        trade("m_near", "A", 600 + 9 * 60),     # 10 min after prod's last A trade (540 s): MISSED (edge, inclusive)
        trade("m_far", "A", 540 + 601),         # 10 min 1 s after: NOT FOLLOWED AT THE TIME
        trade("m_late", "A", 3600),             # an hour later: NOT FOLLOWED AT THE TIME
        trade("m_other", "B", 100),             # prod has no B trades: NOT TRACKED
    ]
    prod = shared + [trade("p_only", "A", 30)]
    code, out = run(mac, prod)
    assert "in both: 10   Mac only: 4   Prod only: 1" in out, out
    assert "NOT TRACKED by prod: 1 trades in 1 markets" in line(out, "Mac-only, market NOT TRACKED"), out
    assert ": 2 trades in 1 markets" in line(out, "Mac-only, prod NOT FOLLOWING"), out
    # denominator = 10 matched + the 1 MISSED trade prod was following
    assert "1 trades = 9.09% of the Mac's 11 trades" in line(out, "Mac-only, MISSED"), out
    assert "FLAG" in line(out, "== Verdict"), out
    assert code == 1, code


def test_window_option():
    shared = [trade("s0", "A", 0)]
    mac = shared + [trade("m", "A", 15 * 60)]   # 15 min after prod's only A trade
    code, out = run(mac, shared)
    assert ": 1 trades in 1 markets" in line(out, "Mac-only, prod NOT FOLLOWING"), out
    assert "PASS" in line(out, "== Verdict") and code == 0, out
    code, out = run(mac, shared, "--missed-window-min", "20")
    assert "MISSED while prod was following the market: 1 trades" in out, out
    assert code == 1, out


def test_identical_exports_pass():
    rows = [trade(f"s{i}", "A", 60 * i) for i in range(2000)]
    code, out = run(rows, rows)
    assert "MISSED while prod was following the market: 0 trades = 0.00% of the Mac's 2000" in out, out
    assert "PASS" in line(out, "== Verdict") and code == 0, out
    assert "too small" not in out, out   # 1/2000 = 0.05% <= 0.1%: no small-sample note


def test_small_sample_note():
    rows = [trade(f"s{i}", "A", 60 * i) for i in range(50)]
    code, out = run(rows, rows)
    assert "PASS" in out and "the sample is too small for a 0.1% limit" in out, out


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
