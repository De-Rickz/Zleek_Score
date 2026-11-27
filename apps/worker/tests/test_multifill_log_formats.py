"""
check_multifill_collisions.py check 1 reads the same events from worker.log
lines and from `docker compose logs` output (container prefix, optional
timestamp), and from stdin (--log -). Offline: no database, no network.

    cd apps/worker && .venv/bin/python tests/test_multifill_log_formats.py
"""
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import check_multifill_collisions as c

EV1 = ("{'market': '0xabc', 'asset_id': '111', 'price': '0.42', 'size': '10', "
       "'side': 'BUY', 'event_type': 'last_trade_price', 'transaction_hash': '0xaa'}")
EV2 = EV1.replace("'size': '10'", "'size': '5'")           # same tx, different fill
EV3 = EV1.replace("0xaa", "0xbb")

FILE_LINES = [f"2026-11-27 10:00:0{i},123 INFO ws {ev}\n" for i, ev in enumerate([EV1, EV2, EV3, EV1])]
DOCKER_LINES = [f"worker-1  | 2026-11-27 10:00:0{i},123 INFO ws {ev}\n" for i, ev in enumerate([EV1, EV2, EV3, EV1])]
DOCKER_TS_LINES = [f"worker-1  | 2026-11-27T10:00:0{i}.123456789Z {ev}\n" for i, ev in enumerate([EV1, EV2, EV3, EV1])]
NOISE = ["worker-1  | Subscribed to 500 assets\n", "worker-1  | 2026-11-27T10:00:00Z {'event_type': 'book'}\n"]


def main():
    assert c.strip_docker_prefix("worker-1  | 2026-11-27T10:00:00.5Z hello") == "hello"
    assert c.strip_docker_prefix("zleek-worker-1 | hello") == "hello"
    assert c.strip_docker_prefix("2026-11-27 10:00:00 INFO hello") == "2026-11-27 10:00:00 INFO hello"
    print("ok  1 prefix stripping")

    results = [c.parse_log_lines(ls) for ls in (FILE_LINES, DOCKER_LINES, DOCKER_TS_LINES, DOCKER_LINES + NOISE)]
    assert all(r == results[0] for r in results), results
    r = results[0]
    assert len(r["0xaa"]) == 2 and len(r["0xbb"]) == 1, r   # redelivery of EV1 isn't a collision
    print("ok  2 file and docker formats parse to the same events (1 collision, redelivery ignored)")

    with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as f:
        f.writelines(DOCKER_TS_LINES)
    try:
        assert c.check_log(f.name) == 1
    finally:
        os.unlink(f.name)
    print("ok  3 --log <file> with docker lines")

    code = ("import sys; sys.path.insert(0, %r); import check_multifill_collisions as c; "
            "sys.exit(c.check_log('-'))" % os.path.join(os.path.dirname(__file__), "..", "scripts"))
    p = subprocess.run([sys.executable, "-c", code], input="".join(DOCKER_LINES + NOISE),
                       capture_output=True, text=True)
    assert p.returncode == 1 and "COLLISIONS FOUND" in p.stdout, (p.returncode, p.stdout, p.stderr)
    print("ok  4 --log - reads stdin")

    h = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "..", "scripts",
                        "check_multifill_collisions.py"), "--help"], capture_output=True, text=True).stdout
    assert "logs --no-color worker" in h and "--log -" in h, h
    print("ok  5 usage text shows the server command")
    print("all multifill log format tests passed")


if __name__ == "__main__":
    main()
