"""
Tests for dead-websocket detection in ingestion/clob_ws.py (ROADMAP phase 2,
item 2). Local only: a websocket server on localhost behind a TCP proxy that
can "black-hole" open connections (sockets stay open, nothing gets through),
which is what a half-open connection looks like. Timings are scaled down
(ping every 1 s instead of 20 s) so the test runs in seconds.

    cd apps/worker && .venv/bin/python tests/test_ws_keepalive.py
"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import websockets

import clob_ws

PING, TIMEOUT, CLOSE, HEARTBEAT = 1, 1, 1, 0.25   # scaled: production is 20, 20, 10, 10


class Server:
    """Polymarket stand-in: records subscriptions, answers text PING with PONG,
    and otherwise stays quiet (like a market with no trades)."""
    def __init__(self):
        self.subs, self.pings = [], 0

    async def handler(self, sock):
        async for msg in sock:
            if msg == "PING":
                self.pings += 1
                await sock.send("PONG")
            elif msg.startswith("{"):
                self.subs.append(time.monotonic())


class BlackholeProxy:
    """TCP proxy; blackhole() silently stops forwarding on every connection that
    is open at that moment. New connections pass normally."""
    def __init__(self, upstream_port):
        self.upstream_port, self.conns = upstream_port, []

    async def handle(self, reader, writer):
        up_r, up_w = await asyncio.open_connection("127.0.0.1", self.upstream_port)
        conn = {"alive": True}
        self.conns.append(conn)

        async def pipe(src, dst):
            try:
                while data := await src.read(65536):
                    if conn["alive"]:
                        dst.write(data)
                        await dst.drain()
            except (ConnectionError, asyncio.CancelledError):
                pass
        await asyncio.gather(pipe(reader, up_w), pipe(up_r, writer))

    def blackhole(self):
        for c in self.conns:
            c["alive"] = False


async def run_scenario(name, ping_interval, blackhole_after, watch_for):
    server = Server()
    ws_server = await websockets.serve(server.handler, "127.0.0.1", 0)
    up_port = ws_server.sockets[0].getsockname()[1]
    proxy = BlackholeProxy(up_port)
    tcp = await asyncio.start_server(proxy.handle, "127.0.0.1", 0)
    clob_ws.url = f"ws://127.0.0.1:{tcp.sockets[0].getsockname()[1]}"
    clob_ws.KEEPALIVE_PING_SEC, clob_ws.KEEPALIVE_TIMEOUT_SEC = ping_interval, TIMEOUT
    clob_ws.CLOSE_TIMEOUT_SEC = CLOSE
    clob_ws.HEARTBEAT_SEC, clob_ws.RESUBSCRIBE_CHECK_SEC = HEARTBEAT, 3600

    async def ids(pool, **kw): return ["tokA", "tokB"]
    async def maps(pool): return {}
    clob_ws.load_asset_ids, clob_ws.load_asset_maps = ids, maps

    task = asyncio.create_task(clob_ws.subscribe(writer=None, pool=None))
    t0 = time.monotonic()
    blackholed_at = None
    while time.monotonic() - t0 < watch_for:
        if blackhole_after is not None and blackholed_at is None and time.monotonic() - t0 >= blackhole_after:
            proxy.blackhole()
            blackholed_at = time.monotonic()
        await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    tcp.close()
    ws_server.close()
    detected = [s - blackholed_at for s in server.subs[1:]] if blackholed_at else []
    return server, detected


async def main():
    # (A) healthy but quiet: keepalive must not reconnect, text PING keeps flowing
    srv, _ = await run_scenario("healthy", PING, None, watch_for=6)
    assert len(srv.subs) == 1, f"healthy quiet connection was reconnected: {len(srv.subs)} subscriptions"
    assert srv.pings >= 6 / HEARTBEAT * 0.7, f"text PING not sent every {HEARTBEAT}s: {srv.pings} in 6s"
    print(f"ok  (A) healthy, quiet for 6s: 1 subscription, no reconnect; {srv.pings} text PINGs answered")

    # (B) new settings: a black-holed connection is detected and resubscribed
    srv, detected = await run_scenario("new", PING, 2, watch_for=10)
    # ping interval + pong timeout + close timeout + reconnect backoff (1s) + slack
    limit = PING + TIMEOUT + CLOSE + 1 + 1.5
    assert detected and detected[0] <= limit, f"not resubscribed within {limit}s: {detected}"
    print(f"ok  (B) black-holed connection: resubscribed {detected[0]:.1f}s later "
          f"(limit {limit:.1f}s; production equivalent ~{20 + 20 + 10 + 1}s)")

    # (C) old settings (ping_interval=None): the same connection hangs
    srv, detected = await run_scenario("old", None, 2, watch_for=10)
    assert not detected, f"old settings unexpectedly reconnected: {detected}"
    print("ok  (C) old settings (ping_interval=None): no reconnect in 8s after black-holing; "
          "the text PING alone doesn't detect it")
    print("all websocket keepalive tests passed")


if __name__ == "__main__":
    asyncio.run(main())
