import asyncio, json, websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

ECHO_URL = "wss://echo-websocket.fly.dev/"   # active; the server may send an unsolicited greeting

async def send_and_recv(ws, payload, label):
    await ws.send(json.dumps(payload))
    msg = await asyncio.wait_for(ws.recv(), timeout=5)
    print(f"{label} echo:", msg)

async def test():
    try:
        async with websockets.connect(
            ECHO_URL,
            max_size=2**20,           # 1 MiB
            ping_interval=20,
            ping_timeout=20,
            compression=None          # avoid permessage-deflate oddities on some echoes
        ) as ws:
            # Some echo servers send a greeting; consume it non-fatally.
            try:
                greeting = await asyncio.wait_for(ws.recv(), timeout=1)
                print("server msg:", greeting)
            except asyncio.TimeoutError:
                pass

            await send_and_recv(ws, {"ping": True}, "ping")
            await send_and_recv(ws, {
                "type": "trade","market_id":"mkt_test_1","price":0.45,"size_usd":1200,"side":"buy"
            }, "trade")
            await send_and_recv(ws, {
                "type":"book","market_id":"mkt_test_1","best_bid":0.44,"best_ask":0.46,
                "bid_depth_usd":8000,"ask_depth_usd":9500
            }, "book")

    except asyncio.TimeoutError:
        print("recv timeout: server didn’t echo the message in time")
    except (ConnectionClosedOK, ConnectionClosedError) as e:
        print(f"connection closed: code={getattr(e, 'code', '?')} reason={getattr(e, 'reason', '')}")

asyncio.run(test())
