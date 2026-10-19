"""Read-only probes for the exact market-data streams needed by the logger."""

import asyncio
import json
import ssl
import time

import certifi
import websockets


SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


async def probe_binance(url, seconds=5.0):
    count = 0
    event_types = {}
    try:
        async with websockets.connect(
            url, ssl=SSL_CONTEXT, ping_interval=20, ping_timeout=20,
        ) as ws:
            started = time.monotonic()
            while time.monotonic() - started < seconds:
                remaining = seconds - (time.monotonic() - started)
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                payload = json.loads(raw)
                data = payload.get("data", payload)
                event = str(data.get("e", "missing"))
                event_types[event] = event_types.get(event, 0) + 1
                count += 1
        print(f"[BINANCE] {url} messages={count} types={event_types}")
    except Exception as exc:
        print(f"[BINANCE] {url} FAILED: {type(exc).__name__}: {exc}")


async def probe_mexc(url, seconds=8.0):
    count = 0
    channels = {}
    try:
        async with websockets.connect(
            url, ssl=SSL_CONTEXT, ping_interval=None, open_timeout=8,
        ) as ws:
            await ws.send(json.dumps({
                "method": "sub.depth.full",
                "param": {"symbol": "WLD_USDT", "limit": 5},
            }))
            started = time.monotonic()
            while time.monotonic() - started < seconds:
                remaining = seconds - (time.monotonic() - started)
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                payload = json.loads(raw)
                channel = str(payload.get("channel", "missing"))
                channels[channel] = channels.get(channel, 0) + 1
                count += 1
                if channel == "ping":
                    await ws.send(json.dumps({"method": "pong"}))
        print(f"[MEXC] {url} messages={count} channels={channels}")
    except Exception as exc:
        print(f"[MEXC] {url} FAILED: {type(exc).__name__}: {exc}")


async def main():
    await probe_binance(
        "wss://fstream.binance.com/ws/btcusdt@aggTrade"
    )
    await probe_binance(
        "wss://fstream.binance.com/ws/btcusdt@trade"
    )
    await probe_mexc("wss://contract.mexc.com/edge")
    await probe_mexc("wss://api.mexc.com/edge")


if __name__ == "__main__":
    asyncio.run(main())
