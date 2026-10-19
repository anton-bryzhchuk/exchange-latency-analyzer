"""Probe Binance Futures aggTrade on a dedicated subscription connection."""

import argparse
import asyncio
import json
import ssl
import statistics
import time

import certifi
import websockets

import config


SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


def percentile(values, fraction):
    values = sorted(values)
    return values[int((len(values) - 1) * fraction)]


async def run(seconds):
    params = [f"{pair['binance'].lower()}@aggTrade" for pair in config.SYMBOL_PAIRS]
    counts = {}
    ages = []
    async with websockets.connect(
        "wss://fstream.binance.com/ws", ssl=SSL_CONTEXT,
        ping_interval=20, ping_timeout=20,
    ) as ws:
        await ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}))
        response = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        print(f"[agg-probe] subscribe={response}")
        started = time.monotonic()
        while time.monotonic() - started < seconds:
            timeout = max(0.1, seconds - (time.monotonic() - started))
            try:
                data = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
            except asyncio.TimeoutError:
                break
            if str(data.get("e", "")).lower() != "aggtrade":
                continue
            symbol = data.get("s", "?")
            counts[symbol] = counts.get(symbol, 0) + 1
            if data.get("E") is not None:
                ages.append(time.time_ns() / 1_000_000.0 - float(data["E"]))
    print(f"[agg-probe] events={sum(counts.values())} symbols={len(counts)}")
    if ages:
        print(
            f"[agg-probe] age p50={statistics.median(ages):.2f}ms "
            f"p90={percentile(ages, 0.90):.2f}ms "
            f"p99={percentile(ages, 0.99):.2f}ms"
        )
    print(f"[agg-probe] busiest={sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=60.0)
    args = parser.parse_args()
    asyncio.run(run(args.seconds))


if __name__ == "__main__":
    main()
