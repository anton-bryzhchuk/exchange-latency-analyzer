"""Compare Binance Futures depth, bookTicker and aggTrade delivery/trigger timing.

Read-only market-data benchmark. No exchange keys and no orders.
"""

import argparse
import asyncio
import json
import ssl
import statistics
import time
from collections import defaultdict

import certifi
import websockets

import config
from main import SymbolDetector, parse_levels


SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


def percentile(values, fraction):
    if not values:
        return float("nan")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * fraction)))
    return ordered[index]


def summary(label, values, suffix="ms"):
    if not values:
        print(f"{label:<24} n=0")
        return
    print(
        f"{label:<24} n={len(values):6} "
        f"p50={statistics.median(values):7.2f}{suffix} "
        f"p90={percentile(values, 0.90):7.2f}{suffix} "
        f"p99={percentile(values, 0.99):7.2f}{suffix}"
    )


async def run(duration):
    pairs = config.SYMBOL_PAIRS
    detectors = {
        pair["binance"].lower(): SymbolDetector(pair) for pair in pairs
    }
    stream_names = []
    for pair in pairs:
        symbol = pair["binance"].lower()
        stream_names.extend((
            f"{symbol}@depth{config.DEPTH_LEVELS}@100ms",
            f"{symbol}@bookTicker",
            f"{symbol}@aggTrade",
        ))
    url = f"{config.BINANCE_WS_BASE}?streams={'/'.join(stream_names)}"

    ages = defaultdict(list)
    cadences = defaultdict(list)
    last_arrival = {}
    latest_trigger = defaultdict(dict)
    last_book = {}
    leads = defaultdict(list)
    event_types = defaultdict(int)
    sweep_count = 0
    started = time.monotonic()

    async with websockets.connect(
        url, ssl=SSL_CONTEXT, ping_interval=20, ping_timeout=20,
        max_queue=4096,
    ) as ws:
        print(
            f"[bench] connected symbols={len(pairs)} streams={len(stream_names)} "
            f"duration={duration:.0f}s"
        )
        while time.monotonic() - started < duration:
            timeout = max(0.1, duration - (time.monotonic() - started))
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                break
            received_wall_ms = time.time_ns() / 1_000_000.0
            received_mono = time.monotonic()
            message = json.loads(raw)
            stream = message.get("stream", "")
            stream_lower = stream.lower()
            data = message.get("data") or {}
            event_type = str(data.get("e", "")).lower()
            event_types[event_type or "missing"] += 1
            symbol_key = str(data.get("s", "")).lower()

            if "@depth" in stream_lower or event_type == "depthupdate":
                kind = "depth"
            elif "@bookticker" in stream_lower or event_type == "bookticker":
                kind = "bookTicker"
            elif "@aggtrade" in stream_lower or event_type == "aggtrade":
                kind = "aggTrade"
            else:
                continue

            event_ms = data.get("E")
            if event_ms is not None:
                ages[kind].append(received_wall_ms - float(event_ms))
            cadence_key = (kind, symbol_key)
            previous_arrival = last_arrival.get(cadence_key)
            if previous_arrival is not None:
                cadences[kind].append(1000.0 * (received_mono - previous_arrival))
            last_arrival[cadence_key] = received_mono

            if kind == "bookTicker":
                bid = float(data["b"])
                ask = float(data["a"])
                previous = last_book.get(symbol_key)
                if previous is not None:
                    old_bid, old_ask = previous
                    if bid > old_bid or ask > old_ask:
                        latest_trigger[symbol_key]["bookTicker_up"] = received_mono
                    if bid < old_bid or ask < old_ask:
                        latest_trigger[symbol_key]["bookTicker_down"] = received_mono
                last_book[symbol_key] = (bid, ask)
                continue

            if kind == "aggTrade":
                direction = "down" if bool(data.get("m")) else "up"
                latest_trigger[symbol_key][f"aggTrade_{direction}"] = received_mono
                continue

            detector = detectors.get(symbol_key)
            if detector is None:
                continue
            event = detector.update(
                parse_levels(data.get("b") or []),
                parse_levels(data.get("a") or []),
            )
            if event is None:
                continue
            sweep_count += 1
            direction = event["direction"]
            for trigger_kind in ("bookTicker", "aggTrade"):
                trigger_time = latest_trigger[symbol_key].get(
                    f"{trigger_kind}_{direction}"
                )
                if trigger_time is None:
                    continue
                lead_ms = 1000.0 * (received_mono - trigger_time)
                if 0.0 <= lead_ms <= 1000.0:
                    leads[trigger_kind].append(lead_ms)

    print(f"[bench] depth sweep signals={sweep_count}")
    print(f"[bench] raw event types={dict(sorted(event_types.items()))}")
    print("[bench] delivery age (local receive time - Binance event E)")
    for kind in ("depth", "bookTicker", "aggTrade"):
        summary(kind, ages[kind])
    print("[bench] per-symbol message cadence")
    for kind in ("depth", "bookTicker", "aggTrade"):
        summary(kind, cadences[kind])
    print("[bench] same-direction trigger lead before depth sweep")
    summary("bookTicker lead", leads["bookTicker"])
    summary("aggTrade lead", leads["aggTrade"])


def main():
    parser = argparse.ArgumentParser(description="Binance feed timing benchmark")
    parser.add_argument("--seconds", type=float, default=120.0)
    args = parser.parse_args()
    asyncio.run(run(args.seconds))


if __name__ == "__main__":
    main()
