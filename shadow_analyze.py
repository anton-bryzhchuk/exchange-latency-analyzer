"""Simulate MEXC Futures entries/exits from recorded executable depth.

No orders are sent. For each Binance sweep, the first MEXC Futures depth
snapshot is treated as the entry opportunity. Later snapshots are evaluated in
time order for target, stop, MFE, MAE, and timeout PnL after taker fees.
"""

import argparse
import csv
import json
import sqlite3
import statistics
import time
from collections import defaultdict
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import urlopen


CONTRACT_INFO_URL = "https://api.mexc.com/api/v1/contract/detail/country"


def contract_size(symbol, cache):
    if symbol in cache:
        return cache[symbol]
    url = f"{CONTRACT_INFO_URL}?{urlencode({'symbol': symbol})}"
    with urlopen(url, timeout=10) as response:
        payload = json.load(response)
    if not payload.get("success"):
        raise RuntimeError(f"contract info failed for {symbol}: {payload}")
    value = float(payload["data"]["contractSize"])
    cache[symbol] = value
    time.sleep(0.22)  # stay below the documented 10 requests / 2 seconds
    return value


def fill_base(levels, base_quantity, size_per_contract):
    """Return (VWAP, quote value) for a base-quantity marketable fill."""
    remaining = base_quantity
    quote = 0.0
    filled = 0.0
    for level in levels:
        price = float(level[0])
        contracts = float(level[1])
        available_base = contracts * size_per_contract
        take = min(remaining, available_base)
        if take > 0:
            quote += take * price
            filled += take
            remaining -= take
        if remaining <= 1e-12:
            break
    if remaining > max(1e-12, base_quantity * 1e-9) or filled <= 0:
        return None
    return quote / filled, quote


def pnl_at(direction, base_quantity, entry_quote, snapshot, size, fee_rate):
    levels = snapshot["bids"] if direction == "up" else snapshot["asks"]
    exit_fill = fill_base(levels, base_quantity, size)
    if exit_fill is None:
        return None
    exit_vwap, exit_quote = exit_fill
    fees = (entry_quote + exit_quote) * fee_rate
    if direction == "up":
        pnl = exit_quote - entry_quote - fees
    else:
        pnl = entry_quote - exit_quote - fees
    return pnl, exit_vwap, exit_quote, fees


def load_events(db_path):
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT e.id AS event_id, e.timestamp AS signal_time,
                   e.binance_symbol, e.mexc_symbol, e.direction,
                   e.consumed_usd_volume, e.binance_best_bid,
                   e.binance_best_ask,
                   d.offset_seconds, d.request_latency_ms,
                   d.response_received, d.server_timestamp,
                   d.bids_json, d.asks_json
            FROM sweep_events e
            JOIN mexc_depth_snapshots d ON d.sweep_event_id = e.id
            ORDER BY e.id, d.offset_seconds, d.id
            """
        ).fetchall()
    finally:
        connection.close()

    grouped = defaultdict(list)
    for row in rows:
        signal_time = datetime.fromisoformat(row["signal_time"])
        response_time = datetime.fromisoformat(row["response_received"])
        grouped[row["event_id"]].append({
            "event_id": row["event_id"],
            "signal_time": row["signal_time"],
            "binance_symbol": row["binance_symbol"],
            "mexc_symbol": row["mexc_symbol"],
            "direction": row["direction"],
            "consumed_usd_volume": row["consumed_usd_volume"],
            "binance_best_bid": row["binance_best_bid"],
            "binance_best_ask": row["binance_best_ask"],
            "nominal_offset": float(row["offset_seconds"]),
            "actual_offset": max(
                0.0, (response_time - signal_time).total_seconds()
            ),
            "request_latency_ms": float(row["request_latency_ms"]),
            "server_timestamp": row["server_timestamp"],
            "bids": json.loads(row["bids_json"]),
            "asks": json.loads(row["asks_json"]),
        })
    return grouped


def simulate(event_snapshots, notional, fee_rate, target, stop, size):
    entries = [s for s in event_snapshots if s["nominal_offset"] == 0]
    if not entries:
        return None
    entry = min(entries, key=lambda item: item["actual_offset"])
    snapshots = sorted(
        (s for s in event_snapshots if s["actual_offset"] > entry["actual_offset"]),
        key=lambda item: item["actual_offset"],
    )
    if not snapshots:
        return None

    direction = entry["direction"]
    entry_levels = entry["asks"] if direction == "up" else entry["bids"]
    if not entry_levels:
        return None
    base_quantity = notional / float(entry_levels[0][0])
    entry_fill = fill_base(entry_levels, base_quantity, size)
    if entry_fill is None:
        return None
    entry_vwap, entry_quote = entry_fill

    path = []
    for snapshot in snapshots:
        result = pnl_at(
            direction, base_quantity, entry_quote, snapshot, size, fee_rate
        )
        if result is not None:
            pnl, exit_vwap, exit_quote, fees = result
            path.append((snapshot, pnl, exit_vwap, exit_quote, fees))
    if not path:
        return None

    chosen = path[-1]
    outcome = "timeout"
    for point in path:
        if point[1] >= target:
            chosen = point
            outcome = "target"
            break
        if point[1] <= -stop:
            chosen = point
            outcome = "stop"
            break

    snapshot, pnl, exit_vwap, exit_quote, fees = chosen
    return {
        "event_id": entry["event_id"],
        "signal_time": entry["signal_time"],
        "binance_symbol": entry["binance_symbol"],
        "mexc_symbol": entry["mexc_symbol"],
        "direction": direction,
        "consumed_usd_volume": entry["consumed_usd_volume"],
        "entry_rtt_ms": entry["request_latency_ms"],
        "entry_vwap": entry_vwap,
        "exit_vwap": exit_vwap,
        "entry_actual_offset_s": entry["actual_offset"],
        "exit_offset_s": snapshot["actual_offset"],
        "base_quantity": base_quantity,
        "entry_quote": entry_quote,
        "exit_quote": exit_quote,
        "fees": fees,
        "pnl": pnl,
        "mfe": max(point[1] for point in path),
        "mae": min(point[1] for point in path),
        "outcome": outcome,
    }


def summarize(label, results):
    if not results:
        print(f"{label:<14} n=0")
        return
    pnls = [row["pnl"] for row in results]
    rtts = [row["entry_rtt_ms"] for row in results]
    targets = sum(row["outcome"] == "target" for row in results)
    stops = sum(row["outcome"] == "stop" for row in results)
    positive = sum(row["pnl"] > 0 for row in results)
    print(
        f"{label:<14} n={len(results):5d} positive={100*positive/len(results):5.1f}% "
        f"target={100*targets/len(results):5.1f}% stop={100*stops/len(results):5.1f}% "
        f"avg=${statistics.mean(pnls):+7.2f} med=${statistics.median(pnls):+7.2f} "
        f"total=${sum(pnls):+9.2f} entryRTTmed={statistics.median(rtts):5.1f}ms"
    )


def load_thresholds(path):
    with open(path) as handle:
        return json.load(handle)


def sweep_tier(symbol, volume, thresholds):
    spec = thresholds.get("symbols", {}).get(symbol)
    if not spec:
        return "observe"
    if volume >= spec["p99"]:
        return "exceptional"
    if volume >= spec["p95"]:
        return "strong"
    if volume >= spec["p90"]:
        return "candidate"
    return "observe"


def main():
    parser = argparse.ArgumentParser(description="MEXC Futures shadow simulator")
    parser.add_argument("db", nargs="?", default="sweep_events_futures.db")
    parser.add_argument("--notional", type=float, default=2000.0)
    parser.add_argument("--fee", type=float, default=0.0008,
                        help="Taker fee per side as a decimal")
    parser.add_argument("--target", type=float, default=10.0)
    parser.add_argument("--stop", type=float, default=5.0)
    parser.add_argument("--csv", default="shadow_results.csv")
    parser.add_argument("--thresholds", default="sweep_thresholds.json")
    args = parser.parse_args()

    grouped = load_events(args.db)
    thresholds = load_thresholds(args.thresholds)
    specs = {}
    results = []
    skipped = 0
    for snapshots in grouped.values():
        try:
            size = contract_size(snapshots[0]["mexc_symbol"], specs)
            result = simulate(
                snapshots, args.notional, args.fee, args.target, args.stop, size
            )
        except Exception as exc:
            print(f"[skip] event {snapshots[0]['event_id']}: {exc}")
            skipped += 1
            continue
        if result is None:
            skipped += 1
        else:
            result["sweep_tier"] = sweep_tier(
                result["binance_symbol"],
                result["consumed_usd_volume"],
                thresholds,
            )
            results.append(result)

    print(
        f"[shadow] db={args.db} notional=${args.notional:.0f} "
        f"fee/side={args.fee*100:.3f}% target=${args.target:.2f} stop=${args.stop:.2f}"
    )
    summarize("ALL", results)
    by_tier = defaultdict(list)
    for row in results:
        by_tier[row["sweep_tier"]].append(row)
    for tier in ("observe", "candidate", "strong", "exceptional"):
        summarize(tier, by_tier[tier])

    print("[shadow] by symbol")
    by_symbol = defaultdict(list)
    for row in results:
        by_symbol[row["binance_symbol"]].append(row)
    for symbol, rows in sorted(by_symbol.items()):
        summarize(symbol, rows)
    print(f"[shadow] simulated={len(results)} skipped={skipped}")

    if results:
        with open(args.csv, "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print(f"[shadow] saved {args.csv}")


if __name__ == "__main__":
    main()
