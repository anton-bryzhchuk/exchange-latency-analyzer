"""Early Binance bookTicker -> MEXC Futures shadow logger.

Uses the latest rolling Binance depth as the liquidity reference, while a
real-time bookTicker price change acts as the early trigger. Public market data
only; no API keys and no orders.
"""

import asyncio
import json
import signal

import config

# Use an isolated DB. This assignment happens before importing main/storage, so
# Storage's default path is bound to the early-signal database.
config.DB_PATH = "sweep_events_bookticker_v3.db"
config.SWEEP_THRESHOLDS_FILE = "bookticker_thresholds.json"
config.MEXC_SNAPSHOT_OFFSETS = [
    0, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2,
    2.5, 3, 3.5, 4, 4.5, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30,
]
config.MEXC_LOG_OFFSETS = {0, 0.25, 1, 2, 5, 10, 20, 30}
config.BINANCE_TRAJECTORY_INTERVAL_SECONDS = 0.1
config.BINANCE_TRAJECTORY_DURATION_SECONDS = 30.0

from main import (  # noqa: E402  (config must be set before this import)
    ArbLogger,
    SymbolDetector,
    now_ms,
    parse_levels,
    utcnow_iso,
)


class BookTickerDetector(SymbolDetector):
    """Depth supplies liquidity; bookTicker supplies the earliest price move."""

    def __init__(self, pair):
        super().__init__(pair)
        self.latest_book_ticker = None

    def update_depth_cache(self, bids, asks):
        ts = now_ms()
        self.buffer.append((ts, bids, asks))
        cutoff = ts - config.BUFFER_RETENTION_MS
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()

    @staticmethod
    def _removed_ask_usd(old_asks, best_ask, best_ask_qty):
        removed = 0.0
        for price, quantity in old_asks:
            if price < best_ask:
                removed += price * quantity
            elif price == best_ask and best_ask_qty < quantity:
                removed += price * (quantity - best_ask_qty)
        return removed

    @staticmethod
    def _removed_bid_usd(old_bids, best_bid, best_bid_qty):
        removed = 0.0
        for price, quantity in old_bids:
            if price > best_bid:
                removed += price * quantity
            elif price == best_bid and best_bid_qty < quantity:
                removed += price * (quantity - best_bid_qty)
        return removed

    def update_book_ticker(
        self, best_bid, bid_qty, best_ask, ask_qty, binance_event_age_ms=None,
        exchange_event_ms=None,
    ):
        ts = now_ms()
        self.latest_book_ticker = {
            "received_at": utcnow_iso(),
            "exchange_event_ms": exchange_event_ms,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid_quantity": bid_qty,
            "ask_quantity": ask_qty,
        }
        if not self.buffer:
            return None
        reference = self._pick_reference(ts - config.DETECTION_WINDOW_MS)
        if reference is None:
            return None
        ref_ts, ref_bids, ref_asks = reference
        if ts - ref_ts < config.DETECTION_WINDOW_MS * 0.5:
            return None
        if not ref_bids or not ref_asks:
            return None

        old_best_bid = ref_bids[0][0]
        old_best_ask = ref_asks[0][0]
        bid_usd = sum(price * quantity for price, quantity in ref_bids)
        ask_usd = sum(price * quantity for price, quantity in ref_asks)
        total_usd = bid_usd + ask_usd
        imbalance = (bid_usd - ask_usd) / total_usd if total_usd else 0.0
        if best_ask > old_best_ask:
            consumed = self._removed_ask_usd(ref_asks, best_ask, ask_qty)
            if consumed >= config.CONSUMED_USD_THRESHOLD:
                return self._early_fire(
                    "up", consumed, best_bid, best_ask, ts,
                    binance_event_age_ms=binance_event_age_ms,
                    reference_age_ms=ts - ref_ts,
                    price_move_bps=10_000.0 * (best_ask / old_best_ask - 1.0),
                    levels_crossed=sum(price < best_ask for price, _ in ref_asks),
                    reference_side_usd=ask_usd,
                    reference_imbalance=imbalance,
                )
        if best_bid < old_best_bid:
            consumed = self._removed_bid_usd(ref_bids, best_bid, bid_qty)
            if consumed >= config.CONSUMED_USD_THRESHOLD:
                return self._early_fire(
                    "down", consumed, best_bid, best_ask, ts,
                    binance_event_age_ms=binance_event_age_ms,
                    reference_age_ms=ts - ref_ts,
                    price_move_bps=10_000.0 * (old_best_bid / best_bid - 1.0),
                    levels_crossed=sum(price > best_bid for price, _ in ref_bids),
                    reference_side_usd=bid_usd,
                    reference_imbalance=imbalance,
                )
        return None

    def _early_fire(
        self, direction, consumed, best_bid, best_ask, ts, **features,
    ):
        if ts - self._last_fire[direction] < config.EVENT_COOLDOWN_SECONDS * 1000.0:
            return None
        self._last_fire[direction] = ts
        event = {
            "timestamp": utcnow_iso(),
            "binance_symbol": self.binance_symbol,
            "mexc_symbol": self.mexc_symbol,
            "direction": direction,
            "consumed_usd_volume": consumed,
            "binance_best_bid": best_bid,
            "binance_best_ask": best_ask,
            "signal_source": "bookTicker",
        }
        event.update(features)
        side_usd = features.get("reference_side_usd") or 0.0
        event["consumed_fraction"] = consumed / side_usd if side_usd else None
        return event


class BookTickerShadowLogger(ArbLogger):
    def __init__(self):
        super().__init__()
        self.detectors = {
            pair["binance"].lower(): BookTickerDetector(pair)
            for pair in config.SYMBOL_PAIRS
        }

    def _combined_url(self):
        streams = []
        for pair in config.SYMBOL_PAIRS:
            symbol = pair["binance"].lower()
            streams.extend((
                f"{symbol}@depth{config.DEPTH_LEVELS}@100ms",
                f"{symbol}@bookTicker",
            ))
        return f"{config.BINANCE_WS_BASE}?streams={'/'.join(streams)}"

    def _on_message(self, raw):
        message = json.loads(raw)
        data = message.get("data", message)
        stream = message.get("stream", "")
        stream_lower = stream.lower()
        stream_key = stream.split("@")[0] if stream else None
        detector = self.detectors.get(stream_key)
        if detector is None:
            return

        if "@depth" in stream_lower:
            detector.update_depth_cache(
                parse_levels(data.get("b") or []),
                parse_levels(data.get("a") or []),
            )
            return
        if "@bookticker" not in stream_lower:
            return

        event_ms = data.get("E")
        event_age_ms = None
        if event_ms is not None:
            event_age_ms = now_ms() - float(event_ms)
        event = detector.update_book_ticker(
            float(data["b"]), float(data.get("B", 0.0)),
            float(data["a"]), float(data.get("A", 0.0)),
            binance_event_age_ms=event_age_ms,
            exchange_event_ms=event_ms,
        )
        if event is not None:
            self.handle_event(event, detector)

    def schedule_lbank_snapshots(self, sweep_event_id, detector):
        # This experiment measures only Binance -> MEXC Futures.
        return


async def main():
    app = BookTickerShadowLogger()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, app.request_stop)
        except NotImplementedError:
            pass
    await app.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
