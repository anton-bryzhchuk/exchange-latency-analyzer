"""Binance order-book state -> MEXC Futures paper executor.

The strategy has no holding-time exit. Entry and exit decisions are driven only
by Binance Futures order-book events. MEXC is used only for executable paper
fills. Public market data only; this process never submits an order.
"""

import asyncio
import json
import signal
import statistics
import time
from collections import defaultdict, deque

import config
import websockets


config.DB_PATH = "sweep_events_orderbook_v8.db"
config.SWEEP_THRESHOLDS_FILE = "bookticker_thresholds.json"
config.MEXC_SNAPSHOT_OFFSETS = [0]
config.MEXC_LOG_OFFSETS = {0}
config.MEXC_MAX_CONCURRENT_REQUESTS = 3
config.CONSUMED_USD_THRESHOLD = 50_000.0
config.DETECTION_WINDOW_MS = 100.0

from main import (  # noqa: E402 - config must be set before importing main
    ArbLogger,
    SSL_CONTEXT,
    SymbolDetector,
    now_ms,
    parse_levels,
    utcnow_iso,
)
from shadow_analyze import fill_base  # noqa: E402


PAPER_POLICY = "binance_momentum_stop"
PAPER_NOTIONAL = 2_000.0
PAPER_ENTRY_FEE = 0.0008
PAPER_EXIT_FEE = 0.0008
ROUND_TRIP_FEE_BPS = (PAPER_ENTRY_FEE + PAPER_EXIT_FEE) * 10_000.0
EXIT_SLIPPAGE_BUFFER_BPS = 2.0
MIN_EXPECTED_PROFIT_BPS = 2.0
REQUIRED_EDGE_BPS = (
    ROUND_TRIP_FEE_BPS + EXIT_SLIPPAGE_BUFFER_BPS + MIN_EXPECTED_PROFIT_BPS
)

EARLY_WINDOW_MS = 100.0
EARLY_P90_FRACTION = 1.0
EARLY_MIN_CONSUMED_USD = 50_000.0
EARLY_MIN_TRADE_USD = 0.0
EARLY_MIN_TRADE_P90_FRACTION = 0.0
EARLY_MIN_CONFIRMATION_RATIO = 0.0

BASIS_INTERVAL_SECONDS = 0.55
BASIS_HISTORY = 20
BASIS_MIN_SAMPLES = 3
BASIS_MAX_ABS_BPS = 500.0

# There is no maximum holding time. This short interval measures only how long
# Binance has failed to print a new favorable BBO after the last progress.
MOMENTUM_STALL_MS = 200.0
MOMENTUM_WATCHDOG_MS = 10.0

# No bookTicker history existed for these three configured symbols. Two use
# their older depth-derived p90; FHE stays on a conservative temporary floor
# until this logger collects enough native order-book events to replace it.
MISSING_SYMBOL_ENTRY_USD = {
    "AKEUSDT": 61_674.36,
    "FHEUSDT": 100_000.00,
    "PIPPINUSDT": 69_113.71,
}


def fill_base_ioc(levels, base_quantity, size_per_contract, direction, limit_price):
    """Require a full IOC paper fill without crossing the acceptable limit."""
    eligible = []
    for level in levels:
        price = float(level[0])
        allowed = (
            price <= limit_price if direction == "up" else price >= limit_price
        )
        if not allowed:
            break
        eligible.append(level)
    return fill_base(eligible, base_quantity, size_per_contract)


class OrderBookDetector(SymbolDetector):
    """Depth supplies liquidity; bookTicker supplies the earliest BBO state."""

    def __init__(self, pair):
        super().__init__(pair)
        self.latest_book_ticker = None
        self.latest_depth = None
        self.trade_buffer = deque()
        self.target_p90_usd = 100_000.0
        self.entry_trigger_usd = max(
            EARLY_MIN_CONSUMED_USD,
            self.target_p90_usd * EARLY_P90_FRACTION,
        )

    def configure_entry_threshold(self, target_p90_usd):
        self.target_p90_usd = float(target_p90_usd)
        self.entry_trigger_usd = max(
            EARLY_MIN_CONSUMED_USD,
            self.target_p90_usd * EARLY_P90_FRACTION,
        )

    def update_depth_cache(self, bids, asks):
        ts = now_ms()
        self.buffer.append((ts, bids, asks))
        self.latest_depth = (ts, bids, asks)
        cutoff = ts - config.BUFFER_RETENTION_MS
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()

    def update_trade(self, price, quantity, buyer_is_maker):
        ts = now_ms()
        quote = price * quantity
        # buyer_is_maker=True means the seller was the aggressor.
        signed_quote = -quote if buyer_is_maker else quote
        self.trade_buffer.append((ts, signed_quote))
        cutoff = ts - EARLY_WINDOW_MS
        while self.trade_buffer and self.trade_buffer[0][0] < cutoff:
            self.trade_buffer.popleft()

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
        self, best_bid, bid_qty, best_ask, ask_qty, exchange_event_ms=None,
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
        reference = self._pick_reference(ts - EARLY_WINDOW_MS)
        if reference is None:
            return None
        ref_ts, ref_bids, ref_asks = reference
        if ts - ref_ts < EARLY_WINDOW_MS * 0.5:
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
            if consumed >= self.entry_trigger_usd:
                return self._fire(
                    "up", consumed, best_bid, best_ask, ts,
                    exchange_event_ms=exchange_event_ms,
                    reference_age_ms=ts - ref_ts,
                    price_move_bps=10_000.0 * (best_ask / old_best_ask - 1.0),
                    levels_crossed=sum(price < best_ask for price, _ in ref_asks),
                    reference_side_usd=ask_usd,
                    reference_imbalance=imbalance,
                )
        if best_bid < old_best_bid:
            consumed = self._removed_bid_usd(ref_bids, best_bid, bid_qty)
            if consumed >= self.entry_trigger_usd:
                return self._fire(
                    "down", consumed, best_bid, best_ask, ts,
                    exchange_event_ms=exchange_event_ms,
                    reference_age_ms=ts - ref_ts,
                    price_move_bps=10_000.0 * (old_best_bid / best_bid - 1.0),
                    levels_crossed=sum(price > best_bid for price, _ in ref_bids),
                    reference_side_usd=bid_usd,
                    reference_imbalance=imbalance,
                )
        return None

    def _fire(
        self, direction, consumed, best_bid, best_ask, ts,
        exchange_event_ms, **features,
    ):
        side_usd = features.get("reference_side_usd") or 0.0
        aggressive_buy_usd = sum(
            signed for _, signed in self.trade_buffer if signed > 0
        )
        aggressive_sell_usd = -sum(
            signed for _, signed in self.trade_buffer if signed < 0
        )
        directional_trade_usd = (
            aggressive_buy_usd if direction == "up" else aggressive_sell_usd
        )
        opposing_trade_usd = (
            aggressive_sell_usd if direction == "up" else aggressive_buy_usd
        )
        confirmation_ratio = directional_trade_usd / consumed if consumed else 0.0
        min_directional_trade = max(
            EARLY_MIN_TRADE_USD,
            self.target_p90_usd * EARLY_MIN_TRADE_P90_FRACTION,
        )
        if (
            directional_trade_usd < min_directional_trade
            or confirmation_ratio < EARLY_MIN_CONFIRMATION_RATIO
        ):
            return None
        if ts - self._last_fire[direction] < config.EVENT_COOLDOWN_SECONDS * 1000:
            return None
        self._last_fire[direction] = ts
        return {
            "timestamp": utcnow_iso(),
            "binance_symbol": self.binance_symbol,
            "mexc_symbol": self.mexc_symbol,
            "direction": direction,
            "consumed_usd_volume": consumed,
            "binance_best_bid": best_bid,
            "binance_best_ask": best_ask,
            "signal_source": "binanceOrderBook",
            "binance_event_age_ms": (
                ts - float(exchange_event_ms)
                if exchange_event_ms is not None else None
            ),
            "consumed_fraction": consumed / side_usd if side_usd else None,
            "aggressive_trade_count": len(self.trade_buffer),
            "aggressive_buy_usd": aggressive_buy_usd,
            "aggressive_sell_usd": aggressive_sell_usd,
            "directional_trade_usd": directional_trade_usd,
            "opposing_trade_usd": opposing_trade_usd,
            "trade_confirmation_ratio": (
                confirmation_ratio
            ),
            "target_p90_usd": self.target_p90_usd,
            "entry_trigger_usd": self.entry_trigger_usd,
            **features,
        }

    def current_book_state(self, direction, signal_bid, signal_ask):
        if self.latest_depth is None:
            return None
        _, bids, asks = self.latest_depth
        if not bids or not asks:
            return None
        bid_usd = sum(price * quantity for price, quantity in bids)
        ask_usd = sum(price * quantity for price, quantity in asks)
        total = bid_usd + ask_usd
        imbalance = (bid_usd - ask_usd) / total if total else 0.0
        directional_pressure = imbalance if direction == "up" else -imbalance
        if direction == "up":
            refill_usd = sum(
                price * quantity for price, quantity in asks
                if price <= signal_ask
            )
        else:
            refill_usd = sum(
                price * quantity for price, quantity in bids
                if price >= signal_bid
            )
        return {
            "book_imbalance": imbalance,
            "directional_pressure": directional_pressure,
            "refill_usd": refill_usd,
        }


class OrderBookShadowLogger(ArbLogger):
    def __init__(self):
        super().__init__()
        self.detectors = {
            pair["binance"].lower(): OrderBookDetector(pair)
            for pair in config.SYMBOL_PAIRS
        }
        for pair in config.SYMBOL_PAIRS:
            symbol = pair["binance"]
            thresholds = self.sweep_thresholds.get(symbol)
            target = (
                float(thresholds["p90"])
                if thresholds else MISSING_SYMBOL_ENTRY_USD[symbol]
            )
            self.detectors[symbol.lower()].configure_entry_threshold(target)
        self.active = defaultdict(list)
        self.event_meta = {}
        self.trackers_by_event = {}
        self.paper_entries = {}
        self.pending_exits = defaultdict(list)
        self.contract_sizes = {}
        self.basis_samples = defaultdict(
            lambda: deque(maxlen=BASIS_HISTORY)
        )

    def _event_tier(self, event):
        symbol = event["binance_symbol"]
        thresholds = self.sweep_thresholds.get(symbol)
        consumed = float(event["consumed_usd_volume"])
        if thresholds:
            p90 = float(thresholds["p90"])
            if consumed >= float(thresholds["p99"]):
                return "exceptional", p90
            if consumed >= float(thresholds["p95"]):
                return "strong", p90
            if consumed >= p90:
                return "candidate", p90
            return "early", p90
        threshold = MISSING_SYMBOL_ENTRY_USD.get(symbol)
        if threshold is None:
            return "observe", self.threshold_floor_usd
        if consumed >= threshold * 3.0:
            return "exceptional", threshold
        if consumed >= threshold * 1.75:
            return "strong", threshold
        if consumed >= threshold:
            return "candidate", threshold
        return "early", threshold

    def schedule_binance_trajectory(self, sweep_event_id, detector):
        # Recorded directly from every relevant Binance BBO change.
        return

    def schedule_lbank_snapshots(self, sweep_event_id, detector):
        return

    def _record_event_only(self, event, tier, threshold, label):
        event_id = self.storage.insert_sweep_event(
            timestamp=event["timestamp"],
            binance_symbol=event["binance_symbol"],
            mexc_symbol=event["mexc_symbol"],
            direction=event["direction"],
            consumed_usd_volume=event["consumed_usd_volume"],
            binance_best_bid=event["binance_best_bid"],
            binance_best_ask=event["binance_best_ask"],
        )
        self.storage.insert_signal_features(
            sweep_event_id=event_id,
            signal_source=event["signal_source"],
            binance_event_age_ms=event.get("binance_event_age_ms"),
            reference_age_ms=event.get("reference_age_ms"),
            price_move_bps=event.get("price_move_bps"),
            levels_crossed=event.get("levels_crossed"),
            reference_side_usd=event.get("reference_side_usd"),
            consumed_fraction=event.get("consumed_fraction"),
            reference_imbalance=event.get("reference_imbalance"),
            aggressive_trade_count=event.get("aggressive_trade_count"),
            aggressive_buy_usd=event.get("aggressive_buy_usd"),
            aggressive_sell_usd=event.get("aggressive_sell_usd"),
            directional_trade_usd=event.get("directional_trade_usd"),
            opposing_trade_usd=event.get("opposing_trade_usd"),
            trade_confirmation_ratio=event.get("trade_confirmation_ratio"),
        )
        print(
            f"\n[{label} #{event_id}] {event['binance_symbol']:<12} "
            f"{event['direction'].upper():<4} consumed="
            f"${event['consumed_usd_volume']:,.0f} tier={tier} "
            f"threshold=${threshold:,.0f}"
        )
        return event_id

    def handle_event(self, event, detector):
        symbol_key = event["binance_symbol"].lower()
        tier, threshold = self._event_tier(event)
        live = [tracker for tracker in self.active[symbol_key]
                if not tracker["closed"]]

        if tier == "observe" or not live:
            event_id = super().handle_event(event, detector)
            if tier == "observe":
                return event_id
            tracker = {
                "event_id": event_id,
                "started": time.monotonic(),
                "direction": event["direction"],
                "signal_bid": float(event["binance_best_bid"]),
                "signal_ask": float(event["binance_best_ask"]),
                "consumed_usd": float(event["consumed_usd_volume"]),
                "peak_bps": 0.0,
                "favorable_price": (
                    float(event["binance_best_bid"])
                    if event["direction"] == "up"
                    else float(event["binance_best_ask"])
                ),
                "last_progress": time.monotonic(),
                "last_depth_progress_price": (
                    float(event["binance_best_bid"])
                    if event["direction"] == "up"
                    else float(event["binance_best_ask"])
                ),
                "last_sample": -1.0,
                "exit_inflight": False,
                "pre_entry_abort_reason": None,
                "closed": False,
            }
            self.active[symbol_key].append(tracker)
            self.trackers_by_event[event_id] = tracker
            self.event_meta[event_id] = {
                "direction": event["direction"],
                "mexc_symbol": detector.mexc_symbol,
                "target_p90_usd": threshold,
            }
            self._spawn(self._momentum_watchdog(tracker, detector))
            return event_id

        label = "CONTINUATION" if all(
            tracker["direction"] == event["direction"] for tracker in live
        ) else "OPPOSITE"
        event_id = self._record_event_only(event, tier, threshold, label)
        for tracker in live:
            if tracker["direction"] != event["direction"]:
                self._trigger_exit(
                    tracker, detector, "opposite_orderbook_sweep",
                    confirmation_updates=1,
                )
            else:
                tracker["consumed_usd"] = max(
                    tracker["consumed_usd"],
                    float(event["consumed_usd_volume"]),
                )
        return event_id

    async def _get_contract_size(self, symbol):
        if symbol in self.contract_sizes:
            return self.contract_sizes[symbol]
        await self._wait_mexc_rate_slot(entry_priority=False)
        payload = await self._get_json_rate_limited(
            "https://api.mexc.com/api/v1/contract/detail/country",
            {"symbol": symbol}, self._mexc_sem,
        )
        if not payload.get("success"):
            raise RuntimeError(
                f"contract size failed {payload.get('code')}: "
                f"{payload.get('message')}"
            )
        size = float(payload["data"]["contractSize"])
        self.contract_sizes[symbol] = size
        return size

    async def keep_mexc_warm(self):
        """Calibrate normal cross-venue basis without blocking entry signals."""
        detectors = list(self.detectors.values())
        index = 0
        while not self._stop.is_set():
            detector = detectors[index % len(detectors)]
            index += 1
            try:
                symbol_key = detector.binance_symbol.lower()
                has_active = any(
                    not tracker["closed"]
                    for tracker in self.active.get(symbol_key, [])
                )
                if detector.mexc_symbol not in self.contract_sizes:
                    await self._get_contract_size(detector.mexc_symbol)
                elif detector.latest_book_ticker is not None and not has_active:
                    started_ns = time.perf_counter_ns()
                    book = await self.fetch_mexc_book(
                        detector.mexc_symbol, entry_priority=False
                    )
                    latency_ms = (
                        time.perf_counter_ns() - started_ns
                    ) / 1_000_000.0
                    bids = book.get("bids") or []
                    asks = book.get("asks") or []
                    latest = detector.latest_book_ticker
                    if bids and asks and latest is not None:
                        binance_mid = (
                            latest["best_bid"] + latest["best_ask"]
                        ) / 2.0
                        mexc_mid_scaled = (
                            (float(bids[0][0]) + float(asks[0][0])) / 2.0
                        ) * detector.divisor
                        basis_bps = 10_000.0 * (
                            binance_mid / mexc_mid_scaled - 1.0
                        )
                        if abs(basis_bps) <= BASIS_MAX_ABS_BPS:
                            samples = self.basis_samples[detector.binance_symbol]
                            was_ready = len(samples) >= BASIS_MIN_SAMPLES
                            samples.append(basis_bps)
                            self.storage.insert_mexc_basis_sample(
                                timestamp=utcnow_iso(),
                                binance_symbol=detector.binance_symbol,
                                mexc_symbol=detector.mexc_symbol,
                                binance_mid=binance_mid,
                                mexc_mid_scaled=mexc_mid_scaled,
                                basis_bps=basis_bps,
                                request_latency_ms=latency_ms,
                            )
                            if not was_ready and len(samples) >= BASIS_MIN_SAMPLES:
                                print(
                                    f"[BASIS] {detector.binance_symbol} ready "
                                    f"median={statistics.median(samples):+.2f}bps"
                                )
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[BASIS] {detector.binance_symbol} FAILED: {exc!r}"
                )
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=BASIS_INTERVAL_SECONDS
                )
            except asyncio.TimeoutError:
                pass

    def _basis_for(self, binance_symbol):
        samples = self.basis_samples.get(binance_symbol)
        if not samples or len(samples) < BASIS_MIN_SAMPLES:
            return None
        return statistics.median(samples)

    def _record_entry_decision(
        self, sweep_event_id, decision, reason, request_latency_ms,
        basis_bps=None, leader_price=None, expected_mexc_fair=None,
        mexc_entry_vwap=None, raw_edge_bps=None, ioc_limit_price=None,
    ):
        self.storage.insert_entry_decision(
            sweep_event_id=sweep_event_id,
            timestamp=utcnow_iso(),
            decision=decision,
            reason=reason,
            required_edge_bps=REQUIRED_EDGE_BPS,
            basis_bps=basis_bps,
            leader_price=leader_price,
            expected_mexc_fair=expected_mexc_fair,
            mexc_entry_vwap=mexc_entry_vwap,
            raw_edge_bps=raw_edge_bps,
            ioc_limit_price=ioc_limit_price,
            request_latency_ms=request_latency_ms,
        )

    def _skip_entry(
        self, sweep_event_id, tracker, reason, request_latency_ms, **values,
    ):
        self._record_entry_decision(
            sweep_event_id, "skip", reason, request_latency_ms, **values,
        )
        tracker["closed"] = True
        print(
            f"    [IOC SKIP] event={sweep_event_id} reason={reason} "
            f"edge={values.get('raw_edge_bps')}bps"
        )

    async def on_mexc_snapshot(
        self, sweep_event_id, detector, offset_seconds, book,
        request_started, response_received, request_latency_ms,
    ):
        if offset_seconds != 0 or sweep_event_id not in self.event_meta:
            return
        tracker = self.trackers_by_event.get(sweep_event_id)
        if tracker is None:
            return
        try:
            meta = self.event_meta[sweep_event_id]
            direction = meta["direction"]
            latest = detector.latest_book_ticker
            basis_bps = self._basis_for(detector.binance_symbol)
            if tracker["pre_entry_abort_reason"] or tracker["closed"]:
                self._skip_entry(
                    sweep_event_id, tracker,
                    tracker["pre_entry_abort_reason"] or "momentum_expired",
                    request_latency_ms, basis_bps=basis_bps,
                )
                return
            if latest is None:
                self._skip_entry(
                    sweep_event_id, tracker, "missing_binance_bbo",
                    request_latency_ms, basis_bps=basis_bps,
                )
                return
            if basis_bps is None:
                self._skip_entry(
                    sweep_event_id, tracker, "basis_not_ready",
                    request_latency_ms,
                )
                return
            size = self.contract_sizes.get(detector.mexc_symbol)
            if size is None:
                self._skip_entry(
                    sweep_event_id, tracker, "contract_size_not_ready",
                    request_latency_ms, basis_bps=basis_bps,
                )
                return
            levels = (
                book.get("asks") or [] if direction == "up"
                else book.get("bids") or []
            )
            if not levels:
                self._skip_entry(
                    sweep_event_id, tracker, "empty_entry_book",
                    request_latency_ms, basis_bps=basis_bps,
                )
                return

            leader_price = (
                latest["best_bid"] if direction == "up"
                else latest["best_ask"]
            ) / detector.divisor
            basis_factor = 1.0 + basis_bps / 10_000.0
            expected_fair = leader_price / basis_factor
            edge_factor = 1.0 + REQUIRED_EDGE_BPS / 10_000.0
            ioc_limit = (
                expected_fair / edge_factor
                if direction == "up" else expected_fair * edge_factor
            )
            quantity = PAPER_NOTIONAL / ioc_limit
            unrestricted = fill_base(levels, quantity, size)
            unrestricted_vwap = unrestricted[0] if unrestricted else None
            raw_edge_bps = None
            if unrestricted_vwap is not None:
                raw_edge_bps = (
                    10_000.0 * (expected_fair / unrestricted_vwap - 1.0)
                    if direction == "up"
                    else 10_000.0 * (unrestricted_vwap / expected_fair - 1.0)
                )
            fill = fill_base_ioc(
                levels, quantity, size, direction, ioc_limit
            )
            if fill is None:
                self._skip_entry(
                    sweep_event_id, tracker, "ioc_not_fully_filled",
                    request_latency_ms,
                    basis_bps=basis_bps,
                    leader_price=leader_price,
                    expected_mexc_fair=expected_fair,
                    mexc_entry_vwap=unrestricted_vwap,
                    raw_edge_bps=raw_edge_bps,
                    ioc_limit_price=ioc_limit,
                )
                return
            entry_vwap, entry_quote = fill
            raw_edge_bps = (
                10_000.0 * (expected_fair / entry_vwap - 1.0)
                if direction == "up"
                else 10_000.0 * (entry_vwap / expected_fair - 1.0)
            )
            if raw_edge_bps < REQUIRED_EDGE_BPS:
                self._skip_entry(
                    sweep_event_id, tracker, "edge_below_required",
                    request_latency_ms,
                    basis_bps=basis_bps,
                    leader_price=leader_price,
                    expected_mexc_fair=expected_fair,
                    mexc_entry_vwap=entry_vwap,
                    raw_edge_bps=raw_edge_bps,
                    ioc_limit_price=ioc_limit,
                )
                return
            entry_fee = entry_quote * PAPER_ENTRY_FEE
            self.paper_entries[sweep_event_id] = {
                "direction": direction,
                "quantity": quantity,
                "size": size,
                "entry_quote": entry_quote,
                "open": True,
            }
            self._record_entry_decision(
                sweep_event_id, "fill", "ioc_full_fill_with_edge",
                request_latency_ms,
                basis_bps=basis_bps,
                leader_price=leader_price,
                expected_mexc_fair=expected_fair,
                mexc_entry_vwap=entry_vwap,
                raw_edge_bps=raw_edge_bps,
                ioc_limit_price=ioc_limit,
            )
            self.storage.insert_paper_position(
                sweep_event_id=sweep_event_id,
                policy_name=PAPER_POLICY,
                direction=direction,
                notional=PAPER_NOTIONAL,
                entry_time=response_received,
                entry_vwap=entry_vwap,
                base_quantity=quantity,
                entry_quote=entry_quote,
                entry_fee=entry_fee,
            )
            print(
                f"    [PAPER OPEN] event={sweep_event_id} "
                f"{direction.upper()} ${entry_quote:.2f} "
                f"vwap={entry_vwap:.8g} edge={raw_edge_bps:.1f}bps "
                f"fee=${entry_fee:.2f} "
                f"latency={request_latency_ms:.1f}ms"
            )
            for pending in sorted(
                self.pending_exits.pop(sweep_event_id, []),
                key=lambda item: item[0],
            ):
                self._apply_paper_exit(sweep_event_id, *pending[1:])
        except Exception as exc:  # noqa: BLE001
            tracker["closed"] = True
            self._record_entry_decision(
                sweep_event_id, "skip", f"entry_error:{type(exc).__name__}",
                request_latency_ms,
            )
            print(f"    [PAPER OPEN] event={sweep_event_id} FAILED: {exc!r}")

    def _apply_paper_exit(self, event_id, reason, book, exit_time, exit_offset):
        entry = self.paper_entries.get(event_id)
        if entry is None:
            self.pending_exits[event_id].append(
                (exit_offset, reason, book, exit_time, exit_offset)
            )
            return
        if not entry["open"]:
            return
        levels = (
            book.get("bids") or [] if entry["direction"] == "up"
            else book.get("asks") or []
        )
        fill = fill_base(levels, entry["quantity"], entry["size"])
        if fill is None:
            raise RuntimeError("$2000 exit cannot fill from recorded depth")
        exit_vwap, exit_quote = fill
        gross = (
            exit_quote - entry["entry_quote"]
            if entry["direction"] == "up"
            else entry["entry_quote"] - exit_quote
        )
        entry_fee = entry["entry_quote"] * PAPER_ENTRY_FEE
        exit_fee = exit_quote * PAPER_EXIT_FEE
        net = gross - entry_fee - exit_fee
        self.storage.close_paper_position(
            sweep_event_id=event_id,
            policy_name=PAPER_POLICY,
            close_reason=reason,
            exit_time=exit_time,
            exit_offset_s=exit_offset,
            exit_vwap=exit_vwap,
            exit_quote=exit_quote,
            exit_fee=exit_fee,
            gross_pnl=gross,
            net_pnl=net,
        )
        entry["open"] = False
        print(
            f"    [PAPER CLOSE] event={event_id} reason={reason} "
            f"hold={exit_offset:.3f}s gross=${gross:+.2f} "
            f"fees=${entry_fee + exit_fee:.2f} net=${net:+.2f}"
        )

    @staticmethod
    def _move_bps(tracker, best_bid, best_ask):
        if tracker["direction"] == "up":
            return 10_000.0 * (best_bid / tracker["signal_bid"] - 1.0)
        return 10_000.0 * (1.0 - best_ask / tracker["signal_ask"])

    def _latest_context(self, tracker, detector):
        latest = detector.latest_book_ticker
        if latest is None:
            return None
        current = self._move_bps(
            tracker, latest["best_bid"], latest["best_ask"]
        )
        state = detector.current_book_state(
            tracker["direction"], tracker["signal_bid"], tracker["signal_ask"]
        ) or {}
        return latest, current, state

    def _trigger_exit(
        self, tracker, detector, reason, confirmation_updates=1,
    ):
        if tracker["exit_inflight"] or tracker["closed"]:
            return
        context = self._latest_context(tracker, detector)
        if context is None:
            return
        latest, current, state = context
        offset = max(0.0, time.monotonic() - tracker["started"])
        frozen = {
            "event_id": tracker["event_id"],
            "offset": offset,
            "reason": reason,
            "best_bid": latest["best_bid"],
            "best_ask": latest["best_ask"],
            "peak_bps": tracker["peak_bps"],
            "current_bps": current,
            "book_imbalance": state.get("book_imbalance"),
            "directional_pressure": state.get("directional_pressure"),
            "refill_usd": state.get("refill_usd"),
            "confirmation_updates": confirmation_updates,
        }
        self.storage.insert_binance_exit_state(
            sweep_event_id=frozen["event_id"],
            rule_name=reason,
            trigger_offset_s=offset,
            timestamp=utcnow_iso(),
            best_bid=frozen["best_bid"],
            best_ask=frozen["best_ask"],
            peak_move_bps=frozen["peak_bps"],
            current_move_bps=frozen["current_bps"],
            book_imbalance=frozen["book_imbalance"],
            directional_pressure=frozen["directional_pressure"],
            refill_usd=frozen["refill_usd"],
            confirmation_updates=confirmation_updates,
        )
        if tracker["event_id"] not in self.paper_entries:
            tracker["pre_entry_abort_reason"] = reason
            tracker["closed"] = True
            print(
                f"    [PRE-ENTRY ABORT] event={tracker['event_id']} "
                f"reason={reason}"
            )
            return
        tracker["exit_inflight"] = True
        self._spawn(self._snapshot_mexc_exit(tracker, detector, frozen))

    async def _snapshot_mexc_exit(self, tracker, detector, context):
        try:
            request_started = utcnow_iso()
            started_ns = time.perf_counter_ns()
            book = await self.fetch_mexc_book(
                detector.mexc_symbol, entry_priority=True
            )
            latency_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0
            response_received = utcnow_iso()
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            self.storage.insert_mexc_exit_snapshot(
                sweep_event_id=context["event_id"],
                rule_name=context["reason"],
                trigger_offset_s=context["offset"],
                request_started=request_started,
                response_received=response_received,
                request_latency_ms=latency_ms,
                server_timestamp=book.get("timestamp"),
                binance_best_bid=context["best_bid"],
                binance_best_ask=context["best_ask"],
                peak_move_bps=context["peak_bps"],
                current_move_bps=context["current_bps"],
                net_flow_usd=None,
                bids_json=json.dumps(bids, separators=(",", ":")),
                asks_json=json.dumps(asks, separators=(",", ":")),
            )
            self._apply_paper_exit(
                context["event_id"], context["reason"], book,
                response_received, context["offset"],
            )
            tracker["closed"] = True
            print(
                f"    [BINANCE EXIT] event={context['event_id']} "
                f"reason={context['reason']} peak={context['peak_bps']:.1f}bps "
                f"now={context['current_bps']:.1f}bps MEXC={latency_ms:.1f}ms"
            )
        except Exception as exc:  # noqa: BLE001
            tracker["exit_inflight"] = False
            print(
                f"    [BINANCE EXIT] event={context['event_id']} "
                f"reason={context['reason']} FAILED: {exc!r}"
            )

    def _record_trajectory(self, tracker, latest, current):
        offset = max(0.0, time.monotonic() - tracker["started"])
        # Sampling cadence affects storage only, never entry/exit decisions.
        interval = 0.1 if offset < 60 else 0.5
        if offset - tracker["last_sample"] < interval:
            return
        self.storage.insert_binance_trajectory_point(
            sweep_event_id=tracker["event_id"],
            offset_seconds=offset,
            timestamp=latest["received_at"],
            exchange_event_ms=latest["exchange_event_ms"],
            best_bid=latest["best_bid"],
            best_ask=latest["best_ask"],
            bid_quantity=latest["bid_quantity"],
            ask_quantity=latest["ask_quantity"],
        )
        tracker["last_sample"] = offset

    def _update_active_price(self, symbol_key, detector):
        latest = detector.latest_book_ticker
        trackers = self.active.get(symbol_key, [])
        if latest is None or not trackers:
            return
        keep = []
        for tracker in trackers:
            if tracker["closed"]:
                continue
            current = self._move_bps(
                tracker, latest["best_bid"], latest["best_ask"]
            )
            price = (
                latest["best_bid"] if tracker["direction"] == "up"
                else latest["best_ask"]
            )
            favorable = (
                price > tracker["favorable_price"]
                if tracker["direction"] == "up"
                else price < tracker["favorable_price"]
            )
            adverse = (
                price < tracker["favorable_price"]
                if tracker["direction"] == "up"
                else price > tracker["favorable_price"]
            )
            if favorable:
                tracker["favorable_price"] = price
                tracker["last_progress"] = time.monotonic()
                tracker["peak_bps"] = max(tracker["peak_bps"], current)
            self._record_trajectory(tracker, latest, current)
            if adverse:
                self._trigger_exit(
                    tracker, detector, "first_adverse_bbo_tick"
                )
            keep.append(tracker)
        if keep:
            self.active[symbol_key] = keep
        else:
            self.active.pop(symbol_key, None)

    def _update_active_depth(self, symbol_key, detector):
        # Full depth remains part of entry detection. Exit momentum is measured
        # from executable Binance BBO progress; raw depth refill can be a normal
        # re-quote and must not close a position before price actually stalls.
        return

    async def _momentum_watchdog(self, tracker, detector):
        poll_seconds = MOMENTUM_WATCHDOG_MS / 1000.0
        while not tracker["closed"]:
            await asyncio.sleep(poll_seconds)
            if tracker["exit_inflight"]:
                continue
            stalled_ms = (
                time.monotonic() - tracker["last_progress"]
            ) * 1000.0
            if stalled_ms >= MOMENTUM_STALL_MS:
                self._trigger_exit(
                    tracker, detector, "no_new_favorable_bbo_200ms"
                )

    def _combined_url(self):
        streams = []
        for pair in config.SYMBOL_PAIRS:
            symbol = pair["binance"].lower()
            streams.extend((
                f"{symbol}@depth{config.DEPTH_LEVELS}@100ms",
                f"{symbol}@bookTicker",
            ))
        return f"{config.BINANCE_WS_BASE}?streams={'/'.join(streams)}"

    def _trade_url(self):
        streams = "/".join(
            f"{pair['binance'].lower()}@trade" for pair in config.SYMBOL_PAIRS
        )
        return f"{config.BINANCE_WS_BASE}?streams={streams}"

    async def _run_trade_stream(self):
        backoff = config.RECONNECT_INITIAL_BACKOFF
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    self._trade_url(), ssl=SSL_CONTEXT,
                    ping_interval=20, ping_timeout=20,
                ) as ws:
                    print("[TRADE WS] connected separately.")
                    backoff = config.RECONNECT_INITIAL_BACKOFF
                    while not self._stop.is_set():
                        message = json.loads(await ws.recv())
                        data = message.get("data", message)
                        stream = message.get("stream", "")
                        detector = self.detectors.get(stream.split("@")[0])
                        if detector is not None:
                            detector.update_trade(
                                float(data["p"]), float(data["q"]),
                                bool(data.get("m")),
                            )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                if self._stop.is_set():
                    break
                print(
                    f"[TRADE WS] disconnected ({exc!r}); "
                    f"reconnecting in {backoff:.1f}s"
                )
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, config.RECONNECT_MAX_BACKOFF)

    async def run_binance(self):
        trade_task = asyncio.create_task(self._run_trade_stream())
        try:
            await super().run_binance()
        finally:
            trade_task.cancel()
            await asyncio.gather(trade_task, return_exceptions=True)

    def _on_message(self, raw):
        message = json.loads(raw)
        data = message.get("data", message)
        stream = message.get("stream", "")
        stream_lower = stream.lower()
        symbol_key = stream.split("@")[0] if stream else None
        detector = self.detectors.get(symbol_key)
        if detector is None:
            return

        if "@depth" in stream_lower:
            detector.update_depth_cache(
                parse_levels(data.get("b") or []),
                parse_levels(data.get("a") or []),
            )
            self._update_active_depth(symbol_key, detector)
            return
        if "@bookticker" not in stream_lower:
            return

        event = detector.update_book_ticker(
            float(data["b"]), float(data.get("B", 0.0)),
            float(data["a"]), float(data.get("A", 0.0)),
            exchange_event_ms=data.get("E"),
        )
        if event is not None:
            self.handle_event(event, detector)
        self._update_active_price(symbol_key, detector)


async def main():
    app = OrderBookShadowLogger()
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
