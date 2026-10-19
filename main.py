"""
Binance -> MEXC latency-arbitrage research logger.

Flow
----
1. Open ONE combined Binance WebSocket connection carrying the depth10@100ms
   partial-book stream for all configured symbols.
2. Per symbol, keep a rolling buffer of recent depth snapshots.
3. On each update, compare "now" against the snapshot ~DETECTION_WINDOW_MS ago
   and estimate the USD volume consumed from the bid side and the ask side.
4. If consumed USD on a side clears the threshold AND the best price moved in the
   matching direction, record a sweep event and immediately + at fixed offsets
   fetch the corresponding MEXC order book (non-blocking asyncio tasks).

All market data comes from public endpoints; no API keys required.
"""

import asyncio
import json
import signal
import ssl
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import certifi
import websockets

import config
from storage import Storage

# macOS Python.framework often ships without wired-up system root certs, which
# makes TLS verification to Binance/MEXC fail. Build an SSL context from the
# bundled certifi CA store so verification works without touching the system.
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_ms() -> float:
    return datetime.now(timezone.utc).timestamp() * 1000.0


def mexc_contract_symbol(symbol: str) -> str:
    """Convert legacy spot-style LDOUSDT into Futures-style LDO_USDT."""
    if "_" not in symbol and symbol.endswith("USDT"):
        return f"{symbol[:-4]}_USDT"
    return symbol


def parse_levels(raw_levels):
    """[["0.0024","10"], ...] -> [(price, qty), ...] as floats."""
    return [(float(p), float(q)) for p, q in raw_levels]


def consumed_usd(old_levels, new_levels):
    """
    Best-effort estimate of USD liquidity consumed on one side between two
    snapshots. For every price present in the OLD book, see how much quantity
    remains at that exact price now; count the reduction as consumed.

    This over-counts levels that merely slid out of the top-N because price
    moved, but that's fine: the caller also requires the best price to have
    moved in the matching direction, which filters out pure drift.
    """
    new_by_price = {price: qty for price, qty in new_levels}
    consumed = 0.0
    for price, old_qty in old_levels:
        new_qty = new_by_price.get(price, 0.0)
        if new_qty < old_qty:
            consumed += (old_qty - new_qty) * price
    return consumed


# ---------------------------------------------------------------------------
# Per-symbol detector
# ---------------------------------------------------------------------------
class SymbolDetector:
    def __init__(self, pair):
        self.pair = pair
        self.binance_symbol = pair["binance"]
        # Config historically used spot-style symbols (e.g. LDOUSDT). The
        # Futures API requires contract-style symbols (e.g. LDO_USDT).
        self.mexc_symbol = mexc_contract_symbol(pair["mexc"])
        self.divisor = pair["binance_price_divisor"]
        self.lbank_symbol = pair.get("lbank")  # None => no LBank pair, skip it
        self.lbank_divisor = pair.get("lbank_price_divisor", 1)
        # buffer of (ts_ms, bids, asks)
        self.buffer = deque()
        # last fire time per direction, for cooldown
        self._last_fire = {"up": 0.0, "down": 0.0}

    def _pick_reference(self, target_ms):
        """Snapshot whose timestamp is closest to `target_ms`."""
        best = None
        best_diff = None
        for snap in self.buffer:
            diff = abs(snap[0] - target_ms)
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best = snap
        return best

    def update(self, bids, asks):
        """
        Feed a new depth snapshot. Returns a sweep-event dict or None.
        bids/asks are lists of (price, qty), best-first.
        """
        ts = now_ms()
        self.buffer.append((ts, bids, asks))

        # prune old snapshots
        cutoff = ts - config.BUFFER_RETENTION_MS
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()

        if not bids or not asks:
            return None

        ref = self._pick_reference(ts - config.DETECTION_WINDOW_MS)
        if ref is None:
            return None
        ref_ts, ref_bids, ref_asks = ref
        # Need a genuinely older snapshot to compare against.
        if ts - ref_ts < config.DETECTION_WINDOW_MS * 0.5:
            return None
        if not ref_bids or not ref_asks:
            return None

        cur_best_bid = bids[0][0]
        cur_best_ask = asks[0][0]
        old_best_bid = ref_bids[0][0]
        old_best_ask = ref_asks[0][0]

        # Ask-side sweep => liquidity taken from asks, best ask rises => price up.
        consumed_ask = consumed_usd(ref_asks, asks)
        # Bid-side sweep => liquidity taken from bids, best bid falls => price down.
        consumed_bid = consumed_usd(ref_bids, bids)

        event = None
        if (
            consumed_ask >= config.CONSUMED_USD_THRESHOLD
            and cur_best_ask > old_best_ask
        ):
            event = self._maybe_fire("up", consumed_ask, cur_best_bid, cur_best_ask, ts)
        elif (
            consumed_bid >= config.CONSUMED_USD_THRESHOLD
            and cur_best_bid < old_best_bid
        ):
            event = self._maybe_fire("down", consumed_bid, cur_best_bid, cur_best_ask, ts)

        return event

    def _maybe_fire(self, direction, consumed, best_bid, best_ask, ts):
        if ts - self._last_fire[direction] < config.EVENT_COOLDOWN_SECONDS * 1000.0:
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
        }


# ---------------------------------------------------------------------------
# The logger app
# ---------------------------------------------------------------------------
class ArbLogger:
    def __init__(self):
        self.storage = Storage(config.DB_PATH)
        self.sweep_thresholds_path = Path(__file__).with_name(
            getattr(config, "SWEEP_THRESHOLDS_FILE", "sweep_thresholds.json")
        )
        with self.sweep_thresholds_path.open(
            encoding="utf-8"
        ) as threshold_file:
            threshold_payload = json.load(threshold_file)
        self.sweep_thresholds = threshold_payload.get("symbols", {})
        self.threshold_floor_usd = float(
            threshold_payload.get("floor_usd", config.CONSUMED_USD_THRESHOLD)
        )
        self.detectors = {}
        for pair in config.SYMBOL_PAIRS:
            # Binance combined-stream names are lowercase.
            stream_key = pair["binance"].lower()
            self.detectors[stream_key] = SymbolDetector(pair)
        self._http = None
        self._stop = asyncio.Event()
        # Per-exchange caps on concurrent in-flight REST calls (429 protection).
        self._mexc_sem = asyncio.Semaphore(config.MEXC_MAX_CONCURRENT_REQUESTS)
        self._lbank_sem = asyncio.Semaphore(config.LBANK_MAX_CONCURRENT_REQUESTS)
        # api.mexc.com currently starts returning code 510 above roughly
        # 10 requests / 2 seconds from this route. Keep one slot of headroom
        # and reserve two slots for new offset=0 entries so older
        # follow-up snapshots cannot starve the time-critical entry snapshot.
        self._mexc_rate_lock = asyncio.Lock()
        self._mexc_all_request_times = deque()
        self._mexc_follow_request_times = deque()
        self._background_tasks = set()
        self._mexc_active_until = 0.0

    def _spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    async def _wait_mexc_rate_slot(self, entry_priority):
        window = 2.05
        total_limit = 9
        follow_limit = 7
        while True:
            delay = 0.05
            async with self._mexc_rate_lock:
                now = time.monotonic()
                cutoff = now - window
                while (self._mexc_all_request_times
                       and self._mexc_all_request_times[0] <= cutoff):
                    self._mexc_all_request_times.popleft()
                while (self._mexc_follow_request_times
                       and self._mexc_follow_request_times[0] <= cutoff):
                    self._mexc_follow_request_times.popleft()

                total_ok = len(self._mexc_all_request_times) < total_limit
                follow_ok = (
                    entry_priority
                    or len(self._mexc_follow_request_times) < follow_limit
                )
                if total_ok and follow_ok:
                    self._mexc_all_request_times.append(now)
                    if not entry_priority:
                        self._mexc_follow_request_times.append(now)
                    return

                waits = []
                if not total_ok:
                    waits.append(
                        window - (now - self._mexc_all_request_times[0])
                    )
                if not follow_ok:
                    waits.append(
                        window - (now - self._mexc_follow_request_times[0])
                    )
                if waits:
                    delay = max(0.01, min(waits))
            await asyncio.sleep(delay)

    # ---- rate-limited REST helper ----------------------------------------
    async def _get_json_rate_limited(self, url, params, semaphore):
        """GET -> parsed JSON, capped by `semaphore` (concurrent in-flight cap)
        and retrying ONLY on HTTP 429 per config.RATE_LIMIT_RETRY_BACKOFFS.
        Non-429 errors (timeouts, connection errors, other HTTP codes) are not
        retried — they propagate immediately, failing fast as before."""
        backoffs = getattr(config, "RATE_LIMIT_RETRY_BACKOFFS", [0.5, 1.0, 2.0])
        async with semaphore:
            for attempt in range(len(backoffs) + 1):
                async with self._http.get(url, params=params, timeout=10) as resp:
                    if resp.status == 429 and attempt < len(backoffs):
                        await asyncio.sleep(backoffs[attempt])
                        continue  # retry the 429
                    resp.raise_for_status()  # raises on 429-after-retries or any other error
                    return await resp.json()

    # ---- MEXC fetching ----------------------------------------------------
    async def fetch_mexc_book(self, mexc_symbol, entry_priority=True):
        mexc_symbol = mexc_contract_symbol(mexc_symbol)
        url = config.MEXC_DEPTH_URL.format(symbol=mexc_symbol)
        backoffs = [0.0, 0.5, 1.0, 2.0]
        for attempt, backoff in enumerate(backoffs):
            if backoff:
                await asyncio.sleep(backoff)
            await self._wait_mexc_rate_slot(entry_priority)
            payload = await self._get_json_rate_limited(
                url, {"limit": config.DEPTH_LEVELS}, self._mexc_sem
            )
            if payload.get("success"):
                return payload.get("data") or {}
            if payload.get("code") != 510 or attempt == len(backoffs) - 1:
                raise RuntimeError(
                    f"MEXC Futures error {payload.get('code')}: "
                    f"{payload.get('message')}"
                )
        raise RuntimeError("MEXC Futures request retries exhausted")

    async def keep_mexc_warm(self):
        """Keep the api.mexc.com TLS connection warm between rare signals."""
        probe = config.SYMBOL_PAIRS[0]["mexc"]
        interval = float(getattr(config, "MEXC_KEEPALIVE_SECONDS", 10.0))
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass
            try:
                if time.monotonic() < self._mexc_active_until:
                    continue
                await self.fetch_mexc_book(probe, entry_priority=False)
            except Exception as exc:  # noqa: BLE001
                print(f"[MEXC] keep-alive FAILED: {exc!r}")

    async def snapshot_mexc(self, sweep_event_id, detector, offset_seconds):
        """Fetch the MEXC book once (after waiting offset_seconds) and store it."""
        if offset_seconds > 0:
            await asyncio.sleep(offset_seconds)
        try:
            request_started = utcnow_iso()
            request_started_ns = time.perf_counter_ns()
            book = await self.fetch_mexc_book(
                detector.mexc_symbol, entry_priority=(offset_seconds == 0)
            )
            request_latency_ms = (
                time.perf_counter_ns() - request_started_ns
            ) / 1_000_000
            response_received = utcnow_iso()
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            best_bid = float(bids[0][0]) if bids else None
            best_ask = float(asks[0][0]) if asks else None

            mid_scaled = None
            if best_bid is not None and best_ask is not None:
                mid = (best_bid + best_ask) / 2.0
                mid_scaled = mid * detector.divisor  # put on Binance price scale

            self.storage.insert_mexc_snapshot(
                sweep_event_id=sweep_event_id,
                offset_seconds=offset_seconds,
                timestamp=utcnow_iso(),
                mexc_best_bid=best_bid,
                mexc_best_ask=best_ask,
                mexc_mid_price_scaled=mid_scaled,
            )
            self.storage.insert_mexc_depth_snapshot(
                sweep_event_id=sweep_event_id,
                offset_seconds=offset_seconds,
                request_started=request_started,
                response_received=response_received,
                request_latency_ms=request_latency_ms,
                server_timestamp=book.get("timestamp"),
                bids_json=json.dumps(bids, separators=(",", ":")),
                asks_json=json.dumps(asks, separators=(",", ":")),
            )
            await self.on_mexc_snapshot(
                sweep_event_id, detector, offset_seconds, book,
                request_started, response_received, request_latency_ms,
            )
            log_offsets = getattr(config, "MEXC_LOG_OFFSETS", None)
            if log_offsets is None or offset_seconds in log_offsets:
                print(
                    f"    [MEXC +{offset_seconds:>4.1f}s] "
                    f"{detector.mexc_symbol:<12} bid={best_bid} ask={best_ask} "
                    f"elapsed={request_latency_ms:.1f}ms"
                )
        except Exception as exc:  # noqa: BLE001 - research tool, keep it running
            print(
                f"    [MEXC +{offset_seconds:>4.1f}s] {detector.mexc_symbol} "
                f"fetch FAILED: {exc!r}"
            )

    async def on_mexc_snapshot(
        self, sweep_event_id, detector, offset_seconds, book,
        request_started, response_received, request_latency_ms,
    ):
        """Subclass hook used by paper executors after a stored snapshot."""
        return

    def schedule_mexc_snapshots(self, sweep_event_id, detector):
        if config.MEXC_SNAPSHOT_OFFSETS:
            self._mexc_active_until = max(
                self._mexc_active_until,
                time.monotonic() + max(config.MEXC_SNAPSHOT_OFFSETS) + 2.0,
            )
        for offset in config.MEXC_SNAPSHOT_OFFSETS:
            self._spawn(
                self.snapshot_mexc(sweep_event_id, detector, offset)
            )

    async def record_binance_trajectory(self, sweep_event_id, detector):
        """Sample the latest Binance BBO while the MEXC move develops."""
        interval = float(getattr(
            config, "BINANCE_TRAJECTORY_INTERVAL_SECONDS", 0.1
        ))
        duration = float(getattr(
            config, "BINANCE_TRAJECTORY_DURATION_SECONDS", 30.0
        ))
        started = time.monotonic()
        index = 0
        while True:
            target = index * interval
            if target > duration + 1e-9:
                break
            delay = started + target - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            latest = getattr(detector, "latest_book_ticker", None)
            if latest:
                actual_offset = max(0.0, time.monotonic() - started)
                self.storage.insert_binance_trajectory_point(
                    sweep_event_id=sweep_event_id,
                    offset_seconds=actual_offset,
                    timestamp=latest["received_at"],
                    exchange_event_ms=latest["exchange_event_ms"],
                    best_bid=latest["best_bid"],
                    best_ask=latest["best_ask"],
                    bid_quantity=latest["bid_quantity"],
                    ask_quantity=latest["ask_quantity"],
                )
            index += 1

    def schedule_binance_trajectory(self, sweep_event_id, detector):
        if getattr(detector, "latest_book_ticker", None) is not None:
            self._spawn(self.record_binance_trajectory(sweep_event_id, detector))

    # ---- LBank fetching ---------------------------------------------------
    async def fetch_lbank_book(self, lbank_symbol):
        # LBank v1 depth: top-level {"asks": [...], "bids": [...]} on success,
        # or {"result": "false", "msg": ...} on error. asks are ascending and
        # bids descending, so index [0] is the best price on each side.
        params = {"symbol": lbank_symbol, "size": config.DEPTH_LEVELS, "merge": 0}
        return await self._get_json_rate_limited(
            config.LBANK_DEPTH_URL, params, self._lbank_sem
        )

    async def snapshot_lbank(self, sweep_event_id, detector, offset_seconds):
        """Fetch the LBank book once (after waiting offset_seconds) and store it."""
        if detector.lbank_symbol is None:
            return  # no LBank pair for this symbol
        if offset_seconds > 0:
            await asyncio.sleep(offset_seconds)
        try:
            book = await self.fetch_lbank_book(detector.lbank_symbol)
            if isinstance(book, dict) and book.get("result") == "false":
                raise RuntimeError(f"LBank API error: {book.get('msg')!r}")
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            best_bid = float(bids[0][0]) if bids else None
            best_ask = float(asks[0][0]) if asks else None

            mid_scaled = None
            if best_bid is not None and best_ask is not None:
                mid = (best_bid + best_ask) / 2.0
                mid_scaled = mid * detector.lbank_divisor  # to Binance price scale

            self.storage.insert_lbank_snapshot(
                sweep_event_id=sweep_event_id,
                offset_seconds=offset_seconds,
                timestamp=utcnow_iso(),
                lbank_best_bid=best_bid,
                lbank_best_ask=best_ask,
                lbank_mid_price_scaled=mid_scaled,
            )
            print(
                f"    [LBANK +{offset_seconds:>4.1f}s] {detector.lbank_symbol:<12} "
                f"bid={best_bid} ask={best_ask} mid_scaled={mid_scaled}"
            )
        except Exception as exc:  # noqa: BLE001 - research tool, keep it running
            print(
                f"    [LBANK +{offset_seconds:>4.1f}s] {detector.lbank_symbol} "
                f"fetch FAILED: {exc!r}"
            )

    def schedule_lbank_snapshots(self, sweep_event_id, detector):
        if detector.lbank_symbol is None:
            return
        for offset in config.MEXC_SNAPSHOT_OFFSETS:
            self._spawn(
                self.snapshot_lbank(sweep_event_id, detector, offset)
            )

    # ---- event handling ---------------------------------------------------
    def _event_tier(self, event):
        thresholds = self.sweep_thresholds.get(event["binance_symbol"])
        if not thresholds:
            return "observe", self.threshold_floor_usd

        consumed = event["consumed_usd_volume"]
        if consumed >= float(thresholds["p99"]):
            return "exceptional", float(thresholds["p90"])
        if consumed >= float(thresholds["p95"]):
            return "strong", float(thresholds["p90"])
        if consumed >= float(thresholds["p90"]):
            return "candidate", float(thresholds["p90"])
        return "observe", float(thresholds["p90"])

    def handle_event(self, event, detector):
        event_id = self.storage.insert_sweep_event(
            timestamp=event["timestamp"],
            binance_symbol=event["binance_symbol"],
            mexc_symbol=event["mexc_symbol"],
            direction=event["direction"],
            consumed_usd_volume=event["consumed_usd_volume"],
            binance_best_bid=event["binance_best_bid"],
            binance_best_ask=event["binance_best_ask"],
        )
        if event.get("signal_source"):
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
        tier, entry_threshold = self._event_tier(event)
        arrow = "UP  ^" if event["direction"] == "up" else "DOWN v"
        print(
            f"\n[SWEEP #{event_id}] {event['timestamp']} "
            f"{event['binance_symbol']:<12} {arrow} "
            f"consumed=${event['consumed_usd_volume']:,.0f} "
            f"tier={tier} p90=${entry_threshold:,.0f} "
            f"bid={event['binance_best_bid']} ask={event['binance_best_ask']}"
        )
        if tier == "observe":
            print("    [SKIP] below symbol p90; no follower REST snapshots")
            return event_id
        self.schedule_mexc_snapshots(event_id, detector)
        self.schedule_binance_trajectory(event_id, detector)
        self.schedule_lbank_snapshots(event_id, detector)
        return event_id

    # ---- Binance stream ---------------------------------------------------
    def _combined_url(self):
        streams = "/".join(
            f"{p['binance'].lower()}@depth{config.DEPTH_LEVELS}@100ms"
            for p in config.SYMBOL_PAIRS
        )
        return f"{config.BINANCE_WS_BASE}?streams={streams}"

    async def run_binance(self):
        url = self._combined_url()
        backoff = config.RECONNECT_INITIAL_BACKOFF
        while not self._stop.is_set():
            try:
                print(f"[WS] connecting to Binance combined stream "
                      f"({len(config.SYMBOL_PAIRS)} symbols)...")
                async with websockets.connect(
                    url, ssl=SSL_CONTEXT, ping_interval=20, ping_timeout=20
                ) as ws:
                    print("[WS] connected.")
                    backoff = config.RECONNECT_INITIAL_BACKOFF  # reset on success
                    while not self._stop.is_set():
                        raw = await ws.recv()
                        self._on_message(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                if self._stop.is_set():
                    break
                print(f"[WS] disconnected ({exc!r}); reconnecting in {backoff:.1f}s")
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, config.RECONNECT_MAX_BACKOFF)

    def _on_message(self, raw):
        msg = json.loads(raw)
        # combined stream envelope: {"stream": "...", "data": {...}}
        data = msg.get("data", msg)
        stream = msg.get("stream", "")
        stream_key = stream.split("@")[0] if stream else None

        detector = self.detectors.get(stream_key)
        if detector is None:
            return

        # Futures partial-book depth uses "b"/"a"; spot uses "bids"/"asks".
        bids = parse_levels(data.get("b") or data.get("bids") or [])
        asks = parse_levels(data.get("a") or data.get("asks") or [])
        event = detector.update(bids, asks)
        if event is not None:
            self.handle_event(event, detector)

    # ---- startup connectivity --------------------------------------------
    async def startup_healthcheck(self):
        """One-time REST ping to MEXC and LBank so all three legs are confirmed
        live at startup (Binance confirms itself via [WS] connected)."""
        # MEXC: use the first pair (all have a MEXC symbol).
        mexc_probe = config.SYMBOL_PAIRS[0]["mexc"]
        try:
            book = await self.fetch_mexc_book(mexc_probe)
            n = len(book.get("bids") or [])
            print(f"[MEXC]  REST OK  (probe {mexc_probe}, {n} bid levels)")
        except Exception as exc:  # noqa: BLE001
            print(f"[MEXC]  REST check FAILED: {exc!r}")

        # LBank: use the first pair that actually has an LBank symbol.
        lbank_probe = next(
            (p["lbank"] for p in config.SYMBOL_PAIRS if p.get("lbank")), None
        )
        if lbank_probe is None:
            print("[LBANK] no LBank pairs configured")
            return
        try:
            book = await self.fetch_lbank_book(lbank_probe)
            if isinstance(book, dict) and book.get("result") == "false":
                raise RuntimeError(book.get("msg"))
            n = len(book.get("bids") or [])
            print(f"[LBANK] REST OK  (probe {lbank_probe}, {n} bid levels)")
        except Exception as exc:  # noqa: BLE001
            print(f"[LBANK] REST check FAILED: {exc!r}")

    # ---- lifecycle --------------------------------------------------------
    async def run(self):
        self._http = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                ssl=SSL_CONTEXT, keepalive_timeout=30.0,
            )
        )
        pairs = config.SYMBOL_PAIRS
        lbank_ok = [p for p in pairs if p.get("lbank")]
        lbank_skipped = [p["binance"] for p in pairs if not p.get("lbank")]
        print("=" * 70)
        print("Latency-arb logger starting")
        print(f"  DB                : {config.DB_PATH}")
        print(f"  Detection window  : {config.DETECTION_WINDOW_MS} ms")
        print(f"  Depth levels      : {config.DEPTH_LEVELS}")
        print(f"  USD threshold     : ${config.CONSUMED_USD_THRESHOLD:,.0f}")
        print(f"  Cooldown          : {config.EVENT_COOLDOWN_SECONDS}s")
        print(f"  Snapshot offsets  : {config.MEXC_SNAPSHOT_OFFSETS}  (MEXC + LBank)")
        print(
            f"  Snapshot gate     : per-symbol p90 from "
            f"{self.sweep_thresholds_path.name}"
        )
        print(f"  Symbol count      : {len(pairs)}")
        print(f"  Symbols           : "
              f"{', '.join(p['binance'] for p in pairs)}")
        print(f"  LBank coverage    : {len(lbank_ok)}/{len(pairs)} pairs"
              + (f"  (skipped: {', '.join(lbank_skipped)})" if lbank_skipped else ""))
        print("=" * 70)
        await self.startup_healthcheck()
        keepalive_task = asyncio.create_task(self.keep_mexc_warm())
        try:
            await self.run_binance()
        finally:
            keepalive_task.cancel()
            await asyncio.gather(keepalive_task, return_exceptions=True)
            for task in tuple(self._background_tasks):
                task.cancel()
            if self._background_tasks:
                await asyncio.gather(
                    *tuple(self._background_tasks), return_exceptions=True
                )
            await self._http.close()
            self.storage.close()
            print("\n[shutdown] closed HTTP + DB. Bye.")

    def request_stop(self):
        self._stop.set()


async def main():
    app = ArbLogger()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, app.request_stop)
        except NotImplementedError:
            pass  # not available on some platforms

    await app.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
