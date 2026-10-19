"""LBank Futures shadow COLLECTOR (no trading).

On each Binance sweep, snapshot LBank Futures depth at offsets
[0, 0.25, 0.5, 1, 2, 5, 10] s. Purpose: measure the TRUE LBank lag magnitude and
convergence horizon. Hypothesis (from a known profitable LBank bot): LBank is
thinner and "freezes" more than MEXC, so its lag is BIGGER and takes LONGER to
close than MEXC's ~0.5-1s -- which the 2s-cap paper bot would have cut off.

Public data only; never trades. Writes to sweep_events_lbank_collect.db
(sweep_events + side table lbank_snap).
"""

import asyncio
import json
import signal
import sqlite3
import time

import config

config.DB_PATH = "sweep_events_lbank_collect.db"
config.SWEEP_THRESHOLDS_FILE = "bookticker_thresholds.json"
config.DETECTION_WINDOW_MS = 100.0
config.CONSUMED_USD_THRESHOLD = 100_000.0     # bigger sweeps -> clearer signal, less LBank load
config.MEXC_SNAPSHOT_OFFSETS = []
config.LBANK_MAX_CONCURRENT_REQUESTS = 5

LBANK_UNIVERSE = [
    "ADAUSDT", "AKEUSDT", "ALLOUSDT", "APTUSDT", "ARBUSDT", "ASTERUSDT", "AVAXUSDT",
    "BANKUSDT", "BCHUSDT", "BEATUSDT", "BILLUSDT", "BSBUSDT", "BTWUSDT", "CHIPUSDT",
    "COAIUSDT", "CRVUSDT", "DASHUSDT", "DEXEUSDT", "DOGEUSDT", "EDENUSDT", "ENAUSDT",
    "ESPORTSUSDT", "ETHFIUSDT", "EVAAUSDT", "FARTCOINUSDT", "FHEUSDT", "FOLKSUSDT",
    "HBARUSDT", "HYPEUSDT", "ICPUSDT", "JASMYUSDT", "KASUSDT", "LABUSDT", "LDOUSDT",
    "LIGHTUSDT", "LINKUSDT", "LTCUSDT", "LYNUSDT", "MEGAUSDT", "MYXUSDT", "ONDOUSDT",
    "ORDIUSDT", "PENGUUSDT", "PIPPINUSDT", "POLUSDT", "POWERUSDT", "RAVEUSDT",
    "REUSDT", "RIVERUSDT", "SIRENUSDT", "SKYAIUSDT", "SOLUSDT", "STOUSDT", "SUIUSDT",
    "SYNUSDT", "TRADOORUSDT", "UNIUSDT", "VELVETUSDT", "VVVUSDT", "WIFUSDT", "WLDUSDT",
    "WLFIUSDT", "XLMUSDT", "XPLUSDT", "XRPUSDT", "ZECUSDT", "ZROUSDT",
]
config.SYMBOL_PAIRS = [
    {"binance": s, "mexc": s, "binance_price_divisor": 1,
     "lbank": s, "lbank_price_divisor": 1}
    for s in LBANK_UNIVERSE
]

from main import ArbLogger, utcnow_iso  # noqa: E402

OFFSETS = [0.0, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0]
LBANK_FUT_URL = "https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketOrder"


class LBankCollector(ArbLogger):
    def __init__(self):
        super().__init__()
        self._meta = sqlite3.connect(config.DB_PATH, timeout=5.0)
        self._meta.execute("PRAGMA journal_mode=WAL")
        self._meta.execute("PRAGMA busy_timeout=5000")
        self._meta.execute(
            "create table if not exists lbank_snap(sweep_event_id integer,"
            " offset_s real, ts text, bid real, ask real,"
            " asks_json text, bids_json text)"
        )
        self._meta.commit()

    async def _lbank_depth(self, detector):
        async with self._lbank_sem:
            async with self._http.get(
                LBANK_FUT_URL,
                params={"symbol": detector.lbank_symbol, "depth": 5},
                timeout=8,
            ) as resp:
                text = await resp.text()
        d = json.loads(text).get("data") or {}
        return d.get("asks") or [], d.get("bids") or []

    async def _snap(self, event_id, detector, offset):
        if offset > 0:
            await asyncio.sleep(offset)
        try:
            asks, bids = await self._lbank_depth(detector)
            bid = float(bids[0]["price"]) if bids else None
            ask = float(asks[0]["price"]) if asks else None
            self._meta.execute(
                "insert into lbank_snap values(?,?,?,?,?,?,?)",
                (event_id, offset, utcnow_iso(), bid, ask,
                 json.dumps(asks, separators=(",", ":")),
                 json.dumps(bids, separators=(",", ":"))),
            )
            self._meta.commit()
        except Exception:  # noqa: BLE001
            pass

    def handle_event(self, event, detector):
        event_id = self.storage.insert_sweep_event(
            timestamp=event["timestamp"], binance_symbol=event["binance_symbol"],
            mexc_symbol=event["mexc_symbol"], direction=event["direction"],
            consumed_usd_volume=event["consumed_usd_volume"],
            binance_best_bid=event["binance_best_bid"],
            binance_best_ask=event["binance_best_ask"],
        )
        print(f"[SWEEP #{event_id}] {event['binance_symbol']:<12} "
              f"{event['direction'].upper():<4} ${event['consumed_usd_volume']:,.0f}"
              f" -> snap LBank x{len(OFFSETS)}")
        for o in OFFSETS:
            self._spawn(self._snap(event_id, detector, o))
        return event_id

    async def keep_mexc_warm(self):
        # No basis/keep-alive needed for pure collection.
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=5.0)
                break
            except asyncio.TimeoutError:
                pass


async def main():
    app = LBankCollector()
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
