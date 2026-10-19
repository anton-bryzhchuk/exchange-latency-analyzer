"""
Configuration for the Binance -> MEXC latency-arbitrage research logger.

Everything you're likely to tune while iterating lives here.
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # dotenv is optional; real env vars still work without it
    pass

# ---------------------------------------------------------------------------
# Symbol mapping
# ---------------------------------------------------------------------------
# Each pair maps a Binance symbol to its counterparts on MEXC Futures and LBank, plus a
# per-exchange price divisor.
#
# `binance_price_divisor` / `lbank_price_divisor`:
#   Divide the Binance price by this number to put it on the SAME numeric scale
#   as the other exchange's price. E.g. Binance "1000PEPEUSDT" quotes 1000x the
#   raw PEPE price, so its divisor is 1000; MEXC/LBank "PEPE" quote the raw
#   price. When we store a snapshot we MULTIPLY the MEXC/LBank price by the
#   relevant divisor so the stored `*_mid_price_scaled` lines up directly with
#   the (raw) Binance best bid/ask we recorded for the event.
#
# `lbank`:
#   LBank spot symbol, lowercase with an underscore ("pepe_usdt"). Set to None
#   for tokens with no active LBank USDT pair (verified via the depth endpoint);
#   LBank fetching is skipped for those, MEXC still runs.

SYMBOL_PAIRS = [
    # --- original 6 ---
    {"binance": "1000PEPEUSDT", "mexc": "PEPEUSDT",   "binance_price_divisor": 1000, "lbank": "pepe_usdt",   "lbank_price_divisor": 1000},
    {"binance": "AKEUSDT",      "mexc": "AKEUSDT",     "binance_price_divisor": 1,    "lbank": "ake_usdt",    "lbank_price_divisor": 1},
    {"binance": "FHEUSDT",      "mexc": "FHEUSDT",     "binance_price_divisor": 1,    "lbank": "fhe_usdt",    "lbank_price_divisor": 1},
    {"binance": "XLMUSDT",      "mexc": "XLMUSDT",     "binance_price_divisor": 1,    "lbank": "xlm_usdt",    "lbank_price_divisor": 1},
    {"binance": "WLFIUSDT",     "mexc": "WLFIUSDT",    "binance_price_divisor": 1,    "lbank": "wlfi_usdt",   "lbank_price_divisor": 1},
    {"binance": "PIPPINUSDT",   "mexc": "PIPPINUSDT",  "binance_price_divisor": 1,    "lbank": "pippin_usdt", "lbank_price_divisor": 1},
    # --- +8 ---
    {"binance": "BANKUSDT",     "mexc": "BANKUSDT",    "binance_price_divisor": 1,    "lbank": "bank_usdt",   "lbank_price_divisor": 1},
    {"binance": "RENDERUSDT",   "mexc": "RENDERUSDT",  "binance_price_divisor": 1,    "lbank": "render_usdt", "lbank_price_divisor": 1},
    {"binance": "LDOUSDT",      "mexc": "LDOUSDT",     "binance_price_divisor": 1,    "lbank": "ldo_usdt",    "lbank_price_divisor": 1},
    {"binance": "ONDOUSDT",     "mexc": "ONDOUSDT",    "binance_price_divisor": 1,    "lbank": "ondo_usdt",   "lbank_price_divisor": 1},
    {"binance": "SKYAIUSDT",    "mexc": "SKYAIUSDT",   "binance_price_divisor": 1,    "lbank": "skyai_usdt",  "lbank_price_divisor": 1},
    {"binance": "NEARUSDT",     "mexc": "NEARUSDT",    "binance_price_divisor": 1,    "lbank": "near_usdt",   "lbank_price_divisor": 1},
    {"binance": "ASTERUSDT",    "mexc": "ASTERUSDT",   "binance_price_divisor": 1,    "lbank": "aster_usdt",  "lbank_price_divisor": 1},
    {"binance": "BSBUSDT",      "mexc": "BSBUSDT",     "binance_price_divisor": 1,    "lbank": "bsb_usdt",    "lbank_price_divisor": 1},
    # --- +17 ---
    {"binance": "ZECUSDT",      "mexc": "ZECUSDT",     "binance_price_divisor": 1,    "lbank": None,          "lbank_price_divisor": 1},  # no LBank USDT pair
    {"binance": "HYPEUSDT",     "mexc": "HYPEUSDT",    "binance_price_divisor": 1,    "lbank": "hype_usdt",   "lbank_price_divisor": 1},
    {"binance": "XRPUSDT",      "mexc": "XRPUSDT",     "binance_price_divisor": 1,    "lbank": "xrp_usdt",    "lbank_price_divisor": 1},
    {"binance": "AVAXUSDT",     "mexc": "AVAXUSDT",    "binance_price_divisor": 1,    "lbank": "avax_usdt",   "lbank_price_divisor": 1},
    {"binance": "SUIUSDT",      "mexc": "SUIUSDT",     "binance_price_divisor": 1,    "lbank": "sui_usdt",    "lbank_price_divisor": 1},
    {"binance": "TAOUSDT",      "mexc": "TAOUSDT",     "binance_price_divisor": 1,    "lbank": "tao_usdt",    "lbank_price_divisor": 1},
    {"binance": "DOGEUSDT",     "mexc": "DOGEUSDT",    "binance_price_divisor": 1,    "lbank": "doge_usdt",   "lbank_price_divisor": 1},
    {"binance": "PUMPUSDT",     "mexc": "PUMPUSDT",    "binance_price_divisor": 1,    "lbank": "pump_usdt",   "lbank_price_divisor": 1},
    {"binance": "WLDUSDT",      "mexc": "WLDUSDT",     "binance_price_divisor": 1,    "lbank": "wld_usdt",    "lbank_price_divisor": 1},
    {"binance": "LINKUSDT",     "mexc": "LINKUSDT",    "binance_price_divisor": 1,    "lbank": "link_usdt",   "lbank_price_divisor": 1},
    {"binance": "ADAUSDT",      "mexc": "ADAUSDT",     "binance_price_divisor": 1,    "lbank": "ada_usdt",    "lbank_price_divisor": 1},
    {"binance": "AAVEUSDT",     "mexc": "AAVEUSDT",    "binance_price_divisor": 1,    "lbank": "aave_usdt",   "lbank_price_divisor": 1},
    {"binance": "BEATUSDT",     "mexc": "BEATUSDT",    "binance_price_divisor": 1,    "lbank": "beat_usdt",   "lbank_price_divisor": 1},
    {"binance": "UNIUSDT",      "mexc": "UNIUSDT",     "binance_price_divisor": 1,    "lbank": "uni_usdt",    "lbank_price_divisor": 1},
    {"binance": "ENAUSDT",      "mexc": "ENAUSDT",     "binance_price_divisor": 1,    "lbank": "ena_usdt",    "lbank_price_divisor": 1},
    {"binance": "DOTUSDT",      "mexc": "DOTUSDT",     "binance_price_divisor": 1,    "lbank": "dot_usdt",    "lbank_price_divisor": 1},
    {"binance": "BTWUSDT",      "mexc": "BTWUSDT",     "binance_price_divisor": 1,    "lbank": "btw_usdt",    "lbank_price_divisor": 1},
]

# ---------------------------------------------------------------------------
# Detection parameters
# ---------------------------------------------------------------------------
DEPTH_LEVELS = 10            # top-N book levels to watch (Binance stream = depth10)
DETECTION_WINDOW_MS = 400    # compare "now" against the snapshot ~this old
CONSUMED_USD_THRESHOLD = 50_000.0   # min consumed USD volume to call it a sweep

# Ignore repeated fires for the same symbol+direction within this many seconds.
# A single real sweep otherwise re-triggers on every 100ms tick inside the window.
EVENT_COOLDOWN_SECONDS = 5.0

# How long to keep depth snapshots in the rolling per-symbol buffer.
BUFFER_RETENTION_MS = 2_000

# ---------------------------------------------------------------------------
# MEXC follow-up snapshots
# ---------------------------------------------------------------------------
# Offsets (seconds) after a sweep event at which to fetch the MEXC book.
# 0 == immediately at detection time.
MEXC_SNAPSHOT_OFFSETS = [0, 0.25, 0.5, 1, 2, 5, 10]

# ---------------------------------------------------------------------------
# REST rate limiting (per exchange)
# ---------------------------------------------------------------------------
# A single burst of sweeps can otherwise fire dozens of concurrent REST calls
# (up to symbols x offsets), tripping HTTP 429. Cap concurrent in-flight
# requests per exchange with a semaphore, and retry 429s with backoff.
MEXC_MAX_CONCURRENT_REQUESTS = 5
LBANK_MAX_CONCURRENT_REQUESTS = 5

# Backoff schedule (seconds) applied ONLY to HTTP 429 responses. Length also
# sets the max number of retries; other errors are not retried.
RATE_LIMIT_RETRY_BACKOFFS = [0.5, 1.0, 2.0]

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
# NOTE: These symbols (1000PEPEUSDT, AKEUSDT, FHEUSDT, PIPPINUSDT, ...) are
# Binance USD-M *Futures* listings — 4 of the 6 don't exist on spot at all.
# So we use the futures combined stream (fstream). The futures partial-book
# depth payload uses "b"/"a" keys (handled in main.py), unlike spot's
# "bids"/"asks". To switch to spot, use "wss://stream.binance.com:9443/stream".
BINANCE_WS_BASE = "wss://fstream.binance.com/stream"
MEXC_DEPTH_URL = "https://api.mexc.com/api/v1/contract/depth/{symbol}"
LBANK_DEPTH_URL = "https://api.lbkex.com/v1/depth.do"

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------
DB_PATH = os.getenv("DB_PATH", "sweep_events_futures.db")

# ---------------------------------------------------------------------------
# Reconnect backoff (seconds)
# ---------------------------------------------------------------------------
RECONNECT_INITIAL_BACKOFF = 1.0
RECONNECT_MAX_BACKOFF = 30.0
