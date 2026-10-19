# Exchange Latency Analyzer

A research tool for measuring **cross-exchange order-book latency** — how quickly a
large price move on one exchange (Binance USD-M futures) shows up on others
(MEXC, LBank). It records synchronized order-book snapshots around significant
"sweep" events and stores them for offline analysis.

It is a **pure market-data collection and analysis tool**. It uses only public
exchange endpoints, needs **no API keys**, and contains **no order-placement or
trading code** of any kind.

## How it works

1. Open **one** combined Binance WebSocket carrying the `depth10@100ms`
   partial-book stream for all configured symbols.
2. Keep a short rolling buffer of recent depth snapshots per symbol.
3. On each update, compare "now" against the snapshot from ~`DETECTION_WINDOW_MS`
   ago and estimate the USD volume consumed on the bid and ask sides.
4. When consumed USD clears a threshold **and** the best price moved the matching
   direction, record a **sweep event** and fetch the counterpart MEXC/LBank order
   books immediately and at fixed time offsets (non-blocking `asyncio` tasks).
5. Everything lands in SQLite; the analysis scripts then quantify how far and how
   fast the other venues followed.

## Architecture

```
            ┌───────────────────────────────────────────┐
            │  Binance USD-M futures depth10@100ms (WS)   │
            └───────────────────┬─────────────────────────┘
                                │ single combined stream
                                ▼
                    ┌───────────────────────┐
                    │        main.py         │
                    │  rolling depth buffer  │
                    │  sweep-event detector  │
                    └───────────┬────────────┘
                   sweep event  │  triggers offset fetches
                                ▼
            ┌───────────────────────────────────────────┐
            │   MEXC / LBank REST order books (public)    │
            │   fetched at t+0, t+Δ, t+2Δ … (async)       │
            └───────────────────┬─────────────────────────┘
                                ▼
                     ┌────────────────────┐
                     │   storage.py        │
                     │   SQLite events DB  │
                     └──────────┬──────────┘
                                ▼
          ┌──────────────────────────────────────────────┐
          │  Offline analysis (pandas)                     │
          │  analyze.py · correlate.py · shadow_analyze.py │
          │  threshold_analysis.py                         │
          └──────────────────────────────────────────────┘
```

Supporting collectors: `bookticker_shadow.py` and `orderbook_shadow.py`
(alternative shadow collectors), `binance_feed_benchmark.py` /
`binance_aggtrade_probe.py` (feed-latency benchmarks), `market_ws_probe.py`
(connection probe), `lbank_collect.py` (LBank depth collector).

## Tech stack

- **Python 3.11**, `asyncio`
- **websockets** + **aiohttp** (public exchange streams / REST)
- **certifi** (TLS roots)
- **SQLite** for event storage
- **pandas** for offline analysis
- Runs 24/7 on a small Linux VPS under `systemd`

## Setup

```bash
git clone https://github.com/anton-bryzhchuk/exchange-latency-analyzer.git
cd exchange-latency-analyzer
python3.11 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env     # optional — only sets the DB file location
python main.py           # start collecting
```

Edit `SYMBOL_PAIRS` and the detection thresholds in `config.py`. After some data
has accumulated, run the analysis scripts, e.g.:

```bash
python analyze.py
python correlate.py
```

### Running 24/7 on a VPS

```ini
[Unit]
Description=Exchange Latency Analyzer
After=network-online.target

[Service]
WorkingDirectory=/opt/exchange-latency-analyzer
ExecStart=/opt/exchange-latency-analyzer/venv/bin/python main.py
Restart=always

[Install]
WantedBy=multi-user.target
```

## Example output

Illustrative sweep-event summary produced by the analysis scripts (sanitized
sample data):

```
symbol      events   median_lag_ms   p90_lag_ms   followed_%
----------------------------------------------------------------
NEARUSDT       412            180          640         78.4
XLMUSDT        355            220          710         74.1
RENDERUSDT     201            260          900         69.8
```

*`median_lag_ms`* = typical delay before the counterpart venue's mid-price moved
in the same direction as the Binance sweep.

## What I learned

- Driving many symbols over a **single** multiplexed WebSocket and keeping a
  time-aligned rolling buffer under load.
- Measuring and reasoning about **propagation latency** between independent
  market-data feeds.
- Designing an append-only SQLite event store and a separate, reproducible
  offline analysis layer (`pandas`).
- Keeping a research codebase strictly read-only and credential-free.

---

*Built with AI-assisted development (Claude Code).*
