"""
SQLite storage for sweep events and their follow-up MEXC snapshots.

Two tables, kept intentionally flat so they load straight into pandas:

    import sqlite3, pandas as pd
    con = sqlite3.connect("sweep_events.db")
    events = pd.read_sql("SELECT * FROM sweep_events", con)
    snaps  = pd.read_sql("SELECT * FROM mexc_snapshots", con)
"""

import sqlite3
import threading

import config


class Storage:
    def __init__(self, db_path: str = config.DB_PATH):
        # check_same_thread=False: we only touch it from the event loop thread,
        # but asyncio may hop threads for scheduled callbacks; the lock serializes.
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._lock = threading.Lock()
        self._create_tables()

    def _create_tables(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sweep_events (
                    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp            TEXT    NOT NULL,   -- ISO8601 UTC, detection time
                    binance_symbol       TEXT    NOT NULL,
                    mexc_symbol          TEXT    NOT NULL,
                    direction            TEXT    NOT NULL,   -- 'up' | 'down'
                    consumed_usd_volume  REAL    NOT NULL,
                    binance_best_bid     REAL    NOT NULL,
                    binance_best_ask     REAL    NOT NULL
                );

                CREATE TABLE IF NOT EXISTS mexc_snapshots (
                    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                    sweep_event_id         INTEGER NOT NULL,
                    offset_seconds         REAL    NOT NULL,   -- 0,1,3,5,10 ...
                    timestamp              TEXT    NOT NULL,   -- ISO8601 UTC, fetch time
                    mexc_best_bid          REAL,
                    mexc_best_ask          REAL,
                    mexc_mid_price_scaled  REAL,              -- on Binance price scale
                    FOREIGN KEY (sweep_event_id) REFERENCES sweep_events(id)
                );

                CREATE INDEX IF NOT EXISTS idx_snap_event
                    ON mexc_snapshots(sweep_event_id);

                CREATE TABLE IF NOT EXISTS mexc_depth_snapshots (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    sweep_event_id      INTEGER NOT NULL,
                    offset_seconds      REAL    NOT NULL,
                    request_started     TEXT    NOT NULL,
                    response_received   TEXT    NOT NULL,
                    request_latency_ms  REAL    NOT NULL,
                    server_timestamp    INTEGER,
                    bids_json           TEXT    NOT NULL,
                    asks_json           TEXT    NOT NULL,
                    FOREIGN KEY (sweep_event_id) REFERENCES sweep_events(id)
                );

                CREATE INDEX IF NOT EXISTS idx_depth_snap_event
                    ON mexc_depth_snapshots(sweep_event_id);

                CREATE TABLE IF NOT EXISTS lbank_snapshots (
                    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                    sweep_event_id         INTEGER NOT NULL,
                    offset_seconds         REAL    NOT NULL,   -- 0,1,3,5,10 ...
                    timestamp              TEXT    NOT NULL,   -- ISO8601 UTC, fetch time
                    lbank_best_bid         REAL,
                    lbank_best_ask         REAL,
                    lbank_mid_price_scaled REAL,              -- on Binance price scale
                    FOREIGN KEY (sweep_event_id) REFERENCES sweep_events(id)
                );

                CREATE INDEX IF NOT EXISTS idx_lbank_snap_event
                    ON lbank_snapshots(sweep_event_id);

                CREATE TABLE IF NOT EXISTS signal_features (
                    sweep_event_id         INTEGER PRIMARY KEY,
                    signal_source          TEXT    NOT NULL,
                    binance_event_age_ms   REAL,
                    reference_age_ms       REAL,
                    price_move_bps         REAL,
                    levels_crossed         INTEGER,
                    reference_side_usd     REAL,
                    consumed_fraction      REAL,
                    reference_imbalance    REAL,
                    aggressive_trade_count INTEGER,
                    aggressive_buy_usd     REAL,
                    aggressive_sell_usd    REAL,
                    directional_trade_usd  REAL,
                    opposing_trade_usd     REAL,
                    trade_confirmation_ratio REAL,
                    FOREIGN KEY (sweep_event_id) REFERENCES sweep_events(id)
                );

                CREATE TABLE IF NOT EXISTS binance_trajectory_points (
                    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                    sweep_event_id       INTEGER NOT NULL,
                    offset_seconds       REAL    NOT NULL,
                    timestamp            TEXT    NOT NULL,
                    exchange_event_ms    INTEGER,
                    best_bid             REAL    NOT NULL,
                    best_ask             REAL    NOT NULL,
                    bid_quantity         REAL,
                    ask_quantity         REAL,
                    FOREIGN KEY (sweep_event_id) REFERENCES sweep_events(id)
                );

                CREATE INDEX IF NOT EXISTS idx_binance_trajectory_event
                    ON binance_trajectory_points(sweep_event_id, offset_seconds);

                CREATE TABLE IF NOT EXISTS mexc_exit_snapshots (
                    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                    sweep_event_id       INTEGER NOT NULL,
                    rule_name            TEXT    NOT NULL,
                    trigger_offset_s      REAL    NOT NULL,
                    request_started      TEXT    NOT NULL,
                    response_received    TEXT    NOT NULL,
                    request_latency_ms   REAL    NOT NULL,
                    server_timestamp     INTEGER,
                    binance_best_bid     REAL,
                    binance_best_ask     REAL,
                    peak_move_bps        REAL,
                    current_move_bps     REAL,
                    net_flow_usd         REAL,
                    bids_json            TEXT    NOT NULL,
                    asks_json            TEXT    NOT NULL,
                    FOREIGN KEY (sweep_event_id) REFERENCES sweep_events(id),
                    UNIQUE (sweep_event_id, rule_name)
                );

                CREATE INDEX IF NOT EXISTS idx_mexc_exit_event
                    ON mexc_exit_snapshots(sweep_event_id, rule_name);

                CREATE TABLE IF NOT EXISTS paper_positions (
                    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                    sweep_event_id       INTEGER NOT NULL,
                    policy_name          TEXT    NOT NULL,
                    direction            TEXT    NOT NULL,
                    notional             REAL    NOT NULL,
                    status               TEXT    NOT NULL,
                    entry_time           TEXT    NOT NULL,
                    entry_vwap           REAL    NOT NULL,
                    base_quantity        REAL    NOT NULL,
                    entry_quote          REAL    NOT NULL,
                    entry_fee            REAL    NOT NULL,
                    close_reason         TEXT,
                    exit_time            TEXT,
                    exit_offset_s        REAL,
                    exit_vwap            REAL,
                    exit_quote           REAL,
                    exit_fee             REAL,
                    gross_pnl            REAL,
                    net_pnl              REAL,
                    FOREIGN KEY (sweep_event_id) REFERENCES sweep_events(id),
                    UNIQUE (sweep_event_id, policy_name)
                );

                CREATE INDEX IF NOT EXISTS idx_paper_status
                    ON paper_positions(status, policy_name);

                CREATE TABLE IF NOT EXISTS binance_exit_states (
                    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                    sweep_event_id         INTEGER NOT NULL,
                    rule_name              TEXT    NOT NULL,
                    trigger_offset_s       REAL    NOT NULL,
                    timestamp              TEXT    NOT NULL,
                    best_bid               REAL    NOT NULL,
                    best_ask               REAL    NOT NULL,
                    peak_move_bps          REAL    NOT NULL,
                    current_move_bps       REAL    NOT NULL,
                    book_imbalance         REAL,
                    directional_pressure   REAL,
                    refill_usd             REAL,
                    confirmation_updates   INTEGER,
                    FOREIGN KEY (sweep_event_id) REFERENCES sweep_events(id),
                    UNIQUE (sweep_event_id, rule_name)
                );

                CREATE INDEX IF NOT EXISTS idx_binance_exit_state_event
                    ON binance_exit_states(sweep_event_id, rule_name);

                CREATE TABLE IF NOT EXISTS mexc_basis_samples (
                    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp              TEXT    NOT NULL,
                    binance_symbol         TEXT    NOT NULL,
                    mexc_symbol            TEXT    NOT NULL,
                    binance_mid            REAL    NOT NULL,
                    mexc_mid_scaled        REAL    NOT NULL,
                    basis_bps              REAL    NOT NULL,
                    request_latency_ms     REAL    NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_basis_symbol_time
                    ON mexc_basis_samples(binance_symbol, id);

                CREATE TABLE IF NOT EXISTS entry_decisions (
                    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                    sweep_event_id         INTEGER NOT NULL UNIQUE,
                    timestamp              TEXT    NOT NULL,
                    decision               TEXT    NOT NULL,
                    reason                 TEXT    NOT NULL,
                    basis_bps              REAL,
                    leader_price           REAL,
                    expected_mexc_fair     REAL,
                    mexc_entry_vwap        REAL,
                    raw_edge_bps           REAL,
                    required_edge_bps      REAL    NOT NULL,
                    ioc_limit_price        REAL,
                    request_latency_ms     REAL,
                    FOREIGN KEY (sweep_event_id) REFERENCES sweep_events(id)
                );

                CREATE INDEX IF NOT EXISTS idx_entry_decision
                    ON entry_decisions(decision, reason);
                """
            )
            feature_columns = {
                row[1] for row in self._conn.execute(
                    "PRAGMA table_info(signal_features)"
                ).fetchall()
            }
            if "aggressive_trade_count" not in feature_columns:
                self._conn.execute(
                    "ALTER TABLE signal_features "
                    "ADD COLUMN aggressive_trade_count INTEGER"
                )
            new_feature_columns = {
                "aggressive_buy_usd": "REAL",
                "aggressive_sell_usd": "REAL",
                "directional_trade_usd": "REAL",
                "opposing_trade_usd": "REAL",
                "trade_confirmation_ratio": "REAL",
            }
            for column, column_type in new_feature_columns.items():
                if column not in feature_columns:
                    self._conn.execute(
                        f"ALTER TABLE signal_features "
                        f"ADD COLUMN {column} {column_type}"
                    )
            self._conn.commit()

    def insert_sweep_event(
        self,
        timestamp: str,
        binance_symbol: str,
        mexc_symbol: str,
        direction: str,
        consumed_usd_volume: float,
        binance_best_bid: float,
        binance_best_ask: float,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO sweep_events
                    (timestamp, binance_symbol, mexc_symbol, direction,
                     consumed_usd_volume, binance_best_bid, binance_best_ask)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp,
                    binance_symbol,
                    mexc_symbol,
                    direction,
                    consumed_usd_volume,
                    binance_best_bid,
                    binance_best_ask,
                ),
            )
            self._conn.commit()
            return cur.lastrowid

    def insert_mexc_snapshot(
        self,
        sweep_event_id: int,
        offset_seconds: float,
        timestamp: str,
        mexc_best_bid,
        mexc_best_ask,
        mexc_mid_price_scaled,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO mexc_snapshots
                    (sweep_event_id, offset_seconds, timestamp,
                     mexc_best_bid, mexc_best_ask, mexc_mid_price_scaled)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    sweep_event_id,
                    offset_seconds,
                    timestamp,
                    mexc_best_bid,
                    mexc_best_ask,
                    mexc_mid_price_scaled,
                ),
            )
            self._conn.commit()

    def insert_signal_features(
        self,
        sweep_event_id: int,
        signal_source: str,
        binance_event_age_ms,
        reference_age_ms,
        price_move_bps,
        levels_crossed,
        reference_side_usd,
        consumed_fraction,
        reference_imbalance,
        aggressive_trade_count=None,
        aggressive_buy_usd=None,
        aggressive_sell_usd=None,
        directional_trade_usd=None,
        opposing_trade_usd=None,
        trade_confirmation_ratio=None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO signal_features
                    (sweep_event_id, signal_source, binance_event_age_ms,
                     reference_age_ms, price_move_bps, levels_crossed,
                     reference_side_usd, consumed_fraction,
                     reference_imbalance, aggressive_trade_count,
                     aggressive_buy_usd, aggressive_sell_usd,
                     directional_trade_usd, opposing_trade_usd,
                     trade_confirmation_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sweep_event_id, signal_source, binance_event_age_ms,
                    reference_age_ms, price_move_bps, levels_crossed,
                    reference_side_usd, consumed_fraction,
                    reference_imbalance,
                    aggressive_trade_count,
                    aggressive_buy_usd,
                    aggressive_sell_usd,
                    directional_trade_usd,
                    opposing_trade_usd,
                    trade_confirmation_ratio,
                ),
            )
            self._conn.commit()

    def insert_lbank_snapshot(
        self,
        sweep_event_id: int,
        offset_seconds: float,
        timestamp: str,
        lbank_best_bid,
        lbank_best_ask,
        lbank_mid_price_scaled,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO lbank_snapshots
                    (sweep_event_id, offset_seconds, timestamp,
                     lbank_best_bid, lbank_best_ask, lbank_mid_price_scaled)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    sweep_event_id,
                    offset_seconds,
                    timestamp,
                    lbank_best_bid,
                    lbank_best_ask,
                    lbank_mid_price_scaled,
                ),
            )
            self._conn.commit()

    def insert_mexc_depth_snapshot(
        self,
        sweep_event_id: int,
        offset_seconds: float,
        request_started: str,
        response_received: str,
        request_latency_ms: float,
        server_timestamp,
        bids_json: str,
        asks_json: str,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO mexc_depth_snapshots
                    (sweep_event_id, offset_seconds, request_started,
                     response_received, request_latency_ms, server_timestamp,
                     bids_json, asks_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sweep_event_id,
                    offset_seconds,
                    request_started,
                    response_received,
                    request_latency_ms,
                    server_timestamp,
                    bids_json,
                    asks_json,
                ),
            )
            self._conn.commit()

    def insert_binance_trajectory_point(
        self,
        sweep_event_id: int,
        offset_seconds: float,
        timestamp: str,
        exchange_event_ms,
        best_bid: float,
        best_ask: float,
        bid_quantity,
        ask_quantity,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO binance_trajectory_points
                    (sweep_event_id, offset_seconds, timestamp,
                     exchange_event_ms, best_bid, best_ask,
                     bid_quantity, ask_quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sweep_event_id, offset_seconds, timestamp,
                    exchange_event_ms, best_bid, best_ask,
                    bid_quantity, ask_quantity,
                ),
            )
            self._conn.commit()

    def insert_mexc_exit_snapshot(
        self,
        sweep_event_id: int,
        rule_name: str,
        trigger_offset_s: float,
        request_started: str,
        response_received: str,
        request_latency_ms: float,
        server_timestamp,
        binance_best_bid,
        binance_best_ask,
        peak_move_bps,
        current_move_bps,
        net_flow_usd,
        bids_json: str,
        asks_json: str,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO mexc_exit_snapshots
                    (sweep_event_id, rule_name, trigger_offset_s,
                     request_started, response_received, request_latency_ms,
                     server_timestamp, binance_best_bid, binance_best_ask,
                     peak_move_bps, current_move_bps, net_flow_usd,
                     bids_json, asks_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sweep_event_id, rule_name, trigger_offset_s,
                    request_started, response_received, request_latency_ms,
                    server_timestamp, binance_best_bid, binance_best_ask,
                    peak_move_bps, current_move_bps, net_flow_usd,
                    bids_json, asks_json,
                ),
            )
            self._conn.commit()

    def insert_paper_position(
        self, sweep_event_id, policy_name, direction, notional,
        entry_time, entry_vwap, base_quantity, entry_quote, entry_fee,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO paper_positions
                    (sweep_event_id, policy_name, direction, notional, status,
                     entry_time, entry_vwap, base_quantity, entry_quote,
                     entry_fee)
                VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?)
                """,
                (
                    sweep_event_id, policy_name, direction, notional,
                    entry_time, entry_vwap, base_quantity, entry_quote,
                    entry_fee,
                ),
            )
            self._conn.commit()

    def close_paper_position(
        self, sweep_event_id, policy_name, close_reason, exit_time,
        exit_offset_s, exit_vwap, exit_quote, exit_fee, gross_pnl, net_pnl,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE paper_positions
                SET status='closed', close_reason=?, exit_time=?,
                    exit_offset_s=?, exit_vwap=?, exit_quote=?, exit_fee=?,
                    gross_pnl=?, net_pnl=?
                WHERE sweep_event_id=? AND policy_name=? AND status='open'
                """,
                (
                    close_reason, exit_time, exit_offset_s, exit_vwap,
                    exit_quote, exit_fee, gross_pnl, net_pnl,
                    sweep_event_id, policy_name,
                ),
            )
            self._conn.commit()

    def insert_binance_exit_state(
        self, sweep_event_id, rule_name, trigger_offset_s, timestamp,
        best_bid, best_ask, peak_move_bps, current_move_bps,
        book_imbalance=None, directional_pressure=None, refill_usd=None,
        confirmation_updates=None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO binance_exit_states
                    (sweep_event_id, rule_name, trigger_offset_s, timestamp,
                     best_bid, best_ask, peak_move_bps, current_move_bps,
                     book_imbalance, directional_pressure, refill_usd,
                     confirmation_updates)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sweep_event_id, rule_name, trigger_offset_s, timestamp,
                    best_bid, best_ask, peak_move_bps, current_move_bps,
                    book_imbalance, directional_pressure, refill_usd,
                    confirmation_updates,
                ),
            )
            self._conn.commit()

    def insert_mexc_basis_sample(
        self, timestamp, binance_symbol, mexc_symbol, binance_mid,
        mexc_mid_scaled, basis_bps, request_latency_ms,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO mexc_basis_samples
                    (timestamp, binance_symbol, mexc_symbol, binance_mid,
                     mexc_mid_scaled, basis_bps, request_latency_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp, binance_symbol, mexc_symbol, binance_mid,
                    mexc_mid_scaled, basis_bps, request_latency_ms,
                ),
            )
            self._conn.commit()

    def insert_entry_decision(
        self, sweep_event_id, timestamp, decision, reason,
        required_edge_bps, basis_bps=None, leader_price=None,
        expected_mexc_fair=None, mexc_entry_vwap=None, raw_edge_bps=None,
        ioc_limit_price=None, request_latency_ms=None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO entry_decisions
                    (sweep_event_id, timestamp, decision, reason, basis_bps,
                     leader_price, expected_mexc_fair, mexc_entry_vwap,
                     raw_edge_bps, required_edge_bps, ioc_limit_price,
                     request_latency_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sweep_event_id, timestamp, decision, reason, basis_bps,
                    leader_price, expected_mexc_fair, mexc_entry_vwap,
                    raw_edge_bps, required_edge_bps, ioc_limit_price,
                    request_latency_ms,
                ),
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
