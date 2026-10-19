"""
Read-only analysis of collected sweep data: do MEXC and LBank follow the Binance
sweep, and with what lag?

For every sweep event we recorded the Binance best bid/ask at detection time and
then sampled the MEXC book and (where a pair exists) the LBank book at offsets
0/1/3/5/10s. This script computes, for EACH follower exchange independently, per
offset:

  * gap_*   - how far the follower mid sits from the Binance mid AT THE SWEEP
              MOMENT (signed follower-minus-Binance), in absolute price and %.
              This is the standing cross-exchange divergence.

  * move_*  - how far the follower mid has moved since the sweep instant
              (offset 0), re-signed so POSITIVE = moved in the sweep's direction
              (up-sweep -> rising, down-sweep -> falling). This is the "did it
              follow, and by how much" signal. Offset 0 is the baseline, so its
              move is 0 by construction.

  * pct_moved_expected - share of events whose follower mid moved in the expected
              (sweep) direction by that offset.

The two exchanges are reported side by side (MEXC block, then LBANK block), both
overall (symbol = ALL) and per symbol. Symbols with no LBank pair (e.g. ZEC)
simply have no LBank snapshots and are skipped from the LBank block. Everything
is printed to the console and saved to analysis_summary.csv with an `exchange`
column so the three-way comparison loads straight into pandas.

Usage:
    python analyze.py [path/to/sweep_events.db]

This script only reads the DB; it never writes to it.
"""

import os
import sys
import sqlite3

import pandas as pd

import config


# Column order for the CSV (full precision) and, trimmed, the console.
CSV_COLUMNS = [
    "symbol", "offset_seconds", "n",
    "gap_abs_mean", "gap_pct_mean", "gap_pct_median", "gap_pct_std",
    "move_abs_mean", "move_abs_median",
    "move_pct_mean", "move_pct_median", "move_pct_std",
    "pct_moved_expected",
]
CONSOLE_COLUMNS = [
    "symbol", "offset_seconds", "n",
    "gap_pct_mean", "move_pct_mean", "move_pct_median", "move_pct_std",
    "pct_moved_expected",
]

# Which follower exchanges to analyze: (label, snapshot table, mid-price column).
EXCHANGES = [
    ("MEXC", "mexc_snapshots", "mexc_mid_price_scaled"),
    ("LBANK", "lbank_snapshots", "lbank_mid_price_scaled"),
]


def load(db_path):
    """Load sweep_events plus each follower's snapshot table into DataFrames."""
    con = sqlite3.connect(db_path)
    try:
        events = pd.read_sql_query("SELECT * FROM sweep_events", con)
        snaps = {}
        for label, table, _mid in EXCHANGES:
            try:
                snaps[label] = pd.read_sql_query(f"SELECT * FROM {table}", con)
            except pd.errors.DatabaseError:
                # Table missing (older DB) — treat as no data for this exchange.
                snaps[label] = pd.DataFrame()
    finally:
        con.close()
    return events, snaps


def build_frame(events, snaps, mid_col):
    """Merge snapshots onto their sweep event and derive the per-row metrics.

    `mid_col` selects which follower's scaled mid price to use, so the same
    metric logic serves MEXC and LBank identically.
    """
    df = snaps.merge(
        events, left_on="sweep_event_id", right_on="id", suffixes=("_snap", "_evt")
    )

    df["binance_mid"] = (df["binance_best_bid"] + df["binance_best_ask"]) / 2.0
    df["dir_sign"] = df["direction"].map({"up": 1.0, "down": -1.0})
    df["mid"] = df[mid_col]

    # Standing cross-exchange gap: follower mid vs Binance mid at the sweep instant.
    df["gap_abs"] = df["mid"] - df["binance_mid"]
    df["gap_pct"] = df["gap_abs"] / df["binance_mid"] * 100.0

    # Follower movement since the sweep instant (offset 0 baseline), re-signed so
    # positive = moved in the sweep direction.
    base = (
        df[df["offset_seconds"] == 0]
        .drop_duplicates("sweep_event_id", keep="last")
        .set_index("sweep_event_id")["mid"]
    )
    df["mid_0"] = df["sweep_event_id"].map(base)
    df["move_abs"] = (df["mid"] - df["mid_0"]) * df["dir_sign"]
    df["move_pct"] = df["move_abs"] / df["binance_mid"] * 100.0
    df["moved_expected"] = df["move_abs"] > 0

    return df


def _aggregate(df, keys):
    g = df.groupby(keys)
    res = g.agg(
        n=("gap_pct", "count"),
        gap_abs_mean=("gap_abs", "mean"),
        gap_pct_mean=("gap_pct", "mean"),
        gap_pct_median=("gap_pct", "median"),
        gap_pct_std=("gap_pct", "std"),
        move_abs_mean=("move_abs", "mean"),
        move_abs_median=("move_abs", "median"),
        move_pct_mean=("move_pct", "mean"),
        move_pct_median=("move_pct", "median"),
        move_pct_std=("move_pct", "std"),
        pct_moved_expected=("moved_expected", "mean"),
    ).reset_index()
    res["pct_moved_expected"] = res["pct_moved_expected"] * 100.0
    return res


def summarize(df):
    overall = _aggregate(df, ["offset_seconds"])
    overall.insert(0, "symbol", "ALL")

    per = _aggregate(df, ["binance_symbol", "offset_seconds"])
    per = per.rename(columns={"binance_symbol": "symbol"})

    out = pd.concat([overall, per], ignore_index=True)
    return out[CSV_COLUMNS]


def summarize_sorted(df):
    """Summary rows sorted: ALL block first, then symbols A–Z, offsets ascending."""
    summary = summarize(df)
    summary["_sym_order"] = (summary["symbol"] != "ALL").astype(int)
    summary = (
        summary.sort_values(["_sym_order", "symbol", "offset_seconds"])
        .drop(columns="_sym_order")
        .reset_index(drop=True)
    )
    return summary


def _fmt_console(summary):
    disp = summary[CONSOLE_COLUMNS].copy()
    disp["n"] = disp["n"].astype(int)
    for col in ["gap_pct_mean", "move_pct_mean", "move_pct_median", "move_pct_std"]:
        disp[col] = disp[col].map(lambda v: f"{v:+.4f}" if pd.notna(v) else "—")
    disp["pct_moved_expected"] = disp["pct_moved_expected"].map(
        lambda v: f"{v:5.1f}%" if pd.notna(v) else "—"
    )
    disp = disp.rename(
        columns={
            "offset_seconds": "offset_s",
            "gap_pct_mean": "gap%_mean",
            "move_pct_mean": "move%_mean",
            "move_pct_median": "move%_med",
            "move_pct_std": "move%_std",
            "pct_moved_expected": "moved_exp",
        }
    )
    return disp


def print_block(summary, label):
    print("\n" + "=" * 78)
    print(f"{label} follow-through vs Binance sweep  (gap = {label}−Binance at sweep;")
    print(f"move = {label} shift since t=0, +signed toward the sweep direction)")
    print("=" * 78)

    console = _fmt_console(summary)
    print("\n--- OVERALL (all symbols) ---")
    print(
        console[console["symbol"] == "ALL"]
        .drop(columns="symbol")
        .to_string(index=False)
    )
    print("\n--- PER SYMBOL ---")
    print(console[console["symbol"] != "ALL"].to_string(index=False))


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else config.DB_PATH
    if not os.path.exists(db_path):
        print(f"[analyze] DB not found: {db_path}")
        print("          Run main.py to collect some sweep events first.")
        return

    events, snaps = load(db_path)
    counts = "  ".join(f"{lbl.lower()}_snapshots={len(snaps[lbl])}" for lbl, _, _ in EXCHANGES)
    print(f"[analyze] DB: {db_path}")
    print(f"[analyze] sweep_events={len(events)}  {counts}")
    if events.empty:
        print("[analyze] No sweep events yet — collect some, then re-run.")
        return

    pd.set_option("display.width", 200)
    pd.set_option("display.max_rows", None)

    # Symbols configured without an LBank pair — noted so their absence from the
    # LBank block is obviously intentional, not a bug.
    no_lbank = [p["binance"] for p in config.SYMBOL_PAIRS if not p.get("lbank")]

    csv_frames = []
    for label, _table, mid_col in EXCHANGES:
        exch_snaps = snaps[label]
        if exch_snaps.empty:
            print(f"\n[analyze] No {label} snapshots yet — skipping {label} block.")
            continue
        summary = summarize_sorted(build_frame(events, exch_snaps, mid_col))
        print_block(summary, label)
        if label == "LBANK" and no_lbank:
            print(f"\n[analyze] (LBank not configured for: {', '.join(no_lbank)} "
                  f"— excluded by design)")
        summary.insert(0, "exchange", label)
        csv_frames.append(summary)

    if not csv_frames:
        print("\n[analyze] No follower snapshots to summarize yet.")
        return

    out_path = "analysis_summary.csv"
    pd.concat(csv_frames, ignore_index=True).to_csv(out_path, index=False)
    print(f"\n[analyze] Saved full-precision summary (exchange column) -> {out_path}")
    print("[analyze] Note: offset 0s is the movement baseline (move≈0 by design);")
    print("          its gap columns still show the initial follower−Binance divergence.")


if __name__ == "__main__":
    main()
