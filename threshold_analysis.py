"""
Read-only analysis of the consumed_usd_volume distribution per symbol.

Liquidity differs enormously across these symbols (XLM vs PEPE vs PIPPIN), so a
single global CONSUMED_USD_THRESHOLD is a blunt instrument. This script prints
the per-symbol distribution of consumed USD volume and suggests a data-driven
per-symbol threshold sitting at the 90th–95th percentile (i.e. keep roughly the
top 5–10% strongest sweeps per symbol).

Usage:
    python threshold_analysis.py [path/to/sweep_events.db]

This script only reads the DB; it never writes to it.
"""

import math
import os
import sys
import sqlite3

import pandas as pd

import config


CSV_COLUMNS = [
    "symbol", "n", "min", "max", "mean", "median",
    "p50", "p75", "p90", "p95", "p99",
    "suggest_p90", "suggest_p95", "recommended",
]


def round_sig(x, sig=2):
    """Round to `sig` significant figures for a readable threshold suggestion."""
    if x is None or not math.isfinite(x) or x == 0:
        return x
    return round(x, -int(math.floor(math.log10(abs(x)))) + (sig - 1))


def _stats_for(series):
    return {
        "n": int(series.count()),
        "min": series.min(),
        "max": series.max(),
        "mean": series.mean(),
        "median": series.median(),
        "p50": series.quantile(0.50),
        "p75": series.quantile(0.75),
        "p90": series.quantile(0.90),
        "p95": series.quantile(0.95),
        "p99": series.quantile(0.99),
    }


def build_table(events):
    rows = []

    # Overall row first.
    overall = _stats_for(events["consumed_usd_volume"])
    overall["symbol"] = "ALL"
    rows.append(overall)

    for symbol, grp in events.groupby("binance_symbol"):
        row = _stats_for(grp["consumed_usd_volume"])
        row["symbol"] = symbol
        rows.append(row)

    df = pd.DataFrame(rows)
    df["suggest_p90"] = df["p90"].map(round_sig)
    df["suggest_p95"] = df["p95"].map(round_sig)
    # Recommended starting point: midpoint of the p90–p95 band, rounded.
    df["recommended"] = df[["p90", "p95"]].mean(axis=1).map(round_sig)

    # ALL first, then symbols alphabetically.
    df["_order"] = (df["symbol"] != "ALL").astype(int)
    df = df.sort_values(["_order", "symbol"]).drop(columns="_order").reset_index(drop=True)
    return df[CSV_COLUMNS]


def _fmt_console(df):
    disp = df.copy()
    money_cols = [
        "min", "max", "mean", "median", "p50", "p75", "p90", "p95", "p99",
        "suggest_p90", "suggest_p95", "recommended",
    ]
    for col in money_cols:
        disp[col] = disp[col].map(lambda v: f"${v:,.0f}" if pd.notna(v) else "—")
    disp["n"] = disp["n"].astype(int)
    return disp


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else config.DB_PATH
    if not os.path.exists(db_path):
        print(f"[threshold] DB not found: {db_path}")
        print("            Run main.py to collect some sweep events first.")
        return

    con = sqlite3.connect(db_path)
    try:
        events = pd.read_sql_query("SELECT * FROM sweep_events", con)
    finally:
        con.close()

    print(f"[threshold] DB: {db_path}")
    print(f"[threshold] sweep_events={len(events)}")
    if events.empty:
        print("[threshold] No events yet — collect some, then re-run.")
        return

    table = build_table(events)

    pd.set_option("display.width", 200)
    pd.set_option("display.max_rows", None)

    print("\n" + "=" * 90)
    print("consumed_usd_volume distribution per symbol  (suggested per-symbol")
    print("CONSUMED_USD_THRESHOLD = p90–p95 band => keep the top ~5–10% of sweeps)")
    print("=" * 90)
    print(_fmt_console(table).to_string(index=False))

    out_path = "threshold_analysis.csv"
    table.to_csv(out_path, index=False)
    print(f"\n[threshold] Saved -> {out_path}")
    print("[threshold] 'recommended' is the rounded midpoint of p90–p95; nudge up")
    print("            for fewer/stronger events, down for more coverage.")


if __name__ == "__main__":
    main()
