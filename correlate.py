"""Correlate the reference bot's ENTRIES (ref_ledger.jsonl) against the time-aligned
market (ref_market.jsonl) to find its trigger: for each entry, what did LBank last /
LBank index (underlying) / Binance do in the seconds BEFORE, and how big was the
last-vs-index gap. Runs on the server (reads the local jsonl files). Read-only.

  venv/bin/python correlate.py
"""
import json
from collections import defaultdict

PRE_S = 3.0        # look this many seconds before the entry
GAP_S = 90         # ledger rows > this apart = a new trade


def load_trades():
    rows = []
    with open("ref_ledger.jsonl") as f:
        for ln in f:
            try:
                rows.append(json.loads(ln))
            except Exception:
                pass
    rows.sort(key=lambda r: r["t"])
    bysym = defaultdict(list)
    for r in rows:
        bysym[r["s"]].append(r)
    trades = []
    for sym, rs in bysym.items():
        if not sym or any(ord(c) > 127 for c in sym):
            continue
        cur = []
        for r in rs:
            if cur and (r["t"] - cur[-1]["t"]) > GAP_S * 1000:
                trades.append((sym, cur[0]["t"], cur))
                cur = []
            cur.append(r)
        if cur:
            trades.append((sym, cur[0]["t"], cur))
    trades.sort(key=lambda t: t[1])
    return trades


def load_market(want):
    """Keep only market rows for wanted (sym) within a window around any entry."""
    keep = defaultdict(list)
    windows = defaultdict(list)
    for sym, t0 in want:
        windows[sym].append((t0 - (PRE_S + 2) * 1000, t0 + 2000))
    with open("ref_market.jsonl") as f:
        for ln in f:
            try:
                m = json.loads(ln)
            except Exception:
                continue
            s = m.get("s")
            if s not in windows:
                continue
            t = m["t"]
            if any(a <= t <= b for a, b in windows[s]):
                keep[s].append(m)
    for s in keep:
        keep[s].sort(key=lambda x: x["t"])
    return keep


def fnum(x):
    try:
        return float(x)
    except Exception:
        return None


def main():
    trades = load_trades()
    print(f"{len(trades)} reference trades in ledger\n")
    # --- his P&L per trade + per symbol (from ledger amounts) ---
    persym = defaultdict(float)
    persym_n = defaultdict(int)
    print(f"{'sym':13}{'open(UTC ms)':15}{'net$':>9}{'liq'}")
    for sym, t0, rows in trades:
        net = sum(fnum(r.get("amt")) or 0 for r in rows)
        liq = any(r.get("typ") == "Liquidation" for r in rows)
        persym[sym] += net
        persym_n[sym] += 1
        print(f"{sym[:12]:13}{t0:<15}{net:>+9.2f}  {'LIQ' if liq else ''}")
    print("\n=== his per-symbol net (sorted) ===")
    for s, v in sorted(persym.items(), key=lambda kv: kv[1]):
        print(f"  {s:14} ${v:+8.2f}  ({persym_n[s]} trades)")
    tot = sum(persym.values())
    wins = sum(1 for _, _, rs in trades if sum(fnum(r.get('amt')) or 0 for r in rs) > 0)
    print(f"\nHIS TOTAL net ${tot:+.2f}, winrate {wins}/{len(trades)}="
          f"{wins/len(trades)*100:.0f}%\n" + "=" * 50)
    want = [(s, t0) for s, t0, _ in trades]
    mkt = load_market(want)

    def at(sym, t):
        rows = mkt.get(sym) or []
        best = None
        for m in rows:
            if m["t"] <= t:
                best = m
            else:
                break
        return best

    def bmid(m):
        b, a = fnum(m.get("bb")), fnum(m.get("ba"))
        return (b + a) / 2 if b and a else None

    print(f"\n{'sym':13}{'entry(UTC ms)':15}{'last-3s%':>9}{'idx-3s%':>9}"
          f"{'bin-3s%':>9}{'last-idx%':>10}")
    agg = {"last": [], "idx": [], "bin": [], "gap": []}
    for sym, t0, _ in trades:
        m0 = at(sym, t0)
        mp = at(sym, t0 - PRE_S * 1000)
        if not m0 or not mp:
            print(f"{sym[:12]:13}{t0:<15}   (no market data in window)")
            continue
        ll0, llp = fnum(m0.get("ll")), fnum(mp.get("ll"))
        li0, lip = fnum(m0.get("li")), fnum(mp.get("li"))
        bm0, bmp = bmid(m0), bmid(mp)
        last_mv = (ll0 / llp - 1) * 1e4 if ll0 and llp else None
        idx_mv = (li0 / lip - 1) * 1e4 if li0 and lip else None
        bin_mv = (bm0 / bmp - 1) * 1e4 if bm0 and bmp else None
        gap = (ll0 / li0 - 1) * 1e4 if ll0 and li0 else None
        for k, v in (("last", last_mv), ("idx", idx_mv), ("bin", bin_mv), ("gap", gap)):
            if v is not None:
                agg[k].append(v)
        def s(v):
            return f"{v:+.1f}" if v is not None else "  -"
        print(f"{sym[:12]:13}{t0:<15}{s(last_mv):>9}{s(idx_mv):>9}"
              f"{s(bin_mv):>9}{s(gap):>10}")

    print("\n=== AVERAGES (abs move in the 3s before his entry) ===")
    for k in ("last", "idx", "bin", "gap"):
        v = agg[k]
        if v:
            avabs = sum(abs(x) for x in v) / len(v)
            print(f"  {k:5}: mean|move|={avabs:6.1f}bps  n={len(v)}")
    print("\nRead: whichever feed moves MOST right before his entries is his trigger "
          "source; a big last-idx gap => he trades the LBank last-vs-index basis.")


if __name__ == "__main__":
    main()
