#!/usr/bin/env python3
"""
Minimal RTH gap-fill extractor (uses logic from V15 script, trimmed).

Outputs: es_gapfills.json  →  [
  { "date": "2025-05-22",
    "prev_close": 5859.25,
    "open_0930":  5855.50,
    "gap": -3.75,
    "half_gap_filled": true,
    "full_gap_filled": true }
  , …
]
"""

import json, pathlib, sys, datetime as dt
import numpy as np
import pandas as pd
import yfinance as yf

# --------------------------------------------------------------------------
TICKER    = "ES=F"
TICK_TOL  = 0.50            # dollars
NY_TZ     = "America/New_York"
LOOKBACK  = "90d"           # gives ~60 trading days even with holidays
OUTFILE   = pathlib.Path(__file__).with_name("es_gapfills.json")
# --------------------------------------------------------------------------

def flatten_and_rename(df):
    """Convert yfinance multi-index columns to Open/High/Low/Close/Volume."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(c) for c in df.columns]

    rename = {}
    for c in df.columns:
        cl = c.lower()
        if "open" in cl and "adj" not in cl:   rename[c] = "Open"
        elif "high" in cl:                     rename[c] = "High"
        elif "low"  in cl:                     rename[c] = "Low"
        elif "close" in cl and "adj" not in cl:rename[c] = "Close"
        elif "volume" in cl:                   rename[c] = "Volume"
    df = df.rename(columns=rename)
    return df[["Open","High","Low","Close","Volume"]]

def fetch_15m(symbol:str)->pd.DataFrame:
    df = yf.download(symbol, period=LOOKBACK, interval="15m", auto_adjust=False, progress=False)
    if df.empty:
        raise RuntimeError("No data from Yahoo")
    df = flatten_and_rename(df)
    # normalise tz → Eastern
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    df.index = df.index.tz_convert(NY_TZ)
    return df

def make_rth_subset(df):
    """Keep weekday 09:30-16:00 Eastern bars only."""
    df2 = df.copy()
    df2.index = df2.index.tz_localize(None)          # easier slicing
    df2 = df2[df2.index.dayofweek < 5]               # Mon-Fri
    return df2.between_time("09:30","16:00")

# ---------------- GAP-FILL LOGIC (unchanged except for vectorisation) -----
def gap_fill_stats(df_rth: pd.DataFrame) -> list[dict]:
    if df_rth.empty:
        return []

    df = df_rth.copy()
    df["Date"] = df.index.date

    # Daily OHLC summary
    daily = df.groupby(df.index.date).agg({
        "Open":"first","High":"max","Low":"min","Close":"last"
    })
    daily.index = pd.to_datetime(daily.index)

    # Gap calculations
    daily["Prev_Close"]    = daily["Close"].shift(1)
    daily["Gap"]           = daily["Open"] - daily["Prev_Close"]
    daily["HalfGapPrice"]  = daily["Prev_Close"] + daily["Gap"]/2.0

    def gap_filled(row, target):
        if pd.isna(target): return False
        low_tol  = row["Low"]  - TICK_TOL
        high_tol = row["High"] + TICK_TOL
        return low_tol <= target <= high_tol

    daily["HalfGapFill"] = daily.apply(lambda r: gap_filled(r, r["HalfGapPrice"]), axis=1)
    daily["FullGapFill"] = daily.apply(lambda r: gap_filled(r, r["Prev_Close"]),    axis=1)

    out = []
    for d, r in daily.iterrows():
        out.append({
            "date":            d.date().isoformat(),
            "prev_close":      round(float(r["Prev_Close"]),2) if pd.notna(r["Prev_Close"]) else np.nan,
            "open_0930":       round(float(r["Open"]),2),
            "gap":             round(float(r["Gap"]),2) if pd.notna(r["Prev_Close"]) else 0.0,
            "half_gap_filled": bool(r["HalfGapFill"]),
            "full_gap_filled": bool(r["FullGapFill"])
        })
    # drop the very first day (no Prev_Close)
    return [x for x in out if pd.notna(x["prev_close"])]

# --------------------------------------------------------------------------
def main()->int:
    try:
        raw = fetch_15m(TICKER)
        rth = make_rth_subset(raw)
        stats = gap_fill_stats(rth)
        OUTFILE.write_text(json.dumps(stats, indent=2))
        print(f"✅ wrote {OUTFILE} ({len(stats)} rows)")
        return 0
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
