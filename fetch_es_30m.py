#!/usr/bin/env python3
"""
Minimal RTH gap-fill extractor
— fixed to respect Yahoo’s 60-day limit for 15-minute data —

Outputs → es_gapfills.json in the format your web page needs.
"""

import json, pathlib, sys, datetime as dt
import numpy as np
import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
TICKER          = "ES=F"
TICK_TOL        = 0.50               # dollars
NY_TZ           = "America/New_York"
LOOKBACK_OPTS   = ["60d", "30d"]     # try each until one works
OUTFILE         = pathlib.Path(__file__).with_name("es_gapfills.json")
# ---------------------------------------------------------------------------


def flatten(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(c) for c in df.columns]

    ren = {}
    for c in df.columns:
        cl = c.lower()
        if "open" in cl and "adj" not in cl:   ren[c] = "Open"
        elif "high" in cl:                     ren[c] = "High"
        elif "low"  in cl:                     ren[c] = "Low"
        elif "close" in cl and "adj" not in cl:ren[c] = "Close"
        elif "volume" in cl:                   ren[c] = "Volume"

    df = df.rename(columns=ren)
    return df[["Open", "High", "Low", "Close", "Volume"]]


def fetch_15m(symbol: str) -> pd.DataFrame:
    for period in LOOKBACK_OPTS:
        df = yf.download(symbol,
                         period=period,
                         interval="15m",
                         auto_adjust=False,
                         progress=False)
        if not df.empty:
            df = flatten(df)
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            df.index = df.index.tz_convert(NY_TZ)
            return df

    raise RuntimeError(
        f"Yahoo returned no 15-minute data for {symbol} within {LOOKBACK_OPTS}"
    )


def make_rth(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.index = df2.index.tz_localize(None)
    df2 = df2[df2.index.dayofweek < 5]
    return df2.between_time("09:30", "16:00")


def rth_gap_stats(df_rth: pd.DataFrame) -> list[dict]:
    if df_rth.empty:
        return []

    daily = df_rth.groupby(df_rth.index.date).agg(
        Open=("Open", "first"),
        High=("High", "max"),
        Low=("Low", "min"),
        Close=("Close", "last")
    )
    daily.index = pd.to_datetime(daily.index)
    daily["Prev_Close"]   = daily["Close"].shift(1)
    daily["Gap"]          = daily["Open"] - daily["Prev_Close"]
    daily["HalfGapPrice"] = daily["Prev_Close"] + daily["Gap"] / 2.0

    def filled(row, tgt):
        if pd.isna(tgt):
            return False
        return (row["Low"] - TICK_TOL) <= tgt <= (row["High"] + TICK_TOL)

    daily["HalfGapFill"] = daily.apply(lambda r: filled(r, r["HalfGapPrice"]), axis=1)
    daily["FullGapFill"] = daily.apply(lambda r: filled(r, r["Prev_Close"]),    axis=1)

    out = []
    for d, r in daily.iterrows():
        if pd.isna(r["Prev_Close"]):         # skip first day (no previous close)
            continue
        out.append({
            "date":            d.date().isoformat(),
            "prev_close":      round(float(r["Prev_Close"]), 2),
            "open_0930":       round(float(r["Open"]), 2),
            "gap":             round(float(r["Gap"]), 2),
            "half_gap_filled": bool(r["HalfGapFill"]),
            "full_gap_filled": bool(r["FullGapFill"])
        })
    return out


def main() -> int:
    try:
        raw = fetch_15m(TICKER)
        rth = make_rth(raw)
        stats = rth_gap_stats(rth)
        OUTFILE.write_text(json.dumps(stats, indent=2))
        print(f"✅ wrote {OUTFILE} ({len(stats)} rows)")
        return 0
    except Exception as exc:
        print("ERROR:", exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
