# fetch_es_30m.py
#
# 1. Download 60 days of 30-minute ES futures from Yahoo Finance.
# 2. Save all candles to es_30m.json
# 3. Compute RTH gap / half-gap / full-gap fills
#    (prev-day 16:00 close vs current-day 09:30 open, Eastern time)
#    and save to es_gapfills.json
#
# Drop this file in your repo root, commit, re-run your Action.

import json, pathlib, sys, datetime as dt
import pandas as pd
import yfinance as yf

TICKER   = "ES=F"
PERIOD   = "60d"
INTERVAL = "30m"
HERE     = pathlib.Path(__file__).parent
NY_TZ    = "America/New_York"


def fetch_history() -> pd.DataFrame:
    df = yf.Ticker(TICKER).history(
        period=PERIOD,
        interval=INTERVAL,
        actions=False,
    )

    if df.empty:
        raise RuntimeError("No data returned from yfinance")

    # -------------  Time-zone handling  -----------------------------------
    #
    # Yahoo usually returns America/New_York already, but occasionally UTC.
    # Standardise to US/Eastern and keep the tz info until FINAL export.
    #
    if df.index.tz is None:
        # Assume UTC if no timezone
        df.index = df.index.tz_localize("UTC").tz_convert(NY_TZ)
    else:
        df.index = df.index.tz_convert(NY_TZ)

    return df


# --------------------------------------------------------------------------
#  JSON helpers
# --------------------------------------------------------------------------
def write_json(path: pathlib.Path, obj):
    path.write_text(json.dumps(obj, indent=2))


def export_intraday(df: pd.DataFrame):
    tidy = (
        df.reset_index()
          .rename(columns={df.index.name: "datetime"})
    )
    tidy["datetime"] = tidy["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    tidy = tidy.round({"Open": 2, "High": 2, "Low": 2, "Close": 2})
    tidy["Volume"] = tidy["Volume"].fillna(0).astype(int)
    tidy = tidy[["datetime", "Open", "High", "Low", "Close", "Volume"]].rename(
        columns={"Open": "open", "High": "high",
                 "Low": "low", "Close": "close", "Volume": "volume"}
    )
    write_json(HERE / "es_30m.json", tidy.to_dict(orient="records"))


# --------------------------------------------------------------------------
#  Gap / fill logic
# --------------------------------------------------------------------------
def gap_stats(df: pd.DataFrame) -> list[dict]:
    """
    Returns list of dicts keyed by trading date with:
      prev_close, open_0930, gap, half_gap_filled, full_gap_filled
    """
    df = df.copy()
    df["date"] = df.index.date
    df["time"] = df.index.time

    # Closing price candidates: bar that *starts* at 16:00 (preferred),
    # else the bar that starts at 15:30 (ends at 16:00).
    close_mask = df["time"].isin([dt.time(16, 0), dt.time(15, 30)])
    closes = (
        df.loc[close_mask, ["date", "Close", "time"]]
          .sort_values("time")               # 16:00 appears after 15:30 if both exist
          .drop_duplicates("date", keep="last")
          .set_index("date")["Close"]
    )

    # 09:30 opens
    open_mask = df["time"] == dt.time(9, 30)
    opens = df.loc[open_mask]

    results = []
    for ts, row in opens.iterrows():
        cur_date = ts.date()
        prev_date = cur_date - dt.timedelta(days=1)

        # Walk back to previous date that has a stored close (skip weekends/holidays)
        while prev_date not in closes.index and prev_date >= min(closes.index):
            prev_date -= dt.timedelta(days=1)
        if prev_date not in closes.index:
            continue

        prev_close = float(closes.loc[prev_date])
        open_0930  = float(row["Open"])
        gap        = round(open_0930 - prev_close, 2)

        day_slice = df[df["date"] == cur_date]
        day_low   = day_slice["Low"].min()
        day_high  = day_slice["High"].max()
        half_gap_price = prev_close + gap / 2

        if gap > 0:   # up-gap
            half_filled = day_low  <= half_gap_price
            full_filled = day_low  <= prev_close
        elif gap < 0: # down-gap
            half_filled = day_high >= half_gap_price
            full_filled = day_high >= prev_close
        else:
            half_filled = full_filled = True  # zero gap trivially filled

        results.append({
            "date"            : cur_date.isoformat(),
            "prev_close"      : round(prev_close, 2),
            "open_0930"       : round(open_0930, 2),
            "gap"             : gap,
            "half_gap_filled" : bool(half_filled),
            "full_gap_filled" : bool(full_filled),
        })

    return results


# --------------------------------------------------------------------------
def main() -> int:
    try:
        hist = fetch_history()
        export_intraday(hist)
        write_json(HERE / "es_gapfills.json", gap_stats(hist))
        print("✅ Wrote es_30m.json and es_gapfills.json")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
