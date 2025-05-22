# fetch_es_30m.py
#
# 1. Pull 60 days of 30-minute ES-futures candles from yfinance.
# 2. Save them to es_30m.json   (same as before).
# 3. Calculate RTH gap / half-gap / full-gap fills and
#    save to es_gapfills.json      NEW ✔︎
#
# Designed for GitHub Actions (cron job).  Python 3.11+.

import yfinance as yf
import pandas as pd
import json, pathlib, datetime, sys

TICKER   = "ES=F"      # CME E-mini S&P 500 continuous contract
PERIOD   = "60d"       # look-back window
INTERVAL = "30m"       # candle size

HERE = pathlib.Path(__file__).parent           # repo root


def fetch_history() -> pd.DataFrame:
    """Download intraday history and return a tidy DataFrame."""
    df = yf.Ticker(TICKER).history(
        period   = PERIOD,
        interval = INTERVAL,
        actions  = False,
    )
    if df.empty:
        raise RuntimeError("No data returned from yfinance.")

    # Drop tz to simplify string formatting later
    df.index = df.index.tz_localize(None)

    return df


def write_es_30m(df: pd.DataFrame) -> None:
    """Write full 30-min candles to es_30m.json (same format as before)."""
    tidy = (
        df.reset_index()
          .rename(columns={df.index.name: "datetime"})
    )

    tidy["datetime"] = tidy["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    tidy = tidy.round({"Open": 2, "High": 2, "Low": 2, "Close": 2})
    tidy["Volume"] = tidy["Volume"].fillna(0).astype(int)

    tidy = tidy[["datetime", "Open", "High", "Low", "Close", "Volume"]].rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    (HERE / "es_30m.json").write_text(
        json.dumps(tidy.to_dict(orient="records"), indent=2)
    )


def gap_stats(df: pd.DataFrame) -> list[dict]:
    """
    Return a list of dicts, one per RTH day, with gap/half/full fill info.
    Logic:
      • prev_close = close of the 15:30-16:00 candle of the *previous* calendar day
      • open_0930  = open of the 09:30-10:00 candle of the current day
      • gap        = open_0930 − prev_close
      • half_fill_price = prev_close + gap/2
      • Up-gap:      check day's Low for half/full fill
      • Down-gap:    check day's High for half/full fill
    """
    df = df.copy()
    df["date"] = df.index.date
    df["time"] = df.index.time

    # Helper masks
    is_1530 = df["time"] == datetime.time(15, 30)   # candle ending at 16:00
    is_0930 = df["time"] == datetime.time(9, 30)

    prev_closes = (
        df.loc[is_1530, ["date", "Close"]]
          .set_index("date")["Close"]
    )

    gap_rows = []
    for cur_date, row in df.loc[is_0930].iterrows():
        prev_date = cur_date.date() - datetime.timedelta(days=1)
        # Walk backwards until we find a date with a 15:30 candle (skip weekends/holidays)
        while prev_date not in prev_closes and prev_date >= min(prev_closes.index):
            prev_date -= datetime.timedelta(days=1)

        if prev_date not in prev_closes:
            continue  # not enough history for a comparison

        prev_close = round(float(prev_closes.loc[prev_date]), 2)
        open_0930  = round(float(row["Open"]), 2)
        gap        = round(open_0930 - prev_close, 2)

        # Extract the day's price extremes (00:00–23:59 of cur_date)
        day_data = df.loc[df["date"] == cur_date.date()]
        day_low  = day_data["Low"].min()
        day_high = day_data["High"].max()

        half_fill_price = prev_close + gap / 2

        if gap > 0:   # Up gap
            half_filled = day_low  <= half_fill_price
            full_filled = day_low  <= prev_close
        elif gap < 0: # Down gap
            half_filled = day_high >= half_fill_price
            full_filled = day_high >= prev_close
        else:         # No gap
            half_filled = full_filled = True

        gap_rows.append({
            "date"            : cur_date.date().isoformat(),
            "prev_close"      : prev_close,
            "open_0930"       : open_0930,
            "gap"             : gap,
            "half_gap_filled" : bool(half_filled),
            "full_gap_filled" : bool(full_filled),
        })

    return gap_rows


def write_gap_json(gap_rows: list[dict]) -> None:
    (HERE / "es_gapfills.json").write_text(
        json.dumps(gap_rows, indent=2)
    )


def main() -> int:
    try:
        df = fetch_history()
        write_es_30m(df)
        write_gap_json(gap_stats(df))

        print(
            f"Wrote es_30m.json ({len(df)} rows) and "
            f"es_gapfills.json ({len(gap_stats(df))} days) "
            f"at {datetime.datetime.now().isoformat(timespec='seconds')}"
        )
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
