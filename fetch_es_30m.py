# fetch_es_30m.py
#
# Re-creates es_30m.json with 30-minute ES-futures candles (last 60 days).
# Fixes the previous “DatetimeProperties has no attribute isoformat” error
# by formatting timestamps with strftime instead.

import yfinance as yf
import pandas as pd
import json, pathlib, datetime, sys


TICKER   = "ES=F"            # E-mini S&P 500 continuous contract
PERIOD   = "60d"
INTERVAL = "30m"


def main() -> int:
    try:
        # ── Pull the data ────────────────────────────────────────────────────
        hist = yf.Ticker(TICKER).history(
            period   = PERIOD,
            interval = INTERVAL,
            actions  = False,
        )

        if hist.empty:
            print("ERROR: No data returned from yfinance.", file=sys.stderr)
            return 1

        # Drop timezone info (isoformat issue came from tz-aware index)
        hist.index = hist.index.tz_localize(None)

        # ── Flatten the index and normalise dtypes ───────────────────────────
        df = hist.reset_index()                             # DatetimeIndex ➜ column
        df.rename(columns={df.columns[0]: "datetime"}, inplace=True)

        # Format datetime strings: 2025-05-19T14:30:00
        df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Round prices, fix volumes
        df = df.round({"Open": 2, "High": 2, "Low": 2, "Close": 2})
        df["Volume"] = df["Volume"].fillna(0).astype(int)

        # Keep/rename desired columns
        df = df[["datetime", "Open", "High", "Low", "Close", "Volume"]].rename(
            columns={
                "Open":  "open",
                "High":  "high",
                "Low":   "low",
                "Close": "close",
                "Volume": "volume",
            }
        )

        records = df.to_dict(orient="records")              # pure-Python objects

        out_path = pathlib.Path(__file__).with_name("es_30m.json")
        out_path.write_text(json.dumps(records, indent=2))

        print(
            f"Wrote {out_path} with {len(records)} rows "
            f"at {datetime.datetime.now().isoformat(timespec='seconds')}"
        )
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
