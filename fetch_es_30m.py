# fetch_es_30m.py
#
# Build es_30m.json containing 30-minute ES-futures candles
# for the last 60 days.  Safe for GitHub Actions / cron use.

import yfinance as yf
import pandas as pd      # explicit for clarity
import json, pathlib, datetime, sys


TICKER   = "ES=F"        # CME E-mini S&P 500 continuous contract
PERIOD   = "60d"         # look-back window
INTERVAL = "30m"         # candle size


def main() -> int:
    try:
        # ── Pull data (Ticker.history avoids MultiIndex headaches) ────────────
        df = yf.Ticker(TICKER).history(
            period   = PERIOD,
            interval = INTERVAL,
            actions  = False,
        )

        if df.empty:
            print("ERROR: No data returned from yfinance.", file=sys.stderr)
            return 1

        # ── Flatten the index and normalise dtypes ───────────────────────────
        df = df.reset_index()                         # index → column 0
        df.rename(columns={df.columns[0]: "Datetime"}, inplace=True)

        # Make datetime strings (ISO 8601, no timezone)
        df["Datetime"] = (
            pd.to_datetime(df["Datetime"]).dt.tz_localize(None).dt.isoformat()
        )

        # Round prices, fix volumes
        df = df.round({"Open": 2, "High": 2, "Low": 2, "Close": 2})
        df["Volume"] = df["Volume"].fillna(0).astype(int)

        # Keep only the columns we need, rename to concise keys
        df = df[["Datetime", "Open", "High", "Low", "Close", "Volume"]].rename(
            columns={
                "Datetime": "datetime",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        )

        records = df.to_dict(orient="records")        # pure-Python objects

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
