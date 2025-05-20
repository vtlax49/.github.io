# fetch_es_30m.py
#
# Creates es_30m.json → 30-minute ES futures candles for the last 60 days.
# Designed for GitHub Actions: every run overwrites the file with fresh data.

import yfinance as yf
import pandas as pd           # yfinance pulls this in, but explicit import is clearer
import json, pathlib, datetime, sys

TICKER   = "ES=F"   # CME E-mini S&P 500 continuous contract
PERIOD   = "60d"    # history window
INTERVAL = "30m"    # candle size

def main() -> int:
    try:
        df = yf.download(
            tickers=TICKER,
            period=PERIOD,
            interval=INTERVAL,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        if df.empty:
            print("ERROR: No data returned from yfinance.", file=sys.stderr)
            return 1

        # ── Massage the DataFrame into JSON-serialisable Python types ──────────
        df = df.reset_index()                                       # Datetime column visible
        df["Datetime"] = df["Datetime"].apply(lambda ts: ts.isoformat())

        df = df.round(2)                                            # floats → 2 decimals
        df["Volume"] = df["Volume"].fillna(0).astype(int)           # np.int64 → int

        records = df.rename(
            columns={
                "Datetime": "datetime",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        ).to_dict(orient="records")

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
