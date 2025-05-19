# fetch_metrics.py
import json, yfinance as yf, datetime, pathlib

# 1️⃣  List the tickers you care about
TICKERS = ["AAPL", "MSFT", "SPY"]

data = {}
for t in TICKERS:
    info = yf.Ticker(t).info
    data[t] = {
        "price": info["regularMarketPrice"],
        "dayHigh": info["dayHigh"],
        "dayLow": info["dayLow"],
        "peRatio": info.get("trailingPE"),   # some tickers may not have it
        "time": datetime.datetime.now().isoformat(timespec="seconds")
    }

# 2️⃣  Save as JSON next to this script (GitHub Pages will serve it)
out = pathlib.Path(__file__).with_name("metrics.json")
out.write_text(json.dumps(data, indent=2))
print("Wrote", out)
