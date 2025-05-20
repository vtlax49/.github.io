# fetch_es_30m.py
import yfinance as yf
import json, pathlib, datetime

TICKER   = "ES=F"            # CME E-mini S&P 500 continuous contract
PERIOD   = "60d"             # Yahoo max for intraday history
INTERVAL = "30m"             # 30-minute candles

df = yf.download(
    tickers=TICKER,
    period=PERIOD,
    interval=INTERVAL,
    auto_adjust=False,
    progress=False,
    threads=False
)

records = [
    {
        "datetime": ts.isoformat(),           # 2025-05-19T20:00:00-04:00
        "open"   : round(row["Open"] , 2),
        "high"   : round(row["High"] , 2),
        "low"    : round(row["Low"]  , 2),
        "close"  : round(row["Close"], 2),
        "volume" : int(row["Volume"])
    }
    for ts, row in df.iterrows()
]

out = pathlib.Path(__file__).with_name("es_30m.json")
out.write_text(json.dumps(records, indent=2))
print(f"Wrote {out} with {len(records)} rows on {datetime.datetime.now():%F %T}")
