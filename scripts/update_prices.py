# api/app/scripts/update_prices.py
import os, sqlite3, pandas as pd
from datetime import date, timedelta
db=os.environ.get("FEATURES_DB","./data/gold/ceo_watchlist.db")
with sqlite3.connect(db) as con:
    tickers=[t for (t,) in con.execute("SELECT DISTINCT ticker FROM ceo_tenures")]
    # synth “latest” bar: copy last price forward one day
    for t in tickers:
        row=con.execute("SELECT d, adj_close, sector_return FROM prices_daily WHERE ticker=? ORDER BY d DESC LIMIT 1", (t,)).fetchone()
        if not row: continue
        last_d, px, sec = row
        # add a fake next business day row
        d=pd.to_datetime(last_d)+pd.tseries.offsets.BDay(1)
        con.execute("INSERT OR IGNORE INTO prices_daily(ticker,d,adj_close,sector_return) VALUES (?,?,?,?)",
                    (t, d.date().isoformat(), float(px), float(sec)))
print("✅ prices updated (stub)")