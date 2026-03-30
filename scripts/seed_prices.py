import os, sqlite3, pandas as pd, numpy as np
from datetime import datetime, timedelta

DB = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")
YEARS = int(os.environ.get("PRICE_YEARS", "12"))  # extendable

def trading_days(n_years=YEARS):
    end = datetime.today().date()
    start = end - timedelta(days=365*n_years + 60)
    days = pd.bdate_range(start=start, end=end)
    return [d.date().isoformat() for d in days]

def main():
    with sqlite3.connect(DB) as con:
        tickers = pd.read_sql_query(
            "SELECT DISTINCT ticker FROM ceo_tenures WHERE ticker IS NOT NULL AND TRIM(ticker)<>''", con
        )["ticker"].unique()
        if len(tickers) == 0:
            print("No tickers in ceo_tenures; nothing to seed."); return
        dates = trading_days()
        rows = []
        rng = np.random.default_rng(42)
        for t in tickers:
            price = rng.uniform(8, 60)
            for d in dates:
                price *= (1.0 + rng.normal(0.0002, 0.018))  # modest drift/vol
                rows.append((t, d, float(max(price, 0.5)), 0.0))  # sector_return=0
        df = pd.DataFrame(rows, columns=["ticker","d","adj_close","sector_return"])
        con.execute("DELETE FROM prices_daily")
        df.to_sql("prices_daily", con, if_exists="append", index=False)
    print(f"Seeded prices_daily: {len(df)} rows across {len(tickers)} tickers for ~{YEARS}y")
if __name__ == "__main__":
    main()
