import os, time, sqlite3, pandas as pd, numpy as np
from datetime import date
import yfinance as yf

DB = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")
START = os.environ.get("PRICE_START", "2015-01-01")
SPY_PROXY = os.environ.get("SECTOR_PROXY", "SPY")  # fallback for sector returns

def safe_download(ticker, start):
    # robust single-ticker fetch with retries
    for attempt in range(4):
        try:
            df = yf.Ticker(ticker).history(start=start, auto_adjust=True, actions=False)
            if df is not None and not df.empty:
                df = df[["Close"]].rename(columns={"Close": "adj_close"}).reset_index()
                df["Date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
                return df.rename(columns={"Date": "date"})
        except Exception as e:
            pass
        time.sleep(1.5 * (attempt + 1))
    return pd.DataFrame(columns=["date", "adj_close"])

def load_tickers_from_db():
    with sqlite3.connect(DB) as con:
        tickers = pd.read_sql_query(
            "SELECT DISTINCT ticker FROM ceo_tenures WHERE ticker IS NOT NULL AND TRIM(ticker)<>''",
            con
        )["ticker"].tolist()
        meta = pd.read_sql_query(
            "SELECT ticker, COALESCE(sector,'Market') AS sector FROM company_metadata",
            con
        )
    return tickers, meta

def main():
    os.makedirs("data/bronze", exist_ok=True)
    tickers, meta = load_tickers_from_db()
    if not tickers:
        print("No tickers found in ceo_tenures.")
        return

    all_px = []
    failed = []
    for t in tickers:
        df = safe_download(t, START)
        if df.empty:
            failed.append(t)
            continue
        df["ticker"] = t
        all_px.append(df)

    if not all_px:
        raise SystemExit(f"All downloads failed (tickers={len(tickers)}). Check your network and try again.")

    prices = pd.concat(all_px, ignore_index=True)

    # Sector mapping + sector return proxy
    prices = prices.merge(meta, on="ticker", how="left")
    prices["sector"] = prices["sector"].fillna("Market")

    # Try to get a proxy series for sector returns; fall back to SPY if ^GSPC fails
    proxy_symbol = "^GSPC"
    proxy = safe_download(proxy_symbol, START)
    if proxy.empty:
        proxy_symbol = SPY_PROXY
        proxy = safe_download(proxy_symbol, START)
    if proxy.empty:
        # last resort: flat 0 returns
        print("WARNING: Could not fetch ^GSPC or SPY. Using flat sector_return=0.")
        sec = prices[["sector","date"]].copy()
        sec["sector_return"] = 0.0
    else:
        proxy["ret"] = proxy["adj_close"].pct_change().fillna(0.0)
        proxy["sector_return"] = proxy["ret"].cumsum()
        proxy = proxy[["date","sector_return"]]
        # assign same proxy to all sectors (simple but consistent)
        sectors = prices["sector"].dropna().unique()
        sec = pd.DataFrame(
            np.repeat(proxy.values, len(sectors), axis=0),
            columns=["date","sector_return"]
        )
        sec["sector"] = np.tile(sectors, len(proxy))
        sec = sec[["sector","date","sector_return"]]

    # Save bronze CSVs (optional for inspection)
    prices[["ticker","date","adj_close"]].to_csv("data/bronze/prices_daily.csv", index=False)
    sec.to_csv("data/bronze/sector_returns_daily.csv", index=False)
    print(f"Saved bronze CSVs with {len(prices):,} price rows; failed: {failed}")

    # Load into DB
    out = prices.merge(sec, on=["sector","date"], how="left").rename(columns={"date":"d"})
    with sqlite3.connect(DB) as con:
        con.execute("DELETE FROM prices_daily")
        out[["ticker","d","adj_close","sector_return"]].to_sql("prices_daily", con, if_exists="append", index=False)

    if failed:
        print("Failed tickers:", failed)
    print(f"Loaded prices_daily → {len(out):,} rows in {DB}")

if __name__ == "__main__":
    main()