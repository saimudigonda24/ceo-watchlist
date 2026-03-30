import os, sqlite3, pandas as pd

DB = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")
PRICES_CSV = "data/bronze/prices_daily.csv"
SECTOR_CSV = "data/bronze/sector_returns_daily.csv"

def main():
    if not (os.path.exists(PRICES_CSV) and os.path.exists(SECTOR_CSV)):
        raise SystemExit(f"Missing CSVs. Need:\n  {PRICES_CSV}\n  {SECTOR_CSV}")

    px = pd.read_csv(PRICES_CSV)
    px.columns = [c.strip().lower() for c in px.columns]
    if set(["ticker","date","adj_close"]) - set(px.columns):
        raise SystemExit("prices_daily.csv must have columns: ticker,date,adj_close")

    sec = pd.read_csv(SECTOR_CSV)
    sec.columns = [c.strip().lower() for c in sec.columns]
    if set(["sector","date","sector_return"]) - set(sec.columns):
        raise SystemExit("sector_returns_daily.csv must have columns: sector,date,sector_return")

    # Normalize types
    px["date"] = pd.to_datetime(px["date"]).dt.date.astype(str)
    sec["date"] = pd.to_datetime(sec["date"]).dt.date.astype(str)

    # Map tickers→sectors from company_metadata
    with sqlite3.connect(DB) as con:
        meta = pd.read_sql_query("SELECT ticker, COALESCE(sector,'Unknown') AS sector FROM company_metadata", con)
    px = px.merge(meta, on="ticker", how="left")
    px["sector"] = px["sector"].fillna("Unknown")

    # Join sector return on (sector,date)
    px = px.merge(sec, left_on=["sector","date"], right_on=["sector","date"], how="left")
    px["sector_return"] = px["sector_return"].fillna(0.0)

    # Rename to DB schema
    out = px.rename(columns={"date":"d"})[["ticker","d","adj_close","sector_return"]]

    with sqlite3.connect(DB) as con:
        con.execute("DELETE FROM prices_daily")
        out.to_sql("prices_daily", con, if_exists="append", index=False)

    print(f"Loaded {len(out):,} rows into prices_daily from real CSVs.")

if __name__ == "__main__":
    main()