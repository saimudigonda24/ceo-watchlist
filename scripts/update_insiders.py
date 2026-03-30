# api/app/scripts/update_insiders.py
import os, sqlite3, datetime as dt
db=os.environ.get("FEATURES_DB","./data/gold/ceo_watchlist.db")
with sqlite3.connect(db) as con:
    con.execute("""CREATE TABLE IF NOT EXISTS insider_trades(
        ticker TEXT, filing_date TEXT, txn_type TEXT, shares REAL, price REAL)""")
    # no-op stub; keep the table present
print("✅ insiders updated (stub)")