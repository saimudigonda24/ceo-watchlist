import os, sqlite3, pandas as pd

db = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")

q = """
SELECT
  person_name AS CEO,
  UPPER(ticker) AS ticker,
  COALESCE(role,'CEO') AS role,
  DATE(start_date) AS start_date
FROM ceo_tenures
WHERE end_date IS NULL
ORDER BY DATE(start_date) DESC
LIMIT 10;
"""

with sqlite3.connect(db) as con:
    df = pd.read_sql_query(q, con)

print("DB path:", db)
print(df if not df.empty else "(no current CEOs found)")