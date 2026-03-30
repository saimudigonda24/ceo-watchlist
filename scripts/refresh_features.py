# api/app/scripts/refresh_features.py
import os, sqlite3, pathlib, sys

SQL_PATH = pathlib.Path("api/app/sql/tenure_features.sql")
DB_PATH = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")

def main():
    if not SQL_PATH.exists():
        print(f"ERROR: {SQL_PATH} not found. Create it first.", file=sys.stderr)
        sys.exit(1)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.executescript(SQL_PATH.read_text())
    print(f"✅ tenure_features refreshed in {DB_PATH}")

if __name__ == "__main__":
    main()