# api/app/scripts/bootstrap_features_inputs.py
import os, sqlite3, json, csv, pathlib

DB_PATH = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")
root = pathlib.Path(".")

TENURE_JSON = root / "data/silver/merged_ceo_changes.json"
TENURE_CSV  = root / "data/silver/ceo_tenures.csv"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ceo_tenures(
  person_id   TEXT,
  person_name TEXT,
  company     TEXT,
  ticker      TEXT,
  role        TEXT,
  start_date  TEXT,
  end_date    TEXT,
  sector      TEXT
);

CREATE TABLE IF NOT EXISTS company_metadata(
  ticker TEXT PRIMARY KEY,
  sector TEXT
);

CREATE TABLE IF NOT EXISTS prices_daily(
  ticker TEXT, d TEXT, adj_close REAL, sector_return REAL
);

CREATE TABLE IF NOT EXISTS fundamentals_quarterly(
  ticker TEXT, period_end TEXT, revenue REAL, r_and_d REAL, sgna REAL
);

-- Optional tables (present in the feature SQL; safe to keep empty)
CREATE TABLE IF NOT EXISTS insider_trades(
  ticker TEXT, filing_date TEXT, txn_type TEXT, shares REAL, price REAL
);
CREATE TABLE IF NOT EXISTS buyback_events(
  ticker TEXT, filing_date TEXT, amount REAL
);
CREATE TABLE IF NOT EXISTS equity_issuance_events(
  ticker TEXT, filing_date TEXT, shares_issued REAL
);
CREATE TABLE IF NOT EXISTS job_postings_daily(
  ticker TEXT, snapshot_date TEXT, postings_open INTEGER
);
CREATE TABLE IF NOT EXISTS transcripts_daily_signals(
  ticker TEXT, d TEXT,
  action_verb_rate REAL,
  focus_operational_rate REAL,
  focus_product_rate REAL
);
"""

def load_tenures(con: sqlite3.Connection):
    rows = []
    if TENURE_JSON.exists():
        with open(TENURE_JSON, "r") as f:
            data = json.load(f)
        for r in data:
            rows.append({
                "person_id":   r.get("person_id") or f"{r.get('person','')}_{r.get('ticker','')}",
                "person_name": r.get("person") or r.get("ceo_name"),
                "company":     r.get("company"),
                "ticker":      r.get("ticker"),
                "role":        r.get("role") or "CEO",
                "start_date":  r.get("start_date") or r.get("effective_date"),
                "end_date":    r.get("end_date"),
                "sector":      r.get("sector"),
            })
    elif TENURE_CSV.exists():
        with open(TENURE_CSV, newline="") as f:
            rdr = csv.DictReader(f)
            for r in rdr:
                rows.append({
                    "person_id":   r.get("person_id") or f"{r.get('person_name','')}_{r.get('ticker','')}",
                    "person_name": r.get("person_name"),
                    "company":     r.get("company"),
                    "ticker":      r.get("ticker"),
                    "role":        r.get("role") or "CEO",
                    "start_date":  r.get("start_date"),
                    "end_date":    r.get("end_date"),
                    "sector":      r.get("sector"),
                })

    if rows:
        con.execute("DELETE FROM ceo_tenures")
        con.executemany("""
            INSERT INTO ceo_tenures
            (person_id, person_name, company, ticker, role, start_date, end_date, sector)
            VALUES (:person_id,:person_name,:company,:ticker,:role,:start_date,:end_date,:sector)
        """, rows)
        # populate company_metadata (unique ticker→sector)
        meta = {}
        for r in rows:
            if r["ticker"]:
                meta[r["ticker"]] = r.get("sector")
        if meta:
            con.executemany(
                "INSERT OR REPLACE INTO company_metadata(ticker, sector) VALUES (?,?)",
                list(meta.items())
            )
        print(f"Loaded {len(rows)} tenures into ceo_tenures")
    else:
        print("No tenure source found (data/silver/merged_ceo_changes.json or data/silver/ceo_tenures.csv). Tables created empty.")

def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.executescript(SCHEMA_SQL)
        load_tenures(con)
    print(f"✅ Base input tables are ready in {DB_PATH}")

if __name__ == "__main__":
    main()