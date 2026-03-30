# api/app/scripts/export_potential_to_gsheet.py
# Purpose: Build the “Potential Dudes” research sheet and publish to Google Sheets.

import os, sqlite3
from pathlib import Path
from datetime import date
import pandas as pd

# ---- DB path ----
DB_PATH = os.environ.get(
    "FEATURES_DB",
    str(Path(__file__).resolve().parents[3] / "data" / "gold" / "ceo_watchlist.db")
)

# ---- Google Sheets config ----
GSHEET_ID   = os.environ.get("GSHEET_ID")                       # spreadsheet id
SHEET_NAME  = os.environ.get("GSHEET_TAB", "Potential Dudes")   # tab name

# ---- Helpers ----
def q(sql, params=()):
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        return pd.read_sql_query(sql, con, params=params)

def looks_like_ticker(x: str) -> bool:
    if not isinstance(x, str): return False
    s = x.strip()
    return (len(s) > 0 and len(s) <= 6 and s.upper() == s
            and " " not in s and s.replace(".","").replace("-","").isalnum())

def tenure_days(start, end=None):
    if not start: return None
    end = end or date.today().isoformat()
    try:
        return (pd.to_datetime(end) - pd.to_datetime(start)).days
    except Exception:
        return None

def stint_return(ticker, start, end=None):
    if not (ticker and start): return None
    end = end or date.today().isoformat()
    sql = """
    WITH span AS (
      SELECT d, adj_close FROM prices_daily
      WHERE LOWER(ticker)=LOWER(?) AND DATE(d) BETWEEN DATE(?) AND DATE(?)
      ORDER BY DATE(d)
    )
    SELECT (SELECT adj_close FROM span ORDER BY d ASC LIMIT 1) AS px0,
           (SELECT adj_close FROM span ORDER BY d DESC LIMIT 1) AS px1
    """
    with sqlite3.connect(DB_PATH) as con:
        r = con.execute(sql, (ticker, start, end)).fetchone()
    if not r: return None
    px0, px1 = r
    if px0 and px1 and px0 > 0:
        return (px1 / px0) - 1.0
    return None

def load_current():
    """
    Prefer the prebuilt current_ceos table (one current CEO per ticker).
    Fallback to ceo_tenures if current_ceos doesn't exist yet.
    """
    try:
        return q("""
            SELECT CEO,
                   company AS company,
                   ticker,
                   role,
                   current_tenure_start
            FROM current_ceos
            ORDER BY CEO, company, ticker
        """)
    except Exception:
        # Fallback (less strict; may include dupes)
        return q("""
            SELECT ct.person_name AS CEO,
                   COALESCE(cm.company_name, ct.company) AS company,
                   UPPER(ct.ticker) AS ticker,
                   COALESCE(ct.role,'CEO') AS role,
                   DATE(ct.start_date) AS current_tenure_start
            FROM ceo_tenures ct
            LEFT JOIN company_metadata cm ON LOWER(cm.ticker)=LOWER(ct.ticker)
            WHERE ct.end_date IS NULL
            ORDER BY CEO, company
        """)

def load_prior(person, current_ticker):
    return q("""
        SELECT COALESCE(cm.company_name, t.company) AS company, UPPER(t.ticker) AS ticker,
               DATE(t.start_date) AS start_date, DATE(t.end_date) AS end_date
        FROM ceo_tenures t
        LEFT JOIN company_metadata cm ON LOWER(cm.ticker)=LOWER(t.ticker)
        WHERE t.person_name = ?
          AND (t.end_date IS NOT NULL OR LOWER(t.ticker) != LOWER(?))
        ORDER BY DATE(t.start_date)
    """, (person, (current_ticker or "").lower()))

def build_dataframe() -> pd.DataFrame:
    now = load_current()
    rows = []
    for _, r in now.iterrows():
        ceo   = r["CEO"]
        comp  = r["company"]
        tick  = (r["ticker"] or "").upper()
        role  = r["role"]
        cur_s = r["current_tenure_start"]
        cur_days = tenure_days(cur_s)
        cur_years = round((cur_days or 0)/365.25, 2) if cur_days else None

        prior = load_prior(ceo, tick)
        prior_names, rets = [], []
        for _, p in prior.iterrows():
            pname = f"{p['company']} ({(p['ticker'] or '').upper()})" if p['company'] else (p['ticker'] or '')
            if pname: prior_names.append(pname)
            ret = stint_return((p['ticker'] or "").upper(), p['start_date'], p['end_date'])
            if ret is not None: rets.append(ret)

        avg_prior = round(sum(rets)/len(rets), 4) if rets else None
        last_prior = round(rets[-1], 4) if rets else None
        yahoo = f"https://finance.yahoo.com/quote/{tick}/profile" if tick else ""

        rows.append({
            "CEO": ceo,
            "Current Company": comp,
            "Ticker": tick,
            "Role": role,
            "Current Tenure Start": cur_s,
            "Total Experience (yrs)": cur_years,
            "Prior Companies": "; ".join(prior_names) if prior_names else "",
            "Prior Stints (#)": len(prior_names),
            "Avg Prior Stint Return": avg_prior,
            "Last Prior Stint Return": last_prior,
            "Yahoo Profile": yahoo,
            "Notes": ""
        })
    df = pd.DataFrame(rows)

    # ---- Cleanup (runs AFTER df is created) ----
    if not df.empty:
        # Make Yahoo link clickable for Sheets
        df["Yahoo Profile"] = df["Yahoo Profile"].apply(
            lambda u: f'=HYPERLINK("{u}", "Profile")' if u else ""
        )

        # De-dup by (CEO, Ticker), keep most recent Current Tenure Start
        if set(["CEO","Ticker"]).issubset(df.columns):
            if "Current Tenure Start" in df.columns:
                df["_dt"] = pd.to_datetime(df["Current Tenure Start"], errors="coerce")
                df = df.sort_values("_dt", ascending=False).drop(columns=["_dt"])
            df = df.drop_duplicates(subset=["CEO","Ticker"], keep="first")

        # Blank company if it looks like a ticker or equals the ticker; you can backfill later
        if "Current Company" in df.columns and "Ticker" in df.columns:
            mask = df["Current Company"].apply(looks_like_ticker) | (
                df["Current Company"].fillna("").str.upper() == df["Ticker"].fillna("").str.upper()
            )
            df.loc[mask, "Current Company"] = ""

    return df

# ---- Push to Google Sheets ----
def publish_to_gsheet(df: pd.DataFrame):
    if not GSHEET_ID:
        raise RuntimeError("GSHEET_ID env var is missing.")

    import gspread
    from gspread_dataframe import set_with_dataframe

    gc = gspread.service_account()  # uses GOOGLE_APPLICATION_CREDENTIALS
    sh = gc.open_by_key(GSHEET_ID)

    # Create or clear the tab
    try:
        ws = sh.worksheet(SHEET_NAME)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=str(max(1000, len(df)+100)), cols="20")

    # Write
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

    # Freeze header
    ws.freeze(rows=1)

    # Basic formatting (optional)
    try:
        import re, gspread.utils
        header = ws.row_values(1)
        num_cols = [i+1 for i, h in enumerate(header) if re.search(r"Return|Stints|yrs", h, re.I)]
        for c in num_cols:
            ws.format(
                gspread.utils.rowcol_to_a1(2, c) + ":" + gspread.utils.rowcol_to_a1(ws.row_count, c),
                {"horizontalAlignment": "RIGHT"}
            )
    except Exception:
        pass

def main():
    df = build_dataframe()
    publish_to_gsheet(df)
    print(f"✅ Published {len(df):,} rows to Google Sheet tab: '{SHEET_NAME}'")

if __name__ == "__main__":
    main()