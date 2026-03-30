import os, sqlite3, pandas as pd, requests
from pathlib import Path
from datetime import date
from math import isfinite

def looks_like_ticker(x: str) -> bool:
    if not isinstance(x, str): return False
    s = x.strip()
    return len(s) > 0 and len(s) <= 6 and s.upper() == s and " " not in s and s.replace(".","").replace("-","").isalnum()

def pct_str(x: float | None) -> str:
    if x is None or not isfinite(x): return ""
    return f"{x*100:+.1f}%"

def years_between(start, end) -> float | None:
    if not start or not end: return None
    try:
        sd = pd.to_datetime(start)
        ed = pd.to_datetime(end)
    except Exception:
        return None
    days = (ed - sd).days
    return max(days / 365.25, 0.0)

def safe_mean(vals):
    vals = [v for v in vals if v is not None and isfinite(v)]
    return sum(vals)/len(vals) if vals else None

DB_PATH = os.environ.get("FEATURES_DB", str(Path(__file__).resolve().parents[3] / "data/gold/ceo_watchlist.db"))
GIST_ID = os.environ["GIST_ID"]               # e.g. "ab12cd34ef56..."
GIST_TOKEN = os.environ["GIST_TOKEN"]         # your GitHub PAT
FILENAME = "potential_ceos.csv"               # name shown in the gist

def q(sql, params=()):
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        return pd.read_sql_query(sql, con, params=params)

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
    sql = """
    SELECT ct.person_name AS CEO,
           COALESCE(cm.company_name, ct.company) AS company,
           ct.ticker, COALESCE(ct.role,'CEO') AS role,
           DATE(ct.start_date) AS current_tenure_start
    FROM ceo_tenures ct
    LEFT JOIN company_metadata cm ON LOWER(cm.ticker)=LOWER(ct.ticker)
    WHERE ct.end_date IS NULL
    ORDER BY CEO, company
    """
    return q(sql)

def load_prior(person, current_ticker):
    sql = """
    SELECT COALESCE(cm.company_name, t.company) AS company, t.ticker,
           DATE(t.start_date) AS start_date, DATE(t.end_date) AS end_date
    FROM ceo_tenures t
    LEFT JOIN company_metadata cm ON LOWER(cm.ticker)=LOWER(t.ticker)
    WHERE t.person_name = ? AND (t.end_date IS NOT NULL OR LOWER(t.ticker) != LOWER(?))
    ORDER BY DATE(t.start_date)
    """
    return q(sql, (person, (current_ticker or "").lower()))

def build_df():
    now = q("""
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
    rows = []

    for _, r in now.iterrows():
        ceo   = r["CEO"]
        comp  = r["company"] or ""
        tick  = (r["ticker"] or "").upper()
        role  = r["role"] or "CEO"
        cur_s = r["current_tenure_start"]

        # Current tenure length (years)
        cur_yrs = None
        try:
            cur_yrs = round(((pd.Timestamp.today().normalize() - pd.to_datetime(cur_s)).days) / 365.25, 2)
        except Exception:
            pass

        # Prior stints for this CEO (exclude current open stint)
        prior = q("""
            SELECT COALESCE(cm.company_name, t.company) AS company,
                   UPPER(t.ticker) AS ticker,
                   DATE(t.start_date) AS start_date,
                   DATE(t.end_date)   AS end_date
            FROM ceo_tenures t
            LEFT JOIN company_metadata cm ON LOWER(cm.ticker)=LOWER(t.ticker)
            WHERE t.person_name = ?
              AND (t.end_date IS NOT NULL OR LOWER(t.ticker) != LOWER(?))
            ORDER BY DATE(t.start_date)
        """, (ceo, tick.lower()))

        prior_names, simple_rets, ann_rets, stint_years = [], [], [], []

        for _, p in prior.iterrows():
            p_comp = p["company"] or ""
            p_tick = (p["ticker"] or "").upper()
            p_start, p_end = p["start_date"], p["end_date"]

            # label for display
            label = f"{p_comp} ({p_tick})" if p_comp else (p_tick or "")
            if label:
                prior_names.append(label)

            # simple total return over the stint
            r_tot = stint_return(p_tick, p_start, p_end)
            simple_rets.append(r_tot)

            # stint length (yrs) + annualized return
            yrs = years_between(p_start, p_end)
            stint_years.append(yrs)
            if r_tot is not None and yrs and yrs > 0:
                try:
                    ann = (1.0 + r_tot) ** (1.0 / yrs) - 1.0
                except Exception:
                    ann = None
            else:
                ann = None
            ann_rets.append(ann)

        # Aggregate prior metrics
        avg_prior_simple = safe_mean(simple_rets)
        last_prior_simple = simple_rets[-1] if simple_rets else None

        avg_prior_ann = safe_mean(ann_rets)
        last_prior_ann = ann_rets[-1] if ann_rets else None

        avg_stint_len = safe_mean(stint_years)
        last_stint_len = stint_years[-1] if stint_years else None

        yahoo = f"https://finance.yahoo.com/quote/{tick}/profile" if tick else ""

        rows.append({
            "CEO": ceo,
            "Current Company": ("" if looks_like_ticker(comp) or comp.upper()==tick else comp),
            "Ticker": tick,
            "Role": role,
            "Current Tenure Start": cur_s,
            "Current Tenure (yrs)": cur_yrs,

            # Easier-to-read outputs
            "Prior Companies": "; ".join(prior_names) if prior_names else "",
            "Prior Stints (#)": len(prior_names),

            # Total return over full stint (as % strings)
            "Avg Prior Stint Return": pct_str(avg_prior_simple),
            "Last Prior Stint Return": pct_str(last_prior_simple),

            # Annualized (per year) versions (as % strings) — what you asked for
            "Avg Prior Stint Return (per yr)": pct_str(avg_prior_ann),
            "Last Prior Stint Return (per yr)": pct_str(last_prior_ann),

            # Stint length in years (rounded)
            "Avg Prior Stint Length (yrs)": (round(avg_stint_len, 2) if avg_stint_len is not None else ""),
            "Last Prior Stint Length (yrs)": (round(last_stint_len, 2) if last_stint_len is not None else ""),

            # Links & notes
            "Yahoo Profile": f'=HYPERLINK("{yahoo}", "Profile")' if yahoo else "",
            "Notes": ""
        })

    # Deduplicate by (CEO, Ticker), keep the most recent current tenure start
    df = pd.DataFrame(rows)
    if not df.empty and {"CEO","Ticker","Current Tenure Start"}.issubset(df.columns):
        df["_dt"] = pd.to_datetime(df["Current Tenure Start"], errors="coerce")
        df = df.sort_values("_dt", ascending=False).drop(columns=["_dt"])
        df = df.drop_duplicates(subset=["CEO","Ticker"], keep="first")

    return df

def push_gist(csv_text: str):
    url = f"https://api.github.com/gists/{GIST_ID}"
    headers = {"Authorization": f"token {GIST_TOKEN}"}
    payload = {"files": {FILENAME: {"content": csv_text}}}
    r = requests.patch(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    raw_url = r.json()["files"][FILENAME]["raw_url"]
    print("✅ Updated gist. Raw URL:", raw_url)

def main():
    df = build_df()
    csv_text = df.to_csv(index=False)
    push_gist(csv_text)

if __name__ == "__main__":
    main()