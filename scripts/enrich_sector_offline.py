# api/app/scripts/enrich_sector_offline.py
import os, sqlite3, re
from pathlib import Path

DB = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")

# coarse SIC -> sector mapper (good enough for UI buckets)
def sector_from_sic(sic):
    if sic is None:
        return None
    try:
        s = int(str(sic)[:2])  # use first 2 digits
    except Exception:
        return None
    if 1 <= s <= 9:    return "Materials"          # agriculture/mining (coarse)
    if 10 <= s <= 14:  return "Materials"
    if 15 <= s <= 17:  return "Industrials"        # construction
    if 20 <= s <= 39:  return "Industrials"        # manufacturing (coarse)
    if 40 <= s <= 49:  return "Industrials"        # transport/comm/util
    if 50 <= s <= 59:  return "Consumer Discretionary"
    if 60 <= s <= 69:  return "Financials"
    if 70 <= s <= 79:  return "Consumer Discretionary"
    if 80 <= s <= 89:  return "Health Care"
    if 90 <= s <= 99:  return "Utilities"
    return None

# keyword fallback from free-text industry strings
KW = [
    (re.compile(r"software|semiconductor|technology|it", re.I), "Technology"),
    (re.compile(r"bank|financial|asset|insur|broker", re.I),     "Financials"),
    (re.compile(r"health|biotech|pharma|medical", re.I),          "Health Care"),
    (re.compile(r"energy|oil|gas|coal|uranium", re.I),            "Energy"),
    (re.compile(r"utility|utilities|power|electric", re.I),       "Utilities"),
    (re.compile(r"telecom|communication", re.I),                  "Communication Services"),
    (re.compile(r"retail|apparel|leisure|hotel|restaurant", re.I),"Consumer Discretionary"),
    (re.compile(r"staple|food|beverage|household|personal", re.I),"Consumer Staples"),
    (re.compile(r"industrial|machin|aerospace|defense|constr", re.I),"Industrials"),
    (re.compile(r"material|chemical|metals|mining|paper|forest", re.I),"Materials"),
    (re.compile(r"real ?estate|reit", re.I),                      "Real Estate"),
]

def sector_from_industry(text):
    if not text:
        return None
    for rx, sec in KW:
        if rx.search(text):
            return sec
    return None

def main():
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB) as con:
        # ensure metadata table exists
        con.execute("""CREATE TABLE IF NOT EXISTS company_metadata(
            ticker TEXT PRIMARY KEY, company_name TEXT, sector TEXT)""")

        # seed rows for any missing tickers
        con.executescript("""
        INSERT OR IGNORE INTO company_metadata(ticker, company_name, sector)
        SELECT DISTINCT ticker, NULL, NULL FROM ceo_tenures;
        """)

        # discover available columns in fundamentals_quarterly
        cols = {r[1] for r in con.execute("PRAGMA table_info(fundamentals_quarterly)").fetchall()}
        has_sic = any(c.lower() in {"sic","sic_code"} for c in cols)
        has_industry = any(c.lower() in {"industry","industry_title","industry_name"} for c in cols)

        # pull raw candidates
        cand = con.execute("""
            SELECT DISTINCT t.ticker
            FROM company_metadata t
            WHERE t.sector IS NULL OR t.sector='' OR t.sector='Unknown'
        """).fetchall()
        tickers = [t for (t,) in cand]
        if not tickers:
            print("✅ No missing sectors.")
            return

        print(f"Trying offline enrichment for {len(tickers)} tickers "
              f"(SIC:{has_sic}, industry:{has_industry})")

        updated = 0
        for t in tickers:
            sec = None
            if has_sic:
                # prefer the *most recent* row for this ticker
                row = con.execute("""
                    SELECT sic FROM fundamentals_quarterly
                    WHERE ticker=? AND sic IS NOT NULL
                    ORDER BY period_end DESC LIMIT 1
                """, (t,)).fetchone()
                if row:
                    sec = sector_from_sic(row[0])

            if not sec and has_industry:
                row = con.execute("""
                    SELECT industry FROM fundamentals_quarterly
                    WHERE ticker=? AND industry IS NOT NULL AND industry!=''
                    ORDER BY period_end DESC LIMIT 1
                """, (t,)).fetchone()
                if row:
                    sec = sector_from_industry(row[0])

            if sec:
                con.execute("""
                    UPDATE company_metadata
                    SET sector=?
                    WHERE ticker=? AND (sector IS NULL OR sector='' OR sector='Unknown')
                """, (sec, t))
                updated += 1

        con.commit()
        print(f"✅ Offline enrichment set sector for {updated} tickers")

        # push sectors into tenure_features immediately
        con.executescript("""
        UPDATE tenure_features AS tf
        SET sector = (SELECT cm.sector FROM company_metadata cm
                      WHERE LOWER(cm.ticker)=LOWER(tf.ticker)
                        AND cm.sector IS NOT NULL AND cm.sector!='')
        WHERE (tf.sector IS NULL OR tf.sector='' OR tf.sector='Unknown')
          AND EXISTS (SELECT 1 FROM company_metadata cm
                      WHERE LOWER(cm.ticker)=LOWER(tf.ticker)
                        AND cm.sector IS NOT NULL AND cm.sector!='');
        """)
        print("✅ tenure_features updated from company_metadata")

if __name__ == "__main__":
    main()