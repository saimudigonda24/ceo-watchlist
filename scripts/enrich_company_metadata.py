# api/app/scripts/enrich_company_metadata.py
import os, time, sqlite3, pandas as pd, re, random

DB = os.environ.get("FEATURES_DB","./data/gold/ceo_watchlist.db")

# yfinance is optional; script still runs without it
try:
    import yfinance as yf
except Exception:
    yf = None

SKIP_RX = re.compile(r"(^UNKN\d+$)|(^.+[PW]$)|(^.+-?WS$)|(^[A-Z]{1,5}\d+$)")  # placeholders, prefs, warrants
BATCH=3
SLEEP=8.0
BACKOFF=[10,25,45]

def ensure_tables(con):
    con.execute("""CREATE TABLE IF NOT EXISTS company_metadata(
        ticker TEXT PRIMARY KEY, company_name TEXT, sector TEXT)""")
    # seed rows for any tickers we know about
    con.executescript("""
        INSERT OR IGNORE INTO company_metadata(ticker, company_name, sector)
        SELECT DISTINCT ticker, NULL, NULL FROM ceo_tenures;
    """)

def load_csv_overlay(con):
    csv_path = "data/bronze/company_metadata.csv"
    if not os.path.exists(csv_path):
        return
    df = pd.read_csv(csv_path)
    df.to_sql("_tmp_md", con, if_exists="replace", index=False)
    # Version-agnostic UPSERT:
    # 1) UPDATE existing rows from _tmp_md
    con.executescript("""
        UPDATE company_metadata
        SET company_name = COALESCE((SELECT company_name FROM _tmp_md WHERE _tmp_md.ticker = company_metadata.ticker),
                                    company_metadata.company_name),
            sector = COALESCE(NULLIF((SELECT sector FROM _tmp_md WHERE _tmp_md.ticker = company_metadata.ticker),''),
                              company_metadata.sector)
        WHERE EXISTS (SELECT 1 FROM _tmp_md WHERE _tmp_md.ticker = company_metadata.ticker);
        -- 2) INSERT rows that don't exist yet
        INSERT INTO company_metadata(ticker, company_name, sector)
        SELECT t.ticker, t.company_name, t.sector
        FROM _tmp_md t
        WHERE NOT EXISTS (SELECT 1 FROM company_metadata cm WHERE cm.ticker = t.ticker);
        DROP TABLE _tmp_md;
    """)

def need(con):
    q = """WITH t AS (SELECT DISTINCT ticker FROM ceo_tenures)
           SELECT t.ticker FROM t
           LEFT JOIN company_metadata cm ON cm.ticker=t.ticker
           WHERE cm.ticker IS NULL OR cm.sector IS NULL OR cm.sector='' OR cm.sector='Unknown'"""
    return [t for (t,) in con.execute(q)]

def fetch_batch(tks):
    if not yf:
        return [(t, t, "") for t in tks]
    ts = yf.Tickers(" ".join(tks))
    out=[]
    for t in tks:
        try:
            info = ts.tickers[t].get_info() or {}
            name = info.get("longName") or info.get("shortName") or t
            sector = info.get("sector") or ""
            out.append((t, name, sector))
        except Exception:
            out.append((t, t, ""))
    return out

def upsert_rows(con, rows):
    if not rows: return
    df = pd.DataFrame(rows, columns=["ticker","company_name","sector"])
    df.to_sql("_tmp_md2", con, if_exists="replace", index=False)
    # 1) UPDATE existing rows
    con.executescript("""
        UPDATE company_metadata
        SET company_name = COALESCE((SELECT company_name FROM _tmp_md2 WHERE _tmp_md2.ticker = company_metadata.ticker),
                                    company_metadata.company_name),
            sector = CASE
                        WHEN (SELECT sector FROM _tmp_md2 WHERE _tmp_md2.ticker = company_metadata.ticker) IS NOT NULL
                             AND (SELECT sector FROM _tmp_md2 WHERE _tmp_md2.ticker = company_metadata.ticker) != ''
                        THEN (SELECT sector FROM _tmp_md2 WHERE _tmp_md2.ticker = company_metadata.ticker)
                        ELSE company_metadata.sector
                     END
        WHERE EXISTS (SELECT 1 FROM _tmp_md2 WHERE _tmp_md2.ticker = company_metadata.ticker);
        -- 2) INSERT new rows
        INSERT INTO company_metadata(ticker, company_name, sector)
        SELECT t.ticker, t.company_name, t.sector
        FROM _tmp_md2 t
        WHERE NOT EXISTS (SELECT 1 FROM company_metadata cm WHERE cm.ticker = t.ticker);
        DROP TABLE _tmp_md2;
    """)

def push_into_features(con):
    con.executescript("""
        UPDATE tenure_features AS tf
        SET sector = (SELECT cm.sector FROM company_metadata cm WHERE cm.ticker=tf.ticker
                      AND cm.sector IS NOT NULL AND cm.sector!='')
        WHERE (tf.sector IS NULL OR tf.sector='' OR tf.sector='Unknown')
          AND EXISTS(SELECT 1 FROM company_metadata cm WHERE cm.ticker=tf.ticker
                     AND cm.sector IS NOT NULL AND cm.sector!='');
    """)

def main():
    with sqlite3.connect(DB) as con:
        ensure_tables(con)
        load_csv_overlay(con)
        # gather targets, skip instruments that don't have sectors
        tks = [t for t in need(con) if not SKIP_RX.search(t)]
        random.shuffle(tks)
        print(f"Need sectors for {len(tks)} tickers (skipping prefs/ETFs/placeholders)")
        i=0
        while i < len(tks):
            batch = tks[i:i+BATCH]
            try:
                rows = fetch_batch(batch)
                upsert_rows(con, rows); con.commit()
                i += len(batch)
                print(f"  ✓ {i}/{len(tks)}")
                time.sleep(SLEEP)
            except Exception as e:
                print("  backoff:", str(e)[:120])
                for sec in BACKOFF:
                    time.sleep(sec)
                    try:
                        rows = fetch_batch(batch)
                        upsert_rows(con, rows); con.commit()
                        i += len(batch)
                        print(f"  ✓ (retry) {i}/{len(tks)}")
                        break
                    except Exception:
                        continue
        push_into_features(con)
        print("✅ sectors pushed to tenure_features")

if __name__ == "__main__":
    main()