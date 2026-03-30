from __future__ import annotations
import argparse, time, re, sys
from pathlib import Path
import pandas as pd
import numpy as np

def norm_tk(s: str) -> str:
    return re.sub(r"[^A-Z0-9.\-]", "", str(s).upper())

def pct(s: pd.Series) -> pd.Series:
    return s.pct_change()

def _as_float(x) -> float | np.nan:
    # robustly convert a scalar / 0-dim array / length-1 Series to float
    try:
        if hasattr(x, "item"):
            return float(x.item())
        return float(x)
    except Exception:
        return np.nan

def download_prices(ticker: str, yf, period="2y", retries=3, sleep_base=0.4) -> pd.Series:
    for i in range(retries):
        try:
            df = yf.download(ticker, period=period, interval="1d",
                             auto_adjust=True, progress=False, threads=False)
            if isinstance(df, pd.DataFrame) and not df.empty and "Close" in df.columns:
                s = df["Close"].dropna()
                s.index = pd.to_datetime(s.index)
                return s.rename(ticker)
        except Exception:
            pass
        time.sleep(sleep_base * (i + 1))  # backoff

    # Fallback path
    try:
        h = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=True)
        if isinstance(h, pd.DataFrame) and not h.empty and "Close" in h.columns:
            s = h["Close"].dropna()
            s.index = pd.to_datetime(s.index)
            return s.rename(ticker)
    except Exception:
        pass

    return pd.Series(dtype=float, name=ticker)

def compute_features(s: pd.Series) -> dict:
    out = {}
    if s is None or s.dropna().empty:
        return out
    r = pct(s).dropna()

    def mom(w):
        if len(s) > w:
            last = _as_float(s.iloc[-1])
            prev = _as_float(s.iloc[-1 - w])
            return (last - prev) / prev if np.isfinite(prev) and prev != 0 else np.nan
        return np.nan

    def vol(w):
        return float(r.iloc[-w:].std(ddof=0) * np.sqrt(252.0)) if len(r) >= w else np.nan

    out["mom_5d"]  = mom(5)
    out["mom_20d"] = mom(20)
    out["mom_63d"] = mom(63)
    out["vol_20d"] = vol(20)
    out["vol_63d"] = vol(63)

    # keep only if at least one feature exists
    return out if any(pd.notna(v) for v in out.values()) else {}

def main():
    ap = argparse.ArgumentParser(description="Build price features per ticker.")
    ap.add_argument("--in",  dest="in_path",  default="/data/gold/watchlist_final.csv")
    ap.add_argument("--out", dest="out_path", default="/data/gold/watchlist_prices.csv")
    ap.add_argument("--period", default="2y")
    ap.add_argument("--map", dest="map_path", default="/data/tickers.csv")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    base = pd.read_csv(args.in_path)
    base["company_u"] = base["company"].astype(str).str.strip().str.upper()

    # Optional mapping file
    tickers = base["company_u"].map(norm_tk)
    mp = Path(args.map_path)
    if mp.exists():
        tm = pd.read_csv(mp)
        if set(["company","ticker"]).issubset(tm.columns):
            tm["company_u"] = tm["company"].astype(str).str.strip().str.upper()
            tm["ticker_u"]  = tm["ticker"].astype(str).str.strip().str.upper()
            d = dict(zip(tm["company_u"], tm["ticker_u"]))
            tickers = base["company_u"].map(lambda c: d.get(c, norm_tk(c)))

    tickers = [t for t in pd.Series(tickers).dropna().unique().tolist() if t]
    if args.limit:
        tickers = tickers[:args.limit]

    if not tickers:
        Path(args.out_path).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["ticker","mom_5d","mom_20d","mom_63d","vol_20d","vol_63d"]).to_csv(args.out_path, index=False)
        print(f"Wrote 0 tickers → {args.out_path}")
        return

    import yfinance as yf
    rows, kept, skipped = [], 0, 0
    for i, t in enumerate(tickers, 1):
        s = download_prices(t, yf, period=args.period, retries=3, sleep_base=0.5)
        nrows = 0 if s is None else int(len(s))
        feats = compute_features(s)
        status = "OK" if feats else "EMPTY"
        print(f"[{i}/{len(tickers)}] {t}: rows={nrows} -> {status}", file=sys.stderr)
        if feats:
            feats["ticker"] = t
            rows.append(feats)
            kept += 1
        else:
            skipped += 1
        time.sleep(0.25)

    out = pd.DataFrame(rows)
    Path(args.out_path).parent.mkdir(parents=True, exist_ok=True)
    if out.empty:
        out = pd.DataFrame(columns=["ticker","mom_5d","mom_20d","mom_63d","vol_20d","vol_63d"])
    out.to_csv(args.out_path, index=False)
    print(f"Wrote {kept} tickers (skipped {skipped}) → {args.out_path}")

if __name__ == "__main__":
    main()