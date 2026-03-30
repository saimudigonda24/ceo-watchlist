# api/app/scripts/enrich_with_prices.py
from __future__ import annotations
import argparse, re
from pathlib import Path
import pandas as pd
import numpy as np

PRICE_COLS = ["mom_5d","mom_20d","mom_63d","vol_20d","vol_63d"]
BAD_TK = {"", "NAN", "NONE", "NULL"}

def norm_tk(s: str) -> str:
    return re.sub(r"[^A-Z0-9.\-]", "", str(s).upper())

def minmax_nonempty(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn, mx = s.min(skipna=True), s.max(skipna=True)
    out = pd.Series(np.nan, index=s.index)
    mask = s.notna()
    if not np.isfinite(mn) or not np.isfinite(mx) or mx == mn:
        out.loc[mask] = 0.0
        return out
    out.loc[mask] = (s.loc[mask] - mn) / (mx - mn)
    return out

def safe_read_csv(p: str, cols: list[str] | None = None) -> pd.DataFrame:
    try:
        df = pd.read_csv(p)
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = np.nan
        return df
    except Exception:
        return pd.DataFrame(columns=cols or [])

def main():
    ap = argparse.ArgumentParser(description="Add price momentum/vol features and update final score.")
    ap.add_argument("--base", default="/data/gold/watchlist_final.csv")
    ap.add_argument("--prices", default="/data/gold/watchlist_prices.csv")
    ap.add_argument("--out", default="/data/gold/watchlist_final.csv")
    ap.add_argument("--map", default="/data/tickers.csv")
    ap.add_argument("--w-price", type=float, default=0.30)
    ap.add_argument("--w-ai",    type=float, default=0.40)
    ap.add_argument("--w-struct",type=float, default=0.30)
    args = ap.parse_args()

    # Load
    df = safe_read_csv(args.base)
    pf = safe_read_csv(args.prices, cols=["ticker"] + PRICE_COLS)

    # Build normalized key on both sides
    df["ticker_key"] = df["company"].astype(str).str.strip().str.upper().map(norm_tk)
    pf["ticker_key"] = pf["ticker"].astype(str).str.strip().str.upper().map(norm_tk)

    # Drop bogus keys
    df = df[~df["ticker_key"].isin(BAD_TK)].copy()
    pf = pf[~pf["ticker_key"].isin(BAD_TK)].copy()

    # CRITICAL: remove any stale price columns from base to avoid suffixing
    cols_to_drop = [c for c in PRICE_COLS if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # Merge: now PRICE_COLS land without _pf suffix
    m = df.merge(pf[["ticker_key"] + PRICE_COLS], how="left", on="ticker_key")
    m["ticker"] = m["ticker_key"]

    # Ensure numeric
    for c in PRICE_COLS:
        m[c] = pd.to_numeric(m[c], errors="coerce")

    # Composite (only rows with any price feature)
    has_price = m[PRICE_COLS].notna().any(axis=1)
    mom = 0.5*minmax_nonempty(m["mom_20d"]) + 0.5*minmax_nonempty(m["mom_63d"])
    vol = 0.5*minmax_nonempty(-m["vol_20d"]) + 0.5*minmax_nonempty(-m["vol_63d"])
    price_comp = (0.6*mom + 0.4*vol)

    # Percentile → 1..100 for valid rows; default 50 otherwise
    rank = price_comp.where(has_price).rank(method="average", na_option="keep")
    valid = rank.notna()
    n = int(valid.sum())
    price_100 = pd.Series(50.0, index=m.index, dtype="float64")
    if n > 1:
        price_100.loc[valid] = (((rank.loc[valid] - 1) / (n - 1)) * 99 + 1).round()
    m["price_score_100"] = price_100.astype(int)

    # Ensure AI/struct exist
    if "ai_score_100" not in m.columns:
        m["ai_score_100"] = m.get("ml_prob", 0.0).fillna(0.0).clip(0,1).mul(99).add(1).round().astype(int)
    if "score_100" not in m.columns:
        m["score_100"] = 50

    # Final blend
    w_price, w_ai, w_struct = float(args.w_price), float(args.w_ai), float(args.w_struct)
    wsum = w_price + w_ai + w_struct or 1.0
    w_price, w_ai, w_struct = w_price/wsum, w_ai/wsum, w_struct/wsum
    m["final_score_100"] = np.rint(
        w_price*m["price_score_100"] + w_ai*m["ai_score_100"] + w_struct*m["score_100"]
    ).astype(int)

    # Reason text
    m["score_reason"] = (
        "blend="
        f"{w_price:.2f}*price + {w_ai:.2f}*ai + {w_struct:.2f}*struct; "
        "mom20=" + m["mom_20d"].round(3).astype(str) + ", "
        "mom63=" + m["mom_63d"].round(3).astype(str) + ", "
        "vol20=" + m["vol_20d"].round(3).astype(str) + ", "
        "vol63=" + m["vol_63d"].round(3).astype(str)
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    m.to_csv(args.out, index=False)

    matched = int(valid.sum())
    print(
        f"Wrote {args.out}; matched_price_rows={matched} | "
        f"price_score_100[{m['price_score_100'].min()}-{m['price_score_100'].max()}] | "
        f"final_score_100[{m['final_score_100'].min()}-{m['final_score_100'].max()}]"
    )

if __name__ == "__main__":
    main()