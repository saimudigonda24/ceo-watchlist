# api/app/scripts/build_scores.py
from __future__ import annotations
import argparse, json, re
from pathlib import Path
import numpy as np
import pandas as pd

# ML
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.impute import SimpleImputer
from sklearn.ensemble import GradientBoostingRegressor


def first_col(df: pd.DataFrame, regex: str) -> str | None:
    for c in df.columns:
        if re.search(regex, c, re.I):
            return c
    return None

def norm_str(x) -> str:
    if pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()

def to_num(x) -> float | None:
    if pd.isna(x):
        return np.nan
    m = re.search(r"-?\d+(?:\.\d+)?", str(x))
    return float(m.group()) if m else np.nan

def minmax(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn, mx = s.min(skipna=True), s.max(skipna=True)
    if not np.isfinite(mn) or not np.isfinite(mx) or mx == mn:
        return pd.Series(np.zeros(len(s)), index=s.index)
    return (s - mn) / (mx - mn)

def count_items(x) -> int:
    if pd.isna(x) or str(x).strip() == "":
        return 0
    return len([t for t in re.split(r"[;,|]", str(x)) if t.strip()])


def load_base_df(base_csvs: list[Path], base_json: Path) -> pd.DataFrame:
    """Load whatever is available, preferring the most enriched CSV first."""
    for p in base_csvs:
        if p.exists():
            return pd.read_csv(p)
    if base_json.exists():
        return pd.DataFrame(json.loads(base_json.read_text()))
    raise FileNotFoundError("No base file found. Expected one of: "
                            + ", ".join(map(str, base_csvs + [base_json])))


def build_scores(
    bronze_csv: Path,
    base_csvs: list[Path],
    base_json: Path,
    out_csv: Path,
    blend_weight_ai: float = 0.5,
    random_state: int = 42,
) -> None:
    # ---------- Load base ----------
    df = load_base_df(base_csvs, base_json)
    # Ensure core columns exist
    for col in ("person", "company", "role", "headline"):
        if col not in df.columns:
            df[col] = ""

    # ---------- Enrich with Boardroom Alpha CSV (extra features) ----------
    if not bronze_csv.exists():
        print(f"WARNING: bronze CSV not found at {bronze_csv}; proceeding without extra features.")
        sdf = pd.DataFrame()
    else:
        src = pd.read_csv(bronze_csv)

        person_col  = first_col(src, r"(?:^|\b)(person|name|executive)\b")
        company_col = first_col(src, r"(?:^|\b)(company|issuer|organization|org)\b")
        role_col    = first_col(src, r"(?:^|\b)(role|title|position)\b")
        rating_col  = first_col(src, r"^rating$")
        tsr_col     = first_col(src, r"career.*tsr")
        age_col     = first_col(src, r"^age$")
        gender_col  = first_col(src, r"^gender$")
        curr_co_col = first_col(src, r"current\s*companies")
        prev_co_col = first_col(src, r"previous\s*companies")

        sdf = pd.DataFrame({
            "person":  src[person_col].map(norm_str) if person_col else "",
            "company": src[company_col].map(norm_str) if company_col else "",
            "role_src":src[role_col].map(norm_str) if role_col else "",
            "rating":  src[rating_col].map(norm_str) if rating_col else "",
            "career_tsr": src[tsr_col] if tsr_col else "",
            "age":     src[age_col] if age_col else np.nan,
            "gender":  src[gender_col] if gender_col else "",
            "current_companies":  src[curr_co_col] if curr_co_col else "",
            "previous_companies": src[prev_co_col] if prev_co_col else "",
        })

        sdf["career_tsr_num"] = sdf["career_tsr"].apply(to_num)
        sdf["age_num"]        = pd.to_numeric(sdf["age"], errors="coerce")
        sdf["curr_count"]     = sdf["current_companies"].apply(count_items)
        sdf["prev_count"]     = sdf["previous_companies"].apply(count_items)
        rating_map = {"A+": 2.0, "A": 1.0}
        sdf["rating_pts"]     = sdf["rating"].map(lambda v: rating_map.get(str(v).strip().upper(), 0.0))

    if not sdf.empty:
        df = pd.merge(
            df,
            sdf[["person","company","rating","rating_pts","career_tsr","career_tsr_num",
                 "age_num","gender","curr_count","prev_count"]],
            on=["person","company"],
            how="left",
        )

    # ---------- Build a continuous target (no ml_prob to avoid circularity) ----------
    df["role_is_ceo"] = (df.get("role","").fillna("").str.upper() == "CEO").astype(float)
    df["rating_pts"]  = pd.to_numeric(df.get("rating_pts", 0.0), errors="coerce").fillna(0.0)
    df["career_tsr_num"] = pd.to_numeric(df.get("career_tsr_num", np.nan), errors="coerce")
    df["age_num"]     = pd.to_numeric(df.get("age_num", np.nan), errors="coerce")
    df["curr_count"]  = pd.to_numeric(df.get("curr_count", 0), errors="coerce").fillna(0.0)
    df["prev_count"]  = pd.to_numeric(df.get("prev_count", 0), errors="coerce").fillna(0.0)
    df["headline"]    = df.get("headline","").fillna("")

    parts = {
        "f_rate": minmax(df["rating_pts"]),
        "f_tsr":  minmax(df["career_tsr_num"]),
        "f_ceo":  minmax(df["role_is_ceo"]),
        "f_curr": minmax(df["curr_count"]),
        "f_prev": minmax(df["prev_count"]),
        "f_age":  minmax(df["age_num"]),
    }
    # base weights (same intent as before)
    base_weights = {"f_rate":0.25,"f_tsr":0.25,"f_ceo":0.15,"f_curr":0.15,"f_prev":0.05,"f_age":0.15}

    # keep only usable (non-constant, non-all-null) parts
    usable = {}
    for k, s in parts.items():
        s = pd.to_numeric(s, errors="coerce")
        if s.notna().any() and (s.max() - s.min()) > 0:
            usable[k] = s

    # renormalize weights over usable keys
    w = {k: base_weights[k] for k in usable.keys() if k in base_weights}
    wsum = sum(w.values()) or 1.0
    w = {k: v/wsum for k, v in w.items()}

    # composite from usable only
    base_cont = sum(w[k] * usable[k] for k in w) if w else pd.Series(np.zeros(len(df)), index=df.index)

    # deterministic tiny tie-breaker to spread ties
    seed = (df["person"].fillna("") + "|" + df["company"].fillna("")).apply(hash).astype("int64")
    jitter = ((seed % 1000) / 1000.0 - 0.5) * 1e-6  # [-5e-7, +5e-7]
    base_cont = base_cont + jitter

    # percentile → 1..100
    rank = base_cont.rank(method="average", na_option="keep")
    n = int(rank.count())
    if n > 1:
        pct = (rank - 1) / (n - 1)
    else:
        pct = pd.Series(np.zeros(len(rank)), index=rank.index)
    score_struct_100 = (pct*99 + 1).clip(1,100)
    # ---------- Train a regressor to predict that target from text+structure ----------
    X = pd.DataFrame({
        "role_is_ceo": df["role_is_ceo"],
        "rating_pts": df["rating_pts"],
        "career_tsr_num": df["career_tsr_num"],
        "age_num": df["age_num"],
        "curr_count": df["curr_count"],
        "prev_count": df["prev_count"],
        "headline": df["headline"],
    })
    y = score_struct_100.values  # continuous target

    num_cols = ["role_is_ceo","rating_pts","career_tsr_num","age_num","curr_count","prev_count"]
    text_col = "headline"  # make sure df['headline'] exists and is string (we fillna("") above)

    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([
                ("imp", SimpleImputer(strategy="median")),
                ("sc",  StandardScaler(with_mean=False)),
            ]), num_cols),
            ("txt", TfidfVectorizer(max_features=3000, ngram_range=(1,2)), text_col),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )
    
    # --- sanity checks (debug) ---
    assert "headline" in X.columns, f"headline missing; cols={list(X.columns)}"
    num_cols = ["role_is_ceo","rating_pts","career_tsr_num","age_num","curr_count","prev_count"]
    text_col = "headline"
    for c in num_cols:
        if c not in X.columns:
            raise ValueError(f"Missing numeric feature: {c}")
    if X[text_col].isna().any():
        raise ValueError("headline has NaN values after fillna('') step")

    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([
                ("imp", SimpleImputer(strategy="median")),
                ("sc",  StandardScaler(with_mean=False)),
            ]), num_cols),
            ("txt", TfidfVectorizer(max_features=3000, ngram_range=(1,2)), text_col),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    # print the transformers to verify they are 3-tuples
    print("DEBUG transformers:", pre.transformers)

    model = Pipeline([
        ("prep", pre),
        ("gbr", GradientBoostingRegressor(random_state=random_state)),
    ])
    model.fit(X, y)
    pred = model.predict(X)  # in-sample for MVP

    df["ai_score_100"] = np.rint(np.clip(pred, 1, 100)).astype(int)

    # Fallback: if an earlier structural score exists, keep it; else use this one
    if "score_100" not in df.columns:
        df["score_100"] = np.rint(score_struct_100).astype(int)

    # Final blend (tweakable)
    w_ai = float(blend_weight_ai)
    w_st = 1.0 - w_ai
    df["final_score_100"] = np.rint(w_ai*df["ai_score_100"] + w_st*df["score_100"]).astype(int)

    # Keep a short quantitative reason
    df["score_reason"] = (
        "blend=" + str(round(w_ai,2)) + "*ai + " + str(round(w_st,2)) + "*struct; "
        "rate=" + parts["f_rate"].round(2).astype(str) + ", "
        "tsr=" + parts["f_tsr"].round(2).astype(str) + ", "
        "ceo=" + parts["f_ceo"].round(2).astype(str) + ", "
        "curr=" + parts["f_curr"].round(2).astype(str) + ", "
        "prev=" + parts["f_prev"].round(2).astype(str) + ", "
        "age=" + parts["f_age"].round(2).astype(str)
    )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)

    print(
        f"Wrote {out_csv} rows={len(df)} | "
        f"ai_score_100:[{int(df['ai_score_100'].min())}-{int(df['ai_score_100'].max())}] "
        f"| struct_score_100:[{int(df['score_100'].min())}-{int(df['score_100'].max())}] "
        f"| final_score_100:[{int(df['final_score_100'].min())}-{int(df['final_score_100'].max())}]"
    )


def parse_args():
    p = argparse.ArgumentParser(description="Build AI + structural scores (1–100) for CEO watchlist.")
    p.add_argument("--bronze", default="/data/bronze/boardroom_alpha.csv")
    p.add_argument("--base-json", default="/data/silver/merged_ceo_changes.json")
    p.add_argument("--base-ml",   default="/data/gold/watchlist_ml.csv")
    p.add_argument("--base-ai",   default="/data/gold/watchlist_ai.csv")
    p.add_argument("--base-ranked", default="/data/gold/watchlist_ranked.csv")
    p.add_argument("--out", default="/data/gold/watchlist_final.csv")
    p.add_argument("--blend-ai", type=float, default=0.5, help="weight for AI score in final blend (0..1)")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()

def main():
    args = parse_args()
    build_scores(
        bronze_csv=Path(args.bronze),
        base_csvs=[Path(args.base_ml), Path(args.base_ai), Path(args.base_ranked)],
        base_json=Path(args.base_json),
        out_csv=Path(args.out),
        blend_weight_ai=args.blend_ai,
        random_state=args.seed,
    )

if __name__ == "__main__":
    main()