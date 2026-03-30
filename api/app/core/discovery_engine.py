# api/app/core/discovery_engine.py
# Purpose: Learn leadership patterns from historical CEO tenures and score current CEOs to produce a watchlist.

from __future__ import annotations

import os
import sqlite3
from datetime import date
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

def _latest_price_date(ticker: str) -> Optional[str]:
    with sqlite3.connect(DB_PATH) as con:
        r = con.execute("SELECT MAX(d) FROM prices_daily WHERE ticker=?", (ticker,)).fetchone()
        return r[0] if r and r[0] else None

def _latest_insider_date(ticker: str) -> Optional[str]:
    with sqlite3.connect(DB_PATH) as con:
        r = con.execute("SELECT MAX(filing_date) FROM insider_trades WHERE ticker=?", (ticker,)).fetchone()
        return r[0] if r and r[0] else None

DB_PATH = os.environ.get("FEATURES_DB", "/data/gold/ceo_watchlist.db")

SUCCESS_HORIZON_DAYS = 252  # ~12 months trading days
ALPHA_SECTOR_ADJ = True     # sector-adjust returns when labeling success

# -----------------------------
# Data Shapes
# -----------------------------
@dataclass
class TenureFeatureRow:
    person_id: str
    company: str
    ticker: str
    role: str
    start_date: str
    end_date: Optional[str]
    # core leadership features (engineered in SQL or here)
    cap_alloc_buyback_rate: float
    cap_alloc_dilution_rate: float
    insider_buy_score: float
    insider_sell_score: float
    r_and_d_intensity_delta: float
    sgna_efficiency_delta: float
    headcount_growth_6m: float
    transcripts_action_verb_rate: float
    transcripts_focus_operational_rate: float
    transcripts_focus_product_rate: float
    # market context
    pre_3m_return: float
    post_12m_excess_return: Optional[float]  # label target in history
    sector: Optional[str]

@dataclass
class WatchCandidate:
    person: str
    company: str
    ticker: str
    role: str
    tenure_start: str
    leadership_score: float
    trajectory_score: float
    why_watch: str
    top_drivers: List[str]
    emergence_boost: float = 0.0
    composite_score: float = 0.0
    latest_price_date: Optional[str] = None
    latest_insider_date: Optional[str] = None
    note: str = ""
    sector: Optional[str] = None   # <-- add this line
# -----------------------------
# Utilities
# -----------------------------
def _fetch_df(sql: str, params: Tuple = ()) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as con:
        return pd.read_sql_query(sql, con, params=params)

# -----------------------------
# 1) Build Historical Training Set
# -----------------------------
def load_historical_tenure_features() -> pd.DataFrame:
    """
    Expects a materialized feature table `tenure_features` with one row per (CEO, company, tenure window).
    """
    q = """
    SELECT
      person_id, person_name, company, ticker, role, start_date, end_date, sector,
      cap_alloc_buyback_rate, cap_alloc_dilution_rate,
      insider_buy_score, insider_sell_score,
      r_and_d_intensity_delta, sgna_efficiency_delta,
      headcount_growth_6m,
      transcripts_action_verb_rate, transcripts_focus_operational_rate, transcripts_focus_product_rate,
      pre_3m_return, post_12m_excess_return
    FROM tenure_features
    WHERE post_12m_excess_return IS NOT NULL
    """
    return _fetch_df(q)

def label_success(df: pd.DataFrame, threshold: float = 0.05) -> pd.DataFrame:
    """
    Label a tenure as 'success' if post_12m_excess_return >= threshold (default +5% vs sector).
    """
    df = df.copy()
    df["success"] = (df["post_12m_excess_return"] >= threshold).astype(int)
    return df

# -----------------------------
# 2) Train Profile Model
# -----------------------------
FEATURE_COLS = [
    "cap_alloc_buyback_rate", "cap_alloc_dilution_rate",
    "insider_buy_score", "insider_sell_score",
    "r_and_d_intensity_delta", "sgna_efficiency_delta",
    "headcount_growth_6m",
    "transcripts_action_verb_rate", "transcripts_focus_operational_rate", "transcripts_focus_product_rate",
    "pre_3m_return",
]

def train_profile_model(df: pd.DataFrame) -> Pipeline:
    X = df[FEATURE_COLS].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    y = df["success"].astype(int)

    unique = np.unique(y)
    if len(unique) < 2:
        # Fallback dummy pipeline when not enough labels
        class Dummy:
            def __init__(self, p=0.5): self.p = float(p)
            def predict_proba(self, X):
                n = len(X)
                return np.vstack([np.full(n, 1-self.p), np.full(n, self.p)]).T
        class DummyPipe:
            named_steps = {"scaler": None, "clf": Dummy(0.5)}
            def predict_proba(self, X): return self.named_steps["clf"].predict_proba(X)
        return DummyPipe()

    model = Pipeline(steps=[
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(max_iter=200, class_weight="balanced")),
    ])
    model.fit(X, y)
    return model

# -----------------------------
# 3) Score Current CEOs
# -----------------------------
def load_current_ceo_features() -> pd.DataFrame:
    """
    Prefer end_date IS NULL. If none, fall back to the most recent start_date per ticker.
    Also bring in company_name/sector + latest_price_date/latest_insider_date.
    """
    # Build "tf.col AS col" for all feature columns so they match FEATURE_COLS
    # build alias list once
    aliased_feats = ", ".join([f"tf.{c} AS {c}" for c in FEATURE_COLS])

    q_current = f"""
    SELECT
    tf.person_id,
    tf.person_name,
    COALESCE(cm.company_name, tf.company) AS company_name,
    tf.company AS company_raw,
    tf.ticker,
    tf.role,
    tf.start_date,
    COALESCE(cm.sector, tf.sector, 'Unknown') AS sector,  -- <-- changed
    {aliased_feats},
    (SELECT MAX(d) FROM prices_daily p WHERE p.ticker = tf.ticker) AS latest_price_date,
    (SELECT MAX(filing_date) FROM insider_trades it WHERE it.ticker = tf.ticker) AS latest_insider_date
    FROM tenure_features tf
    LEFT JOIN company_metadata cm ON cm.ticker = tf.ticker
    WHERE tf.end_date IS NULL
    """

    q_fallback = f"""
    WITH latest AS (
    SELECT ticker, MAX(DATE(start_date)) AS max_start
    FROM tenure_features
    GROUP BY ticker
    )
    SELECT
    tf.person_id,
    tf.person_name,
    COALESCE(cm.company_name, tf.company) AS company_name,
    tf.company AS company_raw,
    tf.ticker,
    tf.role,
    tf.start_date,
    COALESCE(cm.sector, tf.sector, 'Unknown') AS sector,  -- <-- changed
    {aliased_feats},
    (SELECT MAX(d) FROM prices_daily p WHERE p.ticker = tf.ticker) AS latest_price_date,
    (SELECT MAX(filing_date) FROM insider_trades it WHERE it.ticker = tf.ticker) AS latest_insider_date
    FROM tenure_features tf
    JOIN latest l ON l.ticker = tf.ticker AND l.max_start = DATE(tf.start_date)
    LEFT JOIN company_metadata cm ON cm.ticker = tf.ticker
    """
    return _fetch_df(q_fallback)

def explain_top_drivers(model: Pipeline, x_row: pd.Series, top_k: int = 3) -> List[str]:
    """
    Quick linear explanation: coefficient * standardized feature magnitude ranking.
    Uses a one-row DataFrame to preserve column names (avoids sklearn warnings).
    """
    # Dummy pipeline (no coef_)
    if getattr(model, "named_steps", None) is None or getattr(model.named_steps.get("clf", None), "coef_", None) is None:
        return ["baseline model (insufficient labels)"]

    scaler = model.named_steps["scaler"]
    clf = model.named_steps["clf"]

    X_one = pd.DataFrame([x_row[FEATURE_COLS].fillna(0.0)], columns=FEATURE_COLS)
    x_std = scaler.transform(X_one)[0] if scaler is not None else X_one.values[0]
    contrib = clf.coef_[0] * x_std  # per-feature contributions

    ranked_idx = np.argsort(-contrib)
    out = []
    for i in ranked_idx[:top_k]:
        feat = FEATURE_COLS[i]
        val = float(X_one.iloc[0, i])
        sign = "↑" if contrib[i] >= 0 else "↓"
        out.append(f"{feat} {sign} (val={val:.3f})")
    return out

def score_current(model: Pipeline, df_now: pd.DataFrame) -> List[WatchCandidate]:
    X = df_now[FEATURE_COLS].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    probs = model.predict_proba(X)[:, 1]  # P(success)

    # crude trajectory = recent features vs. cohort median
    med = X.median(axis=0)
    traj = (X - med).mean(axis=1)  # single scalar proxy

    out: List[WatchCandidate] = []
    rows = df_now.reset_index(drop=True)
    for i, row in rows.iterrows():
        leadership_score = float(probs[i])
        trajectory_score = float(traj.iloc[i])
        drivers = explain_top_drivers(model, row, top_k=3)

        # --- Emergence boost: newer CEO + insider buying ---
        try:
            start_dt = pd.to_datetime(row["start_date"]).date()
        except Exception:
            start_dt = date.today()
        tenure_days = max(1, (date.today() - start_dt).days)
        newness = max(0.0, 1.0 - (tenure_days / 540.0))  # fades by ~18 months
        insider = float(row.get("insider_buy_score", 0.0))
        emergence_boost = 0.25 * newness + 0.10 * min(insider, 1.0)

        leadership_score_adj = min(1.0, leadership_score + 0.5 * emergence_boost)

        # Freshness from the SELECT (avoid per-row DB calls)
        lp = row.get("latest_price_date", None)
        li = row.get("latest_insider_date", None)

        why = (
            f"P(success)={leadership_score_adj:.2f}. "
            f"Signals: {', '.join(drivers)}. "
            f"Emergence={emergence_boost:.3f} (newness={newness:.2f}, insider={min(insider,1.0):.2f}). "
            f"Sector={row.get('sector','?')}."
        )
        note = (
            f"{row['ticker']} — {row.get('company_name') or row.get('company_raw') or row['ticker']} | "
            f"P(s)={leadership_score_adj:.2f}, Emerg={emergence_boost:.3f}. "
            f"Tenure start {row['start_date']}. Freshness: price {lp or 'n/a'}, insider {li or 'n/a'}."
        )

        out.append(WatchCandidate(
        person=row["person_name"],
        company=(row.get("company_name") or row.get("company_raw") or row["ticker"]),
        ticker=row["ticker"],
        role=row["role"],
        tenure_start=row["start_date"],
        leadership_score=leadership_score_adj,
        trajectory_score=trajectory_score,
        why_watch=why,
        top_drivers=drivers,
        emergence_boost=emergence_boost,
        latest_price_date=lp,
        latest_insider_date=li,
        note=note,
        sector=row.get("sector")  # <-- add this
    ))
    return out

# -----------------------------
# --- baseline fallback when there is no labeled history ---
def score_current_baseline(df_now: pd.DataFrame) -> List[WatchCandidate]:
    # Normalize simple features and build a heuristic P(success)
    def z(x):
        x = pd.Series(x).astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        return (x - x.mean()) / (x.std(ddof=0) + 1e-9)

    # Safe gets
    ib = df_now.get("insider_buy_score", pd.Series(0.0, index=df_now.index)).astype(float)
    isell = df_now.get("insider_sell_score", pd.Series(0.0, index=df_now.index)).astype(float)
    pre3 = df_now.get("pre_3m_return", pd.Series(0.0, index=df_now.index)).astype(float)

    # Heuristic “leadership” score in [0,1]
    raw = 0.6*z(ib) - 0.2*z(isell) + 0.2*z(pre3)
    p = 1 / (1 + np.exp(-raw))  # sigmoid

    # Trajectory proxy
    feats = ["cap_alloc_buyback_rate","cap_alloc_dilution_rate","r_and_d_intensity_delta",
             "sgna_efficiency_delta","headcount_growth_6m",
             "transcripts_action_verb_rate","transcripts_focus_operational_rate","transcripts_focus_product_rate","pre_3m_return"]
    X = df_now[[c for c in feats if c in df_now.columns]].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    med = X.median(axis=0) if not X.empty else 0.0
    traj = (X - med).mean(axis=1) if not X.empty else pd.Series(0.0, index=df_now.index)

    out: List[WatchCandidate] = []
    rows = df_now.reset_index(drop=True)

    for i, row in rows.iterrows():
        leadership_score = float(p.iloc[i])
        trajectory_score = float(traj.iloc[i])

        # Emergence boost (same as your model path)
        try:
            start_dt = pd.to_datetime(row["start_date"]).date()
        except Exception:
            start_dt = date.today()
        tenure_days = max(1, (date.today() - start_dt).days)
        newness = max(0.0, 1.0 - (tenure_days / 540.0))
        insider = float(row.get("insider_buy_score", 0.0))
        emergence_boost = 0.25 * newness + 0.10 * min(insider, 1.0)
        leadership_score_adj = min(1.0, leadership_score + 0.5 * emergence_boost)

        # Freshness helpers you already added
        lp = _latest_price_date(row["ticker"])
        li = _latest_insider_date(row["ticker"])

        drivers = ["baseline (no labeled history)"]
        why = (
            f"P(success)={leadership_score_adj:.2f}. "
            f"Signals: {', '.join(drivers)}. "
            f"Sector={row.get('sector','?')}."
        )
        note = (
            f"{row['ticker']} | P(s)={leadership_score_adj:.2f}, "
            f"Emerg={emergence_boost:.3f}. Drivers: {', '.join(drivers)}. "
            f"Tenure start {row['start_date']}. Freshness: price {lp or 'n/a'}, insider {li or 'n/a'}."
        )

        out.append(WatchCandidate(
            person=row["person_name"],
            company=(row.get("company_name") or row.get("company_raw") or row["ticker"]),
            ticker=row["ticker"],
            role=row["role"],
            tenure_start=row["start_date"],
            leadership_score=leadership_score_adj,
            trajectory_score=trajectory_score,
            why_watch=why,
            top_drivers=drivers,
            emergence_boost=emergence_boost,
            latest_price_date=lp,
            latest_insider_date=li,
            note=note,
            sector=row.get("sector")  # <-- add this
        ))
    return out
# 4) Public API for the service layer
# -----------------------------
def build_watchlist(
    k: int = 50,
    min_prob: float = 0.55,
    role: str = "Any",
    unique_by: str = "ticker",
) -> List[Dict[str, Any]]:
    # 1) Train on history (may be empty on first runs)
    hist = load_historical_tenure_features()
    has_history = not hist.empty
    if has_history:
        labeled = label_success(hist)
        model = train_profile_model(labeled)

    # 2) Current candidates
    current_df = load_current_ceo_features()
    if current_df.empty:
        return []

    # 3) Score (fallback if no history)
    scored = score_current(model, current_df) if has_history else score_current_baseline(current_df)

    # 4) Role filter
    if role and role.lower() != "any":
        scored = [s for s in scored if (s.role or "").upper() == role.upper()]

    # 5) De-dup
    if unique_by in {"ticker", "person"}:
        seen, uniq = set(), []
        for s in scored:
            key = getattr(s, unique_by, None)
            if not key or key in seen:
                continue
            seen.add(key)
            uniq.append(s)
        scored = uniq

    # 6) Composite and sort
    for s in scored:
        s.composite_score = s.leadership_score + 0.15 * getattr(s, "emergence_boost", 0.0)
    scored.sort(key=lambda w: (w.composite_score, w.leadership_score, w.trajectory_score), reverse=True)

    # 7) Threshold + top-k
    filtered = [s for s in scored if s.leadership_score >= min_prob][:k]

    # 8) Dicts
    return [asdict(x) for x in filtered]