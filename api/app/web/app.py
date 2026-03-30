import pandas as pd
import streamlit as st
from pathlib import Path

DATA_CSV = Path("/data/gold/watchlist_final.csv")

st.set_page_config(page_title="CEO Watchlist", page_icon="📈", layout="wide")
st.title("📈 CEO Watchlist — MVP")

if not DATA_CSV.exists():
    st.error(f"Missing data at {DATA_CSV}. Run the pipeline to create it.")
    st.stop()

df = pd.read_csv(DATA_CSV)
if "score_100" not in df.columns:
    st.error("score_100 column not found. Current columns: " + ", ".join(df.columns))
    st.stop()

st.sidebar.header("Filters")
q = st.sidebar.text_input("Search (person/company/headline)").strip().lower()
roles = st.sidebar.multiselect("Role", sorted(df["role"].dropna().unique().tolist()))
labels = st.sidebar.multiselect("AI label", sorted(df.get("ai_label", pd.Series()).dropna().unique().tolist()))
min_score100 = st.sidebar.slider("Min Score (1–100)", 1, 100, 1, 1)

f = df.copy()
if q:
    f = f[f.apply(lambda r: q in str(r.get("person","")).lower()
                        or q in str(r.get("company","")).lower()
                        or q in str(r.get("headline","")).lower(), axis=1)]
if roles:
    f = f[f["role"].isin(roles)]
if labels and "ai_label" in f.columns:
    f = f[f["ai_label"].isin(labels)]
f = f[f["score_100"] >= min_score100]

left, mid, right = st.columns(3)
left.metric("Rows (filtered)", len(f))
mid.metric("CEOs (filtered)", int((f["role"].fillna("").str.upper()=="CEO").sum()))
right.metric("Top Score", int(f["score_100"].max()) if len(f) else 0)

st.subheader("Watchlist (sorted by Score 1–100)")
cols = ["score_100","person","company","role","ml_prob","ai_label","entry_hint","headline","source"]
cols = [c for c in cols if c in f.columns]
f_view = f.sort_values(by=["score_100"], ascending=False)

st.dataframe(
    f_view[cols],
    width="stretch",
    hide_index=True,
    column_config={
        "score_100": st.column_config.NumberColumn("Score (1–100)", min_value=1, max_value=100, step=1),
        "ml_prob": st.column_config.NumberColumn("ML Prob", min_value=0.0, max_value=1.0, step=0.01, format="%.2f"),
    },
)

st.download_button(
    "Download filtered CSV",
    data=f_view[cols].to_csv(index=False).encode("utf-8"),
    file_name="watchlist_filtered.csv",
    mime="text/csv",
)
