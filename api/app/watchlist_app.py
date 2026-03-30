# api/app/watchlist_app.py

import pandas as pd
import streamlit as st

st.set_page_config(page_title="CEO Watchlist", layout="wide")

st.title("CEO Watchlist Scores")

# Load the final enriched dataset
df = pd.read_csv("/data/gold/watchlist_final.csv")

# Show ranges at the top for context
st.write(
    f"Price score range: {df['price_score_100'].min()} – {df['price_score_100'].max()} | "
    f"Final score range: {df['final_score_100'].min()} – {df['final_score_100'].max()}"
)

# Show the full table with key columns
st.dataframe(
    df[
        [
            "person",
            "company",
            "role",
            "final_score_100",
            "price_score_100",
            "ai_score_100",
            "score_100",
            "score_reason",
        ]
    ],
    use_container_width=True,
)

# Optional filters for demo
min_score, max_score = st.slider(
    "Filter by Final Score", 1, 100, (1, 100)
)
filtered = df[(df["final_score_100"] >= min_score) & (df["final_score_100"] <= max_score)]
st.write(f"Showing {len(filtered)} CEOs in range {min_score}–{max_score}")
st.dataframe(
    filtered[
        [
            "person",
            "company",
            "role",
            "final_score_100",
            "price_score_100",
            "ai_score_100",
            "score_100",
            "score_reason",
        ]
    ],
    use_container_width=True,
)