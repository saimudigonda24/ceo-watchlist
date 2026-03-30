from pathlib import Path
import os
import pandas as pd  # requires pandas + openpyxl
import json
import sys

EXCEL_PATH = Path(os.getenv("CEO_EXCEL_PATH", "/data/bronze/boardroom_alpha.xlsx"))
OUT_DIR = Path(os.getenv("CEO_SILVER_DIR", "/data/silver"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_JSON = OUT_DIR / "boardroom_alpha.json"

def main():
    if not EXCEL_PATH.exists():
        sys.stderr.write(f"[ERROR] Excel not found at: {EXCEL_PATH}\n"
                         f"Set CEO_EXCEL_PATH or place the file at /data.\n")
        sys.exit(1)

    # Read first sheet (adjust if you know the exact sheet name)
    df = pd.read_excel(EXCEL_PATH)

    # Normalize columns (rename to match your JSON schema as needed)
    # Example mapping – tweak to your real column names:
    colmap = {
        "Source": "source",
        "Person": "person",
        "Company": "company",
        "Role": "role",
        "Headline": "headline",
    }
    df = df.rename(columns={k: v for k, v in colmap.items() if k in df.columns})

    # Keep only the target columns if present
    keep = ["source", "person", "company", "role", "headline"]
    df = df[[c for c in keep if c in df.columns]]

    data = df.to_dict(orient="records")
    OUT_JSON.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    print(f"Wrote {OUT_JSON} ({len(data)} rows)")

if __name__ == "__main__":
    main()