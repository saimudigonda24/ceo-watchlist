from pathlib import Path
import json

# inputs
SCRAPED_JSON = Path("/data/bronze/intellizence_ceo_changes.json")  # adjust if your scrape writes elsewhere
BOARDROOM_JSON = Path("/data/silver/boardroom_alpha.json")          # from ingest step above

# output
OUT = Path("/data/silver/merged_ceo_changes.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

def load_json(p: Path):
    return json.loads(p.read_text()) if p.exists() else []

def main():
    scraped = load_json(SCRAPED_JSON)
    board   = load_json(BOARDROOM_JSON)

    # very basic merge + de-dupe by (person, company, role, headline)
    def key(rec):
        return tuple((rec.get(k) or "").strip().lower() for k in ("person","company","role","headline"))

    merged = []
    seen = set()
    for rec in scraped + board:
        k = key(rec)
        if k not in seen:
            seen.add(k)
            merged.append(rec)

    OUT.write_text(json.dumps(merged, ensure_ascii=False, indent=2))
    print(f"Merged {len(scraped)} + {len(board)} → {len(merged)} into {OUT}")

if __name__ == "__main__":
    main()