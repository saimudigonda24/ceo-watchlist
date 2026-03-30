# api/app/scripts/snapshot_watchlist.py
import os, csv, datetime as dt
from pathlib import Path
from ..core.discovery_engine import build_watchlist

def main():
    # where to save snapshots
    outdir = Path(os.environ.get("SNAPSHOT_DIR", "data/snapshots"))
    outdir.mkdir(parents=True, exist_ok=True)

    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    params = dict(k=50, min_prob=0.0, role="any", unique_by="ticker")
    items = build_watchlist(**params)

    # filename includes date-time
    outpath = outdir / f"watchlist_{ts}.csv"
    if items:
        cols = sorted(items[0].keys())
        with outpath.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(items)
    print(f"Saved snapshot: {outpath} count={len(items)}")

if __name__ == "__main__":
    main()