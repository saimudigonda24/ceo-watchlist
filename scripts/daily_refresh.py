# api/app/scripts/daily_refresh.py
import os, sys, subprocess
from pathlib import Path

def run(cmd):
    print("→", " ".join(cmd))
    # inherit current env, which now includes FEATURES_DB
    subprocess.check_call(cmd, env=os.environ.copy())

def main():
    # Ensure FEATURES_DB is set for all child processes (incl. -m snapshot_watchlist)
    repo_root = Path(__file__).resolve().parents[3]   # .../ceo-watchlist
    os.environ.setdefault("FEATURES_DB", str(repo_root / "data/gold/ceo_watchlist.db"))

    py = sys.executable
    run([py, "api/app/scripts/update_prices.py"])
    run([py, "api/app/scripts/update_insiders.py"])
    run([py, "api/app/scripts/refresh_features.py"])
    run([py, "-m", "api.app.scripts.snapshot_watchlist"])
    print("✅ daily refresh + snapshot complete")

if __name__ == "__main__":
    main()