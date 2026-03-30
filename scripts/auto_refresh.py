# api/app/scripts/auto_refresh.py
import os, time, subprocess, sys

DB = os.environ.get("FEATURES_DB", "./data/gold/ceo_watchlist.db")

def run(cmd):
    print("→", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    while True:
        try:
            # 1) update prices (you’ll wire this script next)
            run([sys.executable, "api/app/scripts/update_prices.py"])
            # 2) update insiders (wire next)
            run([sys.executable, "api/app/scripts/update_insiders.py"])
            # 3) rebuild features
            run([sys.executable, "api/app/scripts/refresh_features.py"])
            print("✅ hourly refresh complete")
        except subprocess.CalledProcessError as e:
            print("❌ refresh step failed:", e)
        time.sleep(3600)  # 1 hour
if __name__ == "__main__":
    main()