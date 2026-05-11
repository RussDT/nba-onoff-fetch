"""Fetch WNBA league-wide season totals from PBPStats."""

import argparse
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from fetch_season_totals import HEADERS, add_derived_cols

DATA_DIR = Path(__file__).resolve().parent / "data"
WNBA_TOTALS_URL = "https://api.pbpstats.com/get-totals/wnba"
REQUIRED_TOTALS_COLUMNS = {
    "FG2A",
    "FG3A",
    "FTA",
    "Turnovers",
    "OffRebounds",
    "OffPoss",
    "Points",
}


def current_wnba_season():
    return datetime.utcnow().year


def fetch_totals(year, season_type="Regular Season", leverage=False):
    params = {
        "Season": str(year),
        "SeasonType": season_type,
        "Type": "Team",
    }
    if leverage:
        params["Leverage"] = "Medium,High,VeryHigh"

    for attempt in range(1, 16):
        try:
            time.sleep(2)
            resp = requests.get(WNBA_TOTALS_URL, params=params, headers=HEADERS, timeout=(10, 30))
            data = resp.json()
            row = data.get("single_row_table_data") or {}
            missing = REQUIRED_TOTALS_COLUMNS - set(row)
            if missing:
                print(
                    "  No usable totals returned: "
                    f"year={year} type={season_type} leverage={leverage} "
                    f"(missing {sorted(missing)})"
                )
                return pd.DataFrame()
            row["year"] = year
            print(f"  Fetched: year={year} type={season_type} leverage={leverage}")
            return pd.DataFrame([row])
        except Exception as e:
            print(f"  Attempt {attempt}/15 failed: {e}")
            if attempt == 15:
                return pd.DataFrame()
            time.sleep(2)


def update_csv(csv_path, new_data, year):
    if csv_path.exists():
        existing = pd.read_csv(csv_path)
        print(f"  Loaded {len(existing)} rows from {csv_path.name}")
        existing = existing[existing["year"] != year]
    else:
        existing = pd.DataFrame()
        print(f"  {csv_path.name} not found, creating new")

    combined = pd.concat([existing, new_data], ignore_index=True)
    combined = combined.drop_duplicates(subset=["year"], keep="last")
    combined = combined.sort_values("year").reset_index(drop=True)
    combined = add_derived_cols(combined)
    combined.to_csv(csv_path, index=False)
    print(f"  Saved {len(combined)} rows to {csv_path.name}")


def main():
    parser = argparse.ArgumentParser(description="Fetch WNBA PBPStats season totals")
    parser.add_argument("--year", type=int, default=None, help="WNBA season year")
    args = parser.parse_args()
    year = args.year or current_wnba_season()

    print(f"=== fetch_wnba_season_totals.py (year={year}) ===\n")

    jobs = [
        ("Regular Season", False, "wnba_season_totals.csv"),
        ("Playoffs", False, "wnba_season_totals_playoffs.csv"),
        ("Regular Season", True, "wnba_season_totals_leverage.csv"),
        ("Playoffs", True, "wnba_season_totals_playoffs_leverage.csv"),
    ]
    for season_type, leverage, filename in jobs:
        print(f"\n--- {season_type}{' leverage' if leverage else ''} ---")
        data = fetch_totals(year, season_type=season_type, leverage=leverage)
        if not data.empty:
            update_csv(DATA_DIR / filename, data, year)

    print("\nDone.")


if __name__ == "__main__":
    main()
