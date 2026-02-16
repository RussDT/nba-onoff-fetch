"""
Fetch league-wide season totals from PBPStats and update CSV files.

Produces 4 CSV files in data/:
  - season_totals.csv              (Regular Season, non-leverage)
  - season_totals_playoffs.csv     (Playoffs, non-leverage)
  - season_totals_leverage.csv     (Regular Season, leverage)
  - season_totals_playoffs_leverage.csv (Playoffs, leverage)

Each run replaces the current year's row with fresh data.
"""

import argparse
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


def current_nba_season():
    """Auto-detect the current NBA season year.

    NBA seasons span two calendar years (e.g. 2025-26 season = year 2026).
    Oct-Dec: upcoming season (next calendar year).
    Jan-Sep: current season (current calendar year).
    """
    now = datetime.utcnow()
    return now.year + 1 if now.month >= 10 else now.year

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.183"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

DATA_DIR = Path(__file__).resolve().parent / "data"


def fetch_totals(year, season_type="Regular Season", leverage=False):
    """Fetch a single season's totals from PBPStats."""
    season = f"{year - 1}-{str(year)[-2:]}"
    params = {
        "Season": season,
        "SeasonType": season_type,
        "Type": "Team",
    }
    if leverage:
        params["Leverage"] = "Medium,High,VeryHigh"

    url = "https://api.pbpstats.com/get-totals/nba"
    max_retries = 15

    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(2)
            resp = requests.get(url, params=params, headers=HEADERS, timeout=(10, 30))
            data = resp.json()
            row = data["single_row_table_data"]
            row["year"] = year
            print(f"  Fetched: year={year} type={season_type} leverage={leverage}")
            return pd.DataFrame([row])
        except Exception as e:
            print(f"  Attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(2)
            else:
                print(f"  Max retries reached, skipping.")
                return pd.DataFrame()


def add_derived_cols(df):
    """Add FTA_Rate and TOV% columns."""
    fga = df["FG3A"] + df["FG2A"]
    df["FTA_Rate"] = df["FTA"] / fga
    df["TOV%"] = 100 * df["Turnovers"] / (
        fga + (0.44 * df["FTA"]) + df["Turnovers"] - df["OffRebounds"]
    )
    return df


def update_csv(csv_path, new_data, year):
    """Load existing CSV, replace current year rows, append new data."""
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
    return combined


def main():
    parser = argparse.ArgumentParser(description="Fetch PBPStats season totals")
    parser.add_argument("--year", type=int, default=None, help="Season year (auto-detects if omitted)")
    args = parser.parse_args()
    year = args.year or current_nba_season()

    print(f"=== fetch_season_totals.py (year={year}) ===\n")

    # 1. Regular Season (non-leverage)
    print("--- Regular Season ---")
    rs_data = fetch_totals(year, "Regular Season", leverage=False)
    if not rs_data.empty:
        update_csv(DATA_DIR / "season_totals.csv", rs_data, year)

    # 2. Playoffs (non-leverage)
    print("\n--- Playoffs ---")
    ps_data = fetch_totals(year, "Playoffs", leverage=False)
    if not ps_data.empty:
        update_csv(DATA_DIR / "season_totals_playoffs.csv", ps_data, year)

    # 3. Regular Season (leverage)
    print("\n--- Regular Season (Leverage) ---")
    rs_lev = fetch_totals(year, "Regular Season", leverage=True)
    if not rs_lev.empty:
        update_csv(DATA_DIR / "season_totals_leverage.csv", rs_lev, year)

    # 4. Playoffs (leverage)
    print("\n--- Playoffs (Leverage) ---")
    ps_lev = fetch_totals(year, "Playoffs", leverage=True)
    if not ps_lev.empty:
        update_csv(DATA_DIR / "season_totals_playoffs_leverage.csv", ps_lev, year)

    print("\nDone.")


if __name__ == "__main__":
    main()
