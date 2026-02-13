"""
PBPStats On-Off Fetch
=====================
Fetches WOWY (With Or Without You) data from PBPStats API for all NBA teams.

Two blocks of calls:
  Block 1 (leverage): Leverage='Medium,High,VeryHigh' -> {team_id}_leverage.csv, {team_id}_vs_leverage.csv
  Block 2 (non-leverage): No Leverage param -> {team_id}.csv, {team_id}_vs.csv

Each block: 30 teams x 2 (Team + Opponent) = 60 calls. Total: 120 calls.

Output:
  output/data/{year}/{team_id}.csv
  output/data/{year}/{team_id}_vs.csv
  output/data/{year}/{team_id}_leverage.csv
  output/data/{year}/{team_id}_vs_leverage.csv

Usage:
  python fetch_onoff.py --year 2026
"""

import argparse
import os
import sys
import time

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WOWY_URL = "https://api.pbpstats.com/get-wowy-stats/nba"
INDEX_MASTER_URL = (
    "https://raw.githubusercontent.com/gabriel1200/player_sheets/master/lineups/index_master.csv"
)
REFERENCE_YEAR = 2025  # Always use this year's teams for team list

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.183"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,"
        "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Ch-Ua": '"Not/A)Brand";v="99", "Microsoft Edge";v="115", "Chromium";v="115"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

SLEEP_BETWEEN = 1.0  # seconds between API calls
MAX_RETRIES = 5
RETRY_DELAY = 3  # seconds between retries
REQUEST_TIMEOUT = (10, 30)  # (connect, read)


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def fetch_team_ids(year=REFERENCE_YEAR):
    """Get unique team IDs from index_master.csv on GitHub."""
    print(f"Fetching index_master.csv (reference year {year})...")
    df = pd.read_csv(INDEX_MASTER_URL)
    df = df.dropna()
    df = df[df.team != "TOT"]
    df = df[df.year == year].drop_duplicates()
    team_ids = sorted(df.team_id.unique().astype(int).tolist())
    print(f"  Found {len(team_ids)} teams")
    return team_ids


def lineuppull(team_id, season, opp=False, leverage=False):
    """Fetch WOWY stats for one team from PBPStats."""
    params = {
        "TeamId": team_id,
        "Season": season,
        "SeasonType": "Regular Season",
        "Type": "Opponent" if opp else "Team",
    }
    if leverage:
        params["Leverage"] = "Medium,High,VeryHigh"

    resp = requests.get(
        WOWY_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    data = resp.json()
    combos = data["multi_row_table_data"]
    df = pd.DataFrame(combos, index=[0] * len(combos))
    return df


def get_filename(team_id, opp=False, leverage=False):
    """Generate filename: {team_id}[_vs][_leverage].csv"""
    name = str(team_id)
    if opp:
        name += "_vs"
    if leverage:
        name += "_leverage"
    name += ".csv"
    return name


# ---------------------------------------------------------------------------
# Fetch block
# ---------------------------------------------------------------------------

def pull_block(team_ids, year, leverage=False):
    """
    Run one block of fetches (leverage or non-leverage).
    For each team: Team + Opponent = 2 calls.
    Returns (team_frames, vs_frames, fail_list).
    """
    season = f"{year - 1}-{str(year)[-2:]}"
    output_dir = f"output/data/{year}"
    os.makedirs(output_dir, exist_ok=True)

    tag = "leverage" if leverage else "non-leverage"
    team_frames = []
    vs_frames = []
    fail_list = []

    for opp in [False, True]:
        side = "Opponent" if opp else "Team"
        print(f"\n--- {tag} / {side} ---")

        for team_id in team_ids:
            filename = get_filename(team_id, opp=opp, leverage=leverage)
            filepath = os.path.join(output_dir, filename)

            attempts = 0
            success = False
            while attempts < MAX_RETRIES and not success:
                try:
                    df = lineuppull(team_id, season, opp=opp, leverage=leverage)
                    success = True
                except Exception as e:
                    attempts += 1
                    print(f"  Attempt {attempts} failed for {team_id} ({side}): {e}")
                    if attempts < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)

            if not success:
                print(f"  FAILED {team_id} ({side}) after {MAX_RETRIES} attempts")
                fail_list.append((team_id, side))
                continue

            df = df.reset_index(drop=True)
            df["team_id"] = team_id
            df["year"] = year
            df["season"] = season
            df["team_vs"] = opp
            if "Corner3FGM" not in df.columns:
                df["Corner3FGM"] = 0

            if len(df) > 2:
                df.to_csv(filepath, index=False)
                print(f"  Saved {filename} ({len(df)} rows)")
            else:
                print(f"  Skipped {filename} (only {len(df)} rows)")

            if opp:
                vs_frames.append(df)
            else:
                team_frames.append(df)

            time.sleep(SLEEP_BETWEEN)

    return team_frames, vs_frames, fail_list


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fetch PBPStats on-off data")
    parser.add_argument("--year", type=int, default=2026, help="Season year (default: 2026)")
    args = parser.parse_args()

    year = args.year
    print(f"=== PBPStats On-Off Fetch: {year - 1}-{str(year)[-2:]} season ===\n")

    team_ids = fetch_team_ids()
    total_calls = len(team_ids) * 2 * 2  # teams * sides * blocks
    print(f"Will make {total_calls} API calls\n")

    start = time.time()

    # Block 1: Leverage
    print("=" * 60)
    print("BLOCK 1: LEVERAGE (Medium,High,VeryHigh)")
    print("=" * 60)
    lev_team, lev_vs, lev_fails = pull_block(team_ids, year, leverage=True)

    # Block 2: Non-leverage
    print("\n" + "=" * 60)
    print("BLOCK 2: NON-LEVERAGE (all possessions)")
    print("=" * 60)
    nl_team, nl_vs, nl_fails = pull_block(team_ids, year, leverage=False)

    # Summary
    elapsed = time.time() - start
    all_fails = lev_fails + nl_fails

    data_dir = f"output/data/{year}"
    file_count = len([f for f in os.listdir(data_dir) if f.endswith(".csv")])

    print(f"\n{'=' * 60}")
    print(f"DONE in {elapsed:.0f}s")
    print(f"  Files written: {file_count} in {data_dir}/")
    if all_fails:
        print(f"  Failures ({len(all_fails)}):")
        for team_id, side in all_fails:
            print(f"    Team {team_id} ({side})")
    else:
        print("  No failures")

    if all_fails:
        sys.exit(1)


if __name__ == "__main__":
    main()
