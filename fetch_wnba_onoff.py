"""
Fetch WNBA PBPStats WOWY data for all active WNBA teams.

Outputs the same file shape as fetch_onoff.py, under output/wnba_data/{year}/:
  {team_id}.csv
  {team_id}_vs.csv
  {team_id}_leverage.csv
  {team_id}_vs_leverage.csv
  {team_id}_ps.csv
  {team_id}_vs_ps.csv
  {team_id}_ps_leverage.csv
  {team_id}_vs_ps_leverage.csv
"""

import argparse
import os
import sys
import time
from datetime import datetime

import pandas as pd

import fetch_onoff as nba_fetch

WNBA_TEAM_IDS = [
    1611661313,  # New York Liberty
    1611661317,  # Phoenix Mercury
    1611661319,  # Las Vegas Aces
    1611661320,  # Los Angeles Sparks
    1611661321,  # Dallas Wings
    1611661322,  # Washington Mystics
    1611661323,  # Connecticut Sun
    1611661324,  # Minnesota Lynx
    1611661325,  # Indiana Fever
    1611661327,  # Portland Fire
    1611661328,  # Seattle Storm
    1611661329,  # Chicago Sky
    1611661330,  # Atlanta Dream
    1611661331,  # Golden State Valkyries
    1611661332,  # Toronto Tempo
]

WNBA_WOWY_URL = "https://api.pbpstats.com/get-wowy-stats/wnba"
BLOCK_RETRY_DELAY = 30


def current_wnba_season():
    """WNBA seasons are single-calendar-year seasons."""
    return datetime.utcnow().year


def season_plan(include_playoffs=False):
    plan = [("Regular Season", False)]
    if include_playoffs:
        plan.append(("Playoffs", True))
    return plan


def parse_team_ids(values):
    if not values:
        return WNBA_TEAM_IDS

    team_ids = []
    valid_team_ids = set(WNBA_TEAM_IDS)
    for value in values:
        for raw_part in str(value).split(","):
            part = raw_part.strip()
            if not part:
                continue
            try:
                team_id = int(part)
            except ValueError as exc:
                raise SystemExit(f"Invalid --team-id value: {part}") from exc
            if team_id not in valid_team_ids:
                raise SystemExit(f"Unknown WNBA team id: {team_id}")
            team_ids.append(team_id)

    return sorted(set(team_ids))


def lineuppull_full(team_id, year, season_type, opp=False, leverage=False):
    params = {
        "TeamId": team_id,
        "Season": str(year),
        "SeasonType": season_type,
        "Type": "Opponent" if opp else "Team",
    }
    if leverage:
        params["Leverage"] = "Medium,High,VeryHigh"

    df = nba_fetch._api_call(params)
    if len(df) < nba_fetch.ROW_CAP:
        return df

    side = "Opponent" if opp else "Team"
    tag = "leverage" if leverage else "non-leverage"
    print(f"    {team_id} ({side}/{tag}): hit {nba_fetch.ROW_CAP}-row cap, splitting by date...")

    splits = [
        (f"{year}-05-01", f"{year}-07-15"),
        (f"{year}-07-16", f"{year}-11-15"),
    ]
    split_dfs = []
    for from_d, to_d in splits:
        time.sleep(nba_fetch.SLEEP_BETWEEN)
        split_params = dict(params)
        split_params["FromDate"] = from_d
        split_params["ToDate"] = to_d
        split_df = nba_fetch._api_call(split_params, timeout=nba_fetch.SPLIT_TIMEOUT)
        print(f"      {from_d} to {to_d}: {len(split_df)} lineups")
        split_dfs.append(split_df)

    combined = nba_fetch._combine_split_halves(split_dfs)
    print(f"    Combined: {len(combined)} unique lineups")
    return combined


def get_filename(team_id, opp=False, leverage=False, playoffs=False):
    name = str(team_id)
    if opp:
        name += "_vs"
    if playoffs:
        name += "_ps"
    if leverage:
        name += "_leverage"
    return f"{name}.csv"


def pull_block(team_ids, year, season_type, playoffs=False, leverage=False):
    output_dir = f"output/wnba_data/{year}"
    os.makedirs(output_dir, exist_ok=True)
    season_label = "playoffs" if playoffs else "regular season"
    tag = "leverage" if leverage else "non-leverage"
    failed_requests = []

    def fetch_and_write(team_id, opp, failure_label):
        side = "Opponent" if opp else "Team"
        filename = get_filename(team_id, opp=opp, leverage=leverage, playoffs=playoffs)
        filepath = os.path.join(output_dir, filename)
        try:
            df = lineuppull_full(team_id, year, season_type, opp=opp, leverage=leverage)
        except Exception as exc:
            print(f"  {failure_label} {team_id} ({side}): {exc}")
            return False

        df = df.reset_index(drop=True)
        df["team_id"] = team_id
        df["year"] = year
        df["season"] = str(year)
        df["season_type"] = season_type
        df["team_vs"] = opp
        df["playoffs"] = int(playoffs)
        if "Corner3FGM" not in df.columns:
            df["Corner3FGM"] = 0

        if len(df) > 2:
            df.to_csv(filepath, index=False)
            print(f"  Saved {filename} ({len(df)} rows)")
        else:
            print(f"  Skipped {filename} (only {len(df)} rows)")

        time.sleep(nba_fetch.SLEEP_BETWEEN)
        return True

    for opp in (False, True):
        side = "Opponent" if opp else "Team"
        print(f"\n--- WNBA {season_label} / {tag} / {side} ---")

        for team_id in team_ids:
            if not fetch_and_write(team_id, opp, "INITIAL FAILURE"):
                failed_requests.append((team_id, opp))

    if not failed_requests:
        return []

    print(
        f"\nRetrying {len(failed_requests)} failed {season_label} / {tag} "
        f"request(s) after {BLOCK_RETRY_DELAY}s..."
    )
    time.sleep(BLOCK_RETRY_DELAY)
    unresolved = []
    for team_id, opp in failed_requests:
        side = "Opponent" if opp else "Team"
        if fetch_and_write(team_id, opp, "RETRY FAILED"):
            print(f"  RECOVERED {team_id} ({side})")
        else:
            unresolved.append((team_id, side))

    return unresolved


def main():
    parser = argparse.ArgumentParser(description="Fetch WNBA PBPStats on-off data")
    parser.add_argument("--year", type=int, default=None, help="WNBA season year")
    parser.add_argument(
        "--team-id",
        action="append",
        default=[],
        help="Optional WNBA PBPStats team id to fetch. Repeat or comma-separate for smoke tests.",
    )
    parser.add_argument(
        "--include-playoffs",
        action="store_true",
        help="Also fetch playoff splits. Regular season only by default.",
    )
    args = parser.parse_args()
    year = args.year or current_wnba_season()
    team_ids = parse_team_ids(args.team_id)
    seasons = season_plan(args.include_playoffs)

    nba_fetch.WOWY_URL = WNBA_WOWY_URL
    print(f"=== WNBA PBPStats On-Off Fetch: {year} season ===\n")
    print(f"Teams: {', '.join(str(team_id) for team_id in team_ids)}")
    print(f"Season types: {', '.join(season_type for season_type, _ in seasons)}")
    print(f"Will make ~{len(team_ids) * 2 * 2 * len(seasons)} calls before date splits.\n")

    start = time.time()
    all_fails = []
    for season_type, playoffs in seasons:
        for leverage in (True, False):
            all_fails.extend(
                pull_block(team_ids, year, season_type=season_type, playoffs=playoffs, leverage=leverage)
            )

    data_dir = f"output/wnba_data/{year}"
    file_count = len([f for f in os.listdir(data_dir) if f.endswith(".csv")])
    print(f"\nDONE in {time.time() - start:.0f}s")
    print(f"  Files written: {file_count} in {data_dir}/")

    if all_fails:
        print(f"  Failures ({len(all_fails)}):")
        for team_id, side in all_fails:
            print(f"    Team {team_id} ({side})")
        sys.exit(1)


if __name__ == "__main__":
    main()
