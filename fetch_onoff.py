"""
PBPStats On-Off Fetch
=====================
Fetches WOWY (With Or Without You) data from PBPStats API for all NBA teams.

Two blocks of calls:
  Block 1 (leverage): Leverage='Medium,High,VeryHigh' -> {team_id}_leverage.csv, {team_id}_vs_leverage.csv
  Block 2 (non-leverage): No Leverage param -> {team_id}.csv, {team_id}_vs.csv

Each block: 30 teams x 2 (Team + Opponent) = 60 calls. Total: 120 calls.
Teams that hit the 500-row API cap get automatic date-split re-fetches.

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
from datetime import datetime

import numpy as np
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

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WOWY_URL = "https://api.pbpstats.com/get-wowy-stats/nba"
INDEX_MASTER_URL = (
    "https://raw.githubusercontent.com/gabriel1200/player_sheets/master/lineups/index_master.csv"
)
REFERENCE_YEAR = 2025  # Always use this year's teams for team list
ROW_CAP = 500  # PBPStats API row limit

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
REQUEST_TIMEOUT = (10, 60)  # (connect, read)
SPLIT_TIMEOUT = (10, 90)  # longer timeout for date-filtered queries


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


def _api_call(params, timeout=REQUEST_TIMEOUT):
    """Single PBPStats API call with retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                WOWY_URL, params=params, headers=HEADERS, timeout=timeout
            )
            resp.raise_for_status()
            data = resp.json()["multi_row_table_data"]
            return pd.DataFrame(data, index=[0] * len(data))
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                raise


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

    return _api_call(params)


FIRST_VALUE_NUMERIC_COLUMNS = {"TeamId"}
RATE_COLUMN_EXTRAS = {
    "Pace",
    "Usage",
    "SecondsPerPossOff",
    "SecondsPerPossDef",
    "SecondsExcludingORebsPerPossOff",
    "SecondsExcludingORebsPerPossDef",
}


def _is_derived_numeric_column(column):
    return column in RATE_COLUMN_EXTRAS or any(
        token in column for token in ("Pct", "Frequency", "Accuracy", "Avg")
    )


def _numeric_series(df, column):
    if column not in df.columns:
        return pd.Series(0.0, index=df.index, dtype=float)
    return pd.to_numeric(df[column], errors="coerce").fillna(0.0)


def _safe_divide(numerator, denominator):
    numerator = pd.to_numeric(numerator, errors="coerce")
    denominator = pd.to_numeric(denominator, errors="coerce").replace(0, np.nan)
    return (numerator / denominator).replace([np.inf, -np.inf], np.nan)


def _weighted_average_by_group(df, entity_col, value_col, weight):
    if value_col not in df.columns:
        return pd.Series(dtype=float)

    values = pd.to_numeric(df[value_col], errors="coerce")
    weights = pd.to_numeric(weight, errors="coerce").fillna(0.0).clip(lower=0.0)
    valid = values.notna() & (weights > 0)
    if not valid.any():
        return pd.Series(dtype=float)

    tmp = pd.DataFrame(
        {
            entity_col: df.loc[valid, entity_col],
            "_weighted_value": values.loc[valid] * weights.loc[valid],
            "_weight": weights.loc[valid],
        }
    )
    grouped = tmp.groupby(entity_col, as_index=True)[["_weighted_value", "_weight"]].sum()
    return grouped["_weighted_value"] / grouped["_weight"]


def _first_non_null_by_group(df, entity_col, column):
    if column not in df.columns:
        return pd.Series(dtype=float)

    values = pd.to_numeric(df[column], errors="coerce")
    tmp = pd.DataFrame({entity_col: df[entity_col], "_value": values})
    tmp = tmp[tmp["_value"].notna()]
    if tmp.empty:
        return pd.Series(dtype=float)
    return tmp.groupby(entity_col, as_index=True)["_value"].first()


def _assign_weighted_with_fallback(result, combined, entity_col, column, weight):
    if column not in result.columns or column not in combined.columns:
        return False

    assigned = False
    assigned_mask = pd.Series(False, index=result.index)

    weighted = _weighted_average_by_group(combined, entity_col, column, weight)
    if not weighted.empty:
        mapped = result[entity_col].map(weighted)
        valid = mapped.notna()
        if valid.any():
            result.loc[valid, column] = mapped.loc[valid]
            assigned_mask.loc[valid] = True
            assigned = True

    fallback = _first_non_null_by_group(combined, entity_col, column)
    if not fallback.empty:
        mapped = result[entity_col].map(fallback)
        valid = mapped.notna() & ~assigned_mask
        if valid.any():
            result.loc[valid, column] = mapped.loc[valid]
            assigned = True

    return assigned


def _recompute_split_rates(result, combined, entity_col):
    """Rebuild the lineup rate fields that our downstream upload depends on."""
    handled = set()

    fga = _numeric_series(result, "FG2A") + _numeric_series(result, "FG3A")
    fgm = _numeric_series(result, "FG2M") + _numeric_series(result, "FG3M")
    fg_misses = (fga - fgm).clip(lower=0.0)
    fta = _numeric_series(result, "FTA")
    and1_two = _numeric_series(result, "2pt And 1 Free Throw Trips")
    and1_three = _numeric_series(result, "3pt And 1 Free Throw Trips")
    non_and1_fta = (fta - and1_two - and1_three).clip(lower=0.0)
    tsa = fga + and1_two + 1.5 * and1_three + 0.44 * non_and1_fta

    def set_ratio(column, numerator, denominator):
        if column not in result.columns:
            return
        result[column] = _safe_divide(numerator, denominator)
        handled.add(column)

    if "Minutes" in result.columns and "SecondsPlayed" in result.columns:
        result["Minutes"] = _numeric_series(result, "SecondsPlayed") / 60.0

    set_ratio("TsPct", _numeric_series(result, "Points"), 2.0 * tsa)
    set_ratio("OffFGReboundPct", _numeric_series(result, "OffRebounds"), fg_misses)
    set_ratio("EfgPct", _numeric_series(result, "FG2M") + 1.5 * _numeric_series(result, "FG3M"), fga)
    set_ratio("Fg2Pct", _numeric_series(result, "FG2M"), _numeric_series(result, "FG2A"))
    set_ratio("Fg3Pct", _numeric_series(result, "FG3M"), _numeric_series(result, "FG3A"))
    set_ratio("FG3APct", _numeric_series(result, "FG3A"), fga)

    set_ratio("AtRimFrequency", _numeric_series(result, "AtRimFGA"), fga)
    set_ratio("AtRimAccuracy", _numeric_series(result, "AtRimFGM"), _numeric_series(result, "AtRimFGA"))
    set_ratio(
        "ShortMidRangeFrequency",
        _numeric_series(result, "ShortMidRangeFGA"),
        fga,
    )
    set_ratio(
        "ShortMidRangeAccuracy",
        _numeric_series(result, "ShortMidRangeFGM"),
        _numeric_series(result, "ShortMidRangeFGA"),
    )
    set_ratio(
        "LongMidRangeFrequency",
        _numeric_series(result, "LongMidRangeFGA"),
        fga,
    )
    set_ratio(
        "LongMidRangeAccuracy",
        _numeric_series(result, "LongMidRangeFGM"),
        _numeric_series(result, "LongMidRangeFGA"),
    )
    set_ratio("Arc3Frequency", _numeric_series(result, "Arc3FGA"), fga)
    set_ratio("Arc3Accuracy", _numeric_series(result, "Arc3FGM"), _numeric_series(result, "Arc3FGA"))
    set_ratio("Corner3Frequency", _numeric_series(result, "Corner3FGA"), fga)
    set_ratio("Corner3Accuracy", _numeric_series(result, "Corner3FGM"), _numeric_series(result, "Corner3FGA"))

    shot_weight = _numeric_series(combined, "FG2A") + _numeric_series(combined, "FG3A")
    if _assign_weighted_with_fallback(
        result, combined, entity_col, "ShotQualityAvg", shot_weight
    ):
        handled.add("ShotQualityAvg")

    default_weight = _numeric_series(combined, "SecondsPlayed")
    if default_weight.eq(0).all():
        default_weight = pd.Series(1.0, index=combined.index, dtype=float)

    for column in result.columns:
        if (
            column in handled
            or not _is_derived_numeric_column(column)
            or column not in combined.columns
        ):
            continue
        _assign_weighted_with_fallback(result, combined, entity_col, column, default_weight)

    return result


def _combine_split_halves(dfs):
    """
    Combine date-split DataFrames without corrupting rate columns.

    Additive box-score counts are summed by `EntityId`. The lineup rate fields we
    use downstream are then rebuilt from the summed counts, and any remaining
    rate-like numeric columns fall back to a seconds-played weighted average so
    they no longer get summed into nonsense.
    """
    combined = pd.concat(dfs, ignore_index=True)
    entity_col = "EntityId"
    original_columns = combined.columns.tolist()
    numeric_cols = combined.select_dtypes(include=[np.number]).columns.tolist()
    sum_numeric_cols = [
        c
        for c in numeric_cols
        if c not in FIRST_VALUE_NUMERIC_COLUMNS and not _is_derived_numeric_column(c)
    ]

    agg_dict = {}
    for column in combined.columns:
        if column == entity_col:
            continue
        agg_dict[column] = "sum" if column in sum_numeric_cols else "first"

    result = combined.groupby(entity_col, as_index=False).agg(agg_dict)
    result = _recompute_split_rates(result, combined, entity_col)
    return result[original_columns]


def lineuppull_full(team_id, year, season, opp=False, leverage=False):
    """
    Fetch WOWY stats, handling the 500-row API cap.

    If the initial pull returns exactly 500 rows, re-fetches using date splits
    (first half / second half of season) and combines results.
    """
    df = lineuppull(team_id, season, opp=opp, leverage=leverage)

    if len(df) < ROW_CAP:
        return df

    # Hit the cap — re-fetch with date splits
    side = "Opponent" if opp else "Team"
    tag = "leverage" if leverage else "non-leverage"
    print(f"    {team_id} ({side}/{tag}): hit {ROW_CAP}-row cap, splitting by date...")

    splits = [
        (f"{year - 1}-10-01", f"{year}-01-15"),
        (f"{year}-01-16", f"{year}-06-30"),
    ]

    split_dfs = []
    for from_d, to_d in splits:
        time.sleep(SLEEP_BETWEEN)
        params = {
            "TeamId": team_id,
            "Season": season,
            "SeasonType": "Regular Season",
            "Type": "Opponent" if opp else "Team",
            "FromDate": from_d,
            "ToDate": to_d,
        }
        if leverage:
            params["Leverage"] = "Medium,High,VeryHigh"

        split_df = _api_call(params, timeout=SPLIT_TIMEOUT)
        print(f"      {from_d} to {to_d}: {len(split_df)} lineups")
        split_dfs.append(split_df)

    combined = _combine_split_halves(split_dfs)
    print(f"    Combined: {len(combined)} unique lineups (was capped at {ROW_CAP})")
    return combined


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

            try:
                df = lineuppull_full(
                    team_id, year, season, opp=opp, leverage=leverage
                )
            except Exception as e:
                print(f"  FAILED {team_id} ({side}): {e}")
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
    parser.add_argument("--year", type=int, default=None, help="Season year (auto-detects if omitted)")
    args = parser.parse_args()

    year = args.year or current_nba_season()
    print(f"=== PBPStats On-Off Fetch: {year - 1}-{str(year)[-2:]} season ===\n")

    team_ids = fetch_team_ids()
    total_calls = len(team_ids) * 2 * 2  # teams * sides * blocks
    print(f"Will make ~{total_calls}+ API calls (more if teams hit {ROW_CAP}-row cap)\n")

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
