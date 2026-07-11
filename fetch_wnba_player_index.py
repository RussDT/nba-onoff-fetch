"""Fetch WNBA player-team-season index rows from PBPStats player totals."""

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from fetch_season_totals import HEADERS
from wnba_official_enrichment import (
    enrich_player_totals,
    fetch_official_player_totals,
    fetch_official_positions,
    validate_publish_row_count,
)

DATA_DIR = Path(__file__).resolve().parent / "data"
WNBA_TOTALS_URL = "https://api.pbpstats.com/get-totals/wnba"

TEAM_ABBR_BY_ID = {
    1611661313: "NYL",
    1611661317: "PHX",
    1611661319: "LVA",
    1611661320: "LAS",
    1611661321: "DAL",
    1611661322: "WAS",
    1611661323: "CON",
    1611661324: "MIN",
    1611661325: "IND",
    1611661327: "POR",
    1611661328: "SEA",
    1611661329: "CHI",
    1611661330: "ATL",
    1611661331: "GSV",
    1611661332: "TOR",
}


def current_wnba_season():
    return datetime.utcnow().year


def _safe_int(value):
    try:
        if pd.isna(value):
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _team_abbreviation(row, team_id):
    for key in ("TeamAbbreviation", "Team", "TeamShortName", "TeamName"):
        value = row.get(key)
        if value is not None and not pd.isna(value):
            text = str(value).strip()
            if text and text.upper() not in {"TOT", "TOTAL"}:
                return text
    return TEAM_ABBR_BY_ID.get(team_id)


def fetch_player_totals(year, season_type="Regular Season"):
    params = {
        "Season": str(year),
        "SeasonType": season_type,
        "Type": "Player",
        "StarterState": "All",
        "StartType": "All",
    }

    for attempt in range(1, 16):
        try:
            time.sleep(2)
            response = requests.get(WNBA_TOTALS_URL, params=params, headers=HEADERS, timeout=(10, 30))
            response.raise_for_status()
            payload = response.json()
            rows = payload.get("multi_row_table_data") or payload.get("results") or []
            print(f"  Fetched {len(rows)} rows: year={year} type={season_type}")
            return pd.DataFrame(rows)
        except Exception as exc:
            print(f"  Attempt {attempt}/15 failed: {exc}")
            if attempt == 15:
                return pd.DataFrame()
            time.sleep(2)


def player_totals_filename(year, playoffs):
    suffix = "ps" if playoffs else ""
    return DATA_DIR / f"{year}{suffix}_pbp.csv"


def write_player_totals_csv(raw, year, playoffs):
    path = player_totals_filename(year, playoffs)
    if raw.empty:
        print(f"  No player totals rows for {path.name}; leaving artifact unchanged")
        return

    output = raw.copy()
    output["year"] = f"{year}ps" if playoffs else int(year)
    if path.exists():
        previous_count = len(pd.read_csv(path, usecols=["EntityId"]))
        validate_publish_row_count(len(output), previous_count)

    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    output.to_csv(temp_path, index=False)
    content_hash = hashlib.sha256(temp_path.read_bytes()).hexdigest()
    temp_path.replace(path)

    metadata_path = path.with_suffix(".meta.json")
    metadata_temp_path = metadata_path.with_suffix(f"{metadata_path.suffix}.tmp")
    metadata = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "season": int(year),
        "season_type": "Playoffs" if playoffs else "Regular Season",
        "row_count": len(output),
        "sha256": content_hash,
        "advanced_source": "api.pbpstats.com/get-totals/wnba",
        "minutes_source": "stats.wnba.com/stats/leaguedashplayerstats",
        "positions_source": "stats.wnba.com/stats/playerindex",
    }
    metadata_temp_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    metadata_temp_path.replace(metadata_path)
    print(f"  Saved {len(output)} rows to {path.name}")


def enrich_current_totals(raw, year, season_type, official_positions):
    if raw.empty:
        return raw

    official_totals = fetch_official_player_totals(year, season_type)
    enriched = enrich_player_totals(raw, official_totals, official_positions)
    print(
        "  Enriched PBP rows with official minutes and positions: "
        f"rows={len(enriched)} minutes={enriched['Minutes'].sum():.2f} "
        f"positions={enriched['Pos'].notna().sum()}"
    )
    return enriched


def build_index_rows_from_totals(raw, year, playoffs):
    if raw.empty:
        return pd.DataFrame(columns=["player", "url", "year", "team", "bref_id", "nba_id", "team_id", "playoffs"])

    records = []
    for row in raw.to_dict("records"):
        nba_id = _safe_int(row.get("EntityId"))
        team_id = _safe_int(row.get("TeamId"))
        player_name = row.get("Name")
        if nba_id is None or team_id is None or player_name is None or pd.isna(player_name):
            continue

        team = _team_abbreviation(row, team_id)
        if not team:
            continue

        records.append(
            {
                "player": str(player_name).strip(),
                "url": None,
                "year": int(year),
                "team": str(team).strip(),
                "bref_id": None,
                "nba_id": int(nba_id),
                "team_id": int(team_id),
                "playoffs": int(playoffs),
            }
        )

    return pd.DataFrame.from_records(records)


def update_index_csv(csv_path, new_rows, year):
    if csv_path.exists():
        existing = pd.read_csv(csv_path)
        print(f"  Loaded {len(existing)} rows from {csv_path.name}")
        existing = existing[existing["year"] != year]
    else:
        existing = pd.DataFrame(columns=new_rows.columns)
        print(f"  {csv_path.name} not found, creating new")

    combined = pd.concat([existing, new_rows], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["nba_id", "team_id", "year", "playoffs"],
        keep="last",
    )
    combined = combined.sort_values(["year", "team_id", "player", "playoffs"]).reset_index(drop=True)
    combined.to_csv(csv_path, index=False)
    print(f"  Saved {len(combined)} rows to {csv_path.name}")


def main():
    parser = argparse.ArgumentParser(description="Fetch WNBA historical player index")
    parser.add_argument("--year", type=int, default=None, help="Single WNBA season year")
    parser.add_argument("--start-year", type=int, default=None, help="First WNBA season year")
    parser.add_argument("--end-year", type=int, default=None, help="Last WNBA season year")
    args = parser.parse_args()

    if args.year is not None and (args.start_year is not None or args.end_year is not None):
        raise SystemExit("Use either --year or --start-year/--end-year, not both")

    if args.year is not None:
        years = [args.year]
    elif args.start_year is not None or args.end_year is not None:
        start_year = args.start_year or args.end_year
        end_year = args.end_year or args.start_year
        years = list(range(int(start_year), int(end_year) + 1))
    else:
        years = [current_wnba_season()]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = DATA_DIR / "wnba_player_index.csv"
    print(f"=== fetch_wnba_player_index.py (years={years[0]}-{years[-1]}) ===")

    for year in years:
        print(f"\n--- {year} ---")
        official_positions = fetch_official_positions(year)

        regular_totals = fetch_player_totals(year, season_type="Regular Season")
        regular_totals = enrich_current_totals(
            regular_totals,
            year,
            "Regular Season",
            official_positions,
        )
        write_player_totals_csv(regular_totals, year, playoffs=0)

        playoff_totals = fetch_player_totals(year, season_type="Playoffs")
        playoff_totals = enrich_current_totals(
            playoff_totals,
            year,
            "Playoffs",
            official_positions,
        )
        write_player_totals_csv(playoff_totals, year, playoffs=1)

        frames = [
            build_index_rows_from_totals(regular_totals, year, playoffs=0),
            build_index_rows_from_totals(playoff_totals, year, playoffs=1),
        ]
        year_rows = pd.concat(frames, ignore_index=True)
        if year_rows.empty:
            print(f"  No player index rows for {year}")
            continue
        update_index_csv(csv_path, year_rows, year)

    print("\nDone.")


if __name__ == "__main__":
    main()
