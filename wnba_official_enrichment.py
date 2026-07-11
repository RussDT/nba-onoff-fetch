"""Official WNBA minutes and roster-position enrichment for PBPStats rows."""

import math
import time

import pandas as pd

try:
    from curl_cffi import requests as curl_requests
except ImportError:  # pragma: no cover - exercised by the workflow dependency check
    curl_requests = None


class EnrichmentError(RuntimeError):
    """Raised when official data cannot safely enrich the full PBP artifact."""


POSITION_ALIASES = {
    "G": "G",
    "GUARD": "G",
    "F": "F",
    "FORWARD": "F",
    "C": "C",
    "CENTER": "C",
    "G-F": "G-F",
    "GUARD-FORWARD": "G-F",
    "F-G": "F-G",
    "FORWARD-GUARD": "F-G",
    "F-C": "F-C",
    "FORWARD-CENTER": "F-C",
    "C-F": "C-F",
    "CENTER-FORWARD": "C-F",
}

STATS_BASE_URL = "https://stats.wnba.com/stats"
REGULATION_MINUTES_PER_GAME = 400.0
OVERTIME_PLAYER_MINUTES = 50.0
MAX_PLAYER_OVERTIME_MINUTES_PER_GAME = 5.0
LEAGUE_MINUTES_TOLERANCE = 2.0


def stats_headers():
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.wnba.com",
        "Referer": "https://www.wnba.com/",
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
    }


def result_set_frame(payload, expected_name):
    result_sets = payload.get("resultSets") or [] if isinstance(payload, dict) else []
    result_set = next(
        (candidate for candidate in result_sets if candidate.get("name") == expected_name),
        None,
    )
    if result_set is None:
        raise EnrichmentError(f"official result set {expected_name} missing")

    try:
        return pd.DataFrame(result_set["rowSet"], columns=result_set["headers"])
    except (KeyError, TypeError, ValueError) as exc:
        raise EnrichmentError(f"official result set {expected_name} could not be parsed: {exc}") from exc


def fetch_official_frame(endpoint, params, result_set_name, attempts=5, timeout=40):
    if curl_requests is None:
        raise EnrichmentError("curl_cffi is required for official WNBA stats requests")

    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            response = curl_requests.get(
                f"{STATS_BASE_URL}/{endpoint}",
                params=params,
                headers=stats_headers(),
                timeout=timeout,
                impersonate="chrome124",
            )
            response.raise_for_status()
            frame = result_set_frame(response.json(), result_set_name)
            if frame.empty:
                raise EnrichmentError(f"official result set {result_set_name} was empty")
            return frame
        except Exception as exc:
            last_error = exc
            print(f"  Official WNBA {endpoint} attempt {attempt}/{attempts} failed: {exc}")
            if attempt < attempts:
                time.sleep(min(2 ** attempt, 8))

    raise EnrichmentError(f"official WNBA {endpoint} failed after {attempts} attempts: {last_error}")


def fetch_official_player_totals(year, season_type):
    params = {
        "LastNGames": "0",
        "MeasureType": "Base",
        "Month": "0",
        "OpponentTeamID": "0",
        "PaceAdjust": "N",
        "PerMode": "Totals",
        "Period": "0",
        "PlusMinus": "N",
        "Rank": "N",
        "Season": str(year),
        "SeasonType": season_type,
        "College": "",
        "Conference": "",
        "Country": "",
        "DateFrom": "",
        "DateTo": "",
        "Division": "",
        "DraftPick": "",
        "DraftYear": "",
        "GameScope": "",
        "GameSegment": "",
        "Height": "",
        "LeagueID": "10",
        "Location": "",
        "Outcome": "",
        "PORound": "",
        "PlayerExperience": "",
        "PlayerPosition": "",
        "SeasonSegment": "",
        "ShotClockRange": "",
        "StarterBench": "",
        "TeamID": "",
        "TwoWay": "",
        "VsConference": "",
        "VsDivision": "",
        "Weight": "",
    }
    return fetch_official_frame(
        "leaguedashplayerstats",
        params,
        "LeagueDashPlayerStats",
    )


def fetch_official_positions(year):
    params = {
        "Active": "",
        "AllStar": "",
        "College": "",
        "Country": "",
        "DraftPick": "",
        "DraftYear": "",
        "Height": "",
        "PlayerPosition": "",
        "Historical": "",
        "LeagueID": "10",
        "Season": str(year),
        "TeamID": "",
        "Weight": "",
    }
    return fetch_official_frame("playerindex", params, "PlayerIndex")


def fetch_official_team_totals(year, season_type):
    params = {
        "Conference": "",
        "DateFrom": "",
        "DateTo": "",
        "Division": "",
        "GameScope": "",
        "GameSegment": "",
        "LastNGames": "0",
        "LeagueID": "10",
        "Location": "",
        "MeasureType": "Base",
        "Month": "0",
        "OpponentTeamID": "0",
        "Outcome": "",
        "PORound": "",
        "PaceAdjust": "N",
        "PerMode": "Totals",
        "Period": "0",
        "PlayerExperience": "",
        "PlayerPosition": "",
        "PlusMinus": "N",
        "Rank": "N",
        "Season": str(year),
        "SeasonSegment": "",
        "SeasonType": season_type,
        "ShotClockRange": "",
        "StarterBench": "",
        "TeamID": "",
        "VsConference": "",
        "VsDivision": "",
    }
    return fetch_official_frame("leaguedashteamstats", params, "LeagueDashTeamStats")


def _id_key(value):
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def normalize_position(value):
    if value is None or pd.isna(value):
        return ""
    normalized = str(value).strip().upper().replace(" / ", "-").replace("/", "-")
    normalized = "-".join(part.strip() for part in normalized.split("-") if part.strip())
    return POSITION_ALIASES.get(normalized, "")


def validate_publish_row_count(new_count, previous_count, minimum_retention=0.9):
    if previous_count <= 0:
        return
    minimum_count = math.ceil(previous_count * minimum_retention)
    if new_count < minimum_count:
        raise EnrichmentError(
            f"WNBA artifact row count dropped from {previous_count} to {new_count} "
            f"(minimum {minimum_count})"
        )


def _unique_lookup(frame, id_column, value_column, label, transform=lambda value: value):
    missing_columns = {id_column, value_column} - set(frame.columns)
    if missing_columns:
        raise EnrichmentError(f"{label} response missing columns: {sorted(missing_columns)}")

    lookup = {}
    for row in frame[[id_column, value_column]].to_dict("records"):
        player_id = _id_key(row[id_column])
        if not player_id:
            continue
        value = transform(row[value_column])
        if player_id in lookup and lookup[player_id] != value:
            raise EnrichmentError(f"conflicting {label} rows for player {player_id}")
        lookup[player_id] = value
    return lookup


def _official_minute(value):
    try:
        minutes = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(minutes) or minutes < 0:
        return None
    return minutes


def _official_games(value):
    try:
        games = int(value)
    except (TypeError, ValueError):
        return None
    if games < 0:
        return None
    return games


def validate_league_minutes(official_totals, official_team_totals):
    """Reconcile official player minutes to completed games plus overtime."""
    if "MIN" not in official_totals or "GP" not in official_team_totals:
        raise EnrichmentError("official league-minute reconciliation columns are missing")

    player_minutes = pd.to_numeric(official_totals["MIN"], errors="coerce")
    team_games = pd.to_numeric(official_team_totals["GP"], errors="coerce")
    if player_minutes.isna().any() or not player_minutes.map(math.isfinite).all():
        raise EnrichmentError("official player minutes are invalid for league reconciliation")
    if team_games.isna().any() or not team_games.map(math.isfinite).all() or (team_games < 0).any():
        raise EnrichmentError("official team games are invalid for league reconciliation")

    game_sides = float(team_games.sum())
    completed_games = int(round(game_sides / 2.0))
    if abs(game_sides - completed_games * 2.0) > 0.01:
        raise EnrichmentError("official team games do not reconcile to completed games")

    actual_minutes = float(player_minutes.sum())
    regulation_minutes = completed_games * REGULATION_MINUTES_PER_GAME
    overtime_residual = actual_minutes - regulation_minutes
    overtime_periods = max(0, int(round(overtime_residual / OVERTIME_PLAYER_MINUTES)))
    expected_minutes = regulation_minutes + overtime_periods * OVERTIME_PLAYER_MINUTES
    if overtime_residual < -LEAGUE_MINUTES_TOLERANCE or abs(actual_minutes - expected_minutes) > LEAGUE_MINUTES_TOLERANCE:
        raise EnrichmentError(
            "official player minutes do not reconcile to completed games and overtime: "
            f"actual={actual_minutes:.2f} expected={expected_minutes:.2f}"
        )

    return {
        "completed_games": completed_games,
        "overtime_periods": overtime_periods,
        "official_player_minutes": actual_minutes,
        "expected_player_minutes": expected_minutes,
    }


def enrich_player_totals(pbp_rows, official_totals, official_positions):
    """Return PBP rows with official time and broad roster position fields."""
    if "EntityId" not in pbp_rows.columns:
        raise EnrichmentError("PBP artifact missing EntityId")
    if pbp_rows.empty:
        return pbp_rows.copy()

    minutes_by_id = _unique_lookup(
        official_totals,
        "PLAYER_ID",
        "MIN",
        "official minutes",
        _official_minute,
    )
    games_by_id = _unique_lookup(
        official_totals,
        "PLAYER_ID",
        "GP",
        "official games",
        _official_games,
    )
    positions_by_id = _unique_lookup(
        official_positions,
        "PERSON_ID",
        "POSITION",
        "official positions",
        normalize_position,
    )

    player_ids = pbp_rows["EntityId"].map(_id_key)
    missing_minutes = sorted(player_id for player_id in player_ids.unique() if not player_id or player_id not in minutes_by_id)
    if missing_minutes:
        raise EnrichmentError(f"official minutes missing for player IDs: {missing_minutes}")

    invalid_minutes = sorted(player_id for player_id in player_ids.unique() if minutes_by_id[player_id] is None)
    if invalid_minutes:
        raise EnrichmentError(f"invalid official minutes for player IDs: {invalid_minutes}")

    invalid_games = sorted(
        player_id for player_id in player_ids.unique() if games_by_id.get(player_id) is None
    )
    if invalid_games:
        raise EnrichmentError(f"invalid official games for player IDs: {invalid_games}")

    implausible_minutes = sorted(
        player_id
        for player_id in player_ids.unique()
        if minutes_by_id[player_id]
        > games_by_id[player_id] * (40.0 + MAX_PLAYER_OVERTIME_MINUTES_PER_GAME)
    )
    if implausible_minutes:
        raise EnrichmentError(
            "official minutes exceed the plausible maximum for player IDs: "
            f"{implausible_minutes}"
        )

    missing_positions = sorted(
        player_id for player_id in player_ids.unique() if not player_id or not positions_by_id.get(player_id)
    )
    if missing_positions:
        raise EnrichmentError(f"official positions missing or invalid for player IDs: {missing_positions}")

    output = pbp_rows.copy()
    output["GamesPlayed"] = player_ids.map(games_by_id).astype(int)
    output["Minutes"] = player_ids.map(minutes_by_id).astype(float)
    output["SecondsPlayed"] = output["Minutes"] * 60.0
    output["mp"] = output["Minutes"]
    output["MPG"] = output["Minutes"].div(output["GamesPlayed"]).where(output["GamesPlayed"] > 0, 0.0)
    output["Pos"] = player_ids.map(positions_by_id)
    output["pos"] = output["Pos"]
    output["Pos2"] = output["Pos"]

    if not output["Minutes"].map(math.isfinite).all() or (output["Minutes"] < 0).any():
        raise EnrichmentError("invalid official minutes remained after enrichment")
    if output["Pos"].fillna("").str.strip().eq("").any():
        raise EnrichmentError("blank official positions remained after enrichment")

    return output
