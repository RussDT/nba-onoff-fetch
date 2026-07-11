import unittest

import pandas as pd

from wnba_official_enrichment import (
    EnrichmentError,
    enrich_player_totals,
    result_set_frame,
    validate_league_minutes,
    validate_publish_row_count,
)


class EnrichPlayerTotalsTests(unittest.TestCase):
    def setUp(self):
        self.pbp = pd.DataFrame(
            [
                {
                    "EntityId": 101,
                    "Name": "Alpha Guard",
                    "Minutes": -12.0,
                    "SecondsPlayed": -720.0,
                    "GamesPlayed": 8,
                    "Points": 99,
                    "OffPoss": 240,
                },
                {
                    "EntityId": "202",
                    "Name": "Beta Big",
                    "Minutes": 44.0,
                    "SecondsPlayed": 2640.0,
                    "GamesPlayed": 7,
                    "Points": 55,
                    "OffPoss": 180,
                },
            ]
        )
        self.official_totals = pd.DataFrame(
            [
                {"PLAYER_ID": "101", "GP": 8, "MIN": 231.5},
                {"PLAYER_ID": 202, "GP": 7, "MIN": 178.25},
            ]
        )
        self.positions = pd.DataFrame(
            [
                {"PERSON_ID": 101, "POSITION": "Guard-Forward"},
                {"PERSON_ID": "202", "POSITION": "C"},
            ]
        )

    def test_overrides_time_and_position_by_player_id_without_touching_advanced_stats(self):
        result = enrich_player_totals(self.pbp, self.official_totals, self.positions)
        rows = result.set_index(result["EntityId"].astype(str))

        self.assertEqual(rows.loc["101", "Minutes"], 231.5)
        self.assertEqual(rows.loc["101", "SecondsPlayed"], 13890.0)
        self.assertEqual(rows.loc["101", "mp"], 231.5)
        self.assertAlmostEqual(rows.loc["101", "MPG"], 231.5 / 8)
        self.assertEqual(rows.loc["101", "Pos"], "G-F")
        self.assertEqual(rows.loc["101", "Pos2"], "G-F")
        self.assertEqual(rows.loc["101", "Points"], 99)
        self.assertEqual(rows.loc["101", "OffPoss"], 240)
        self.assertEqual(rows.loc["202", "Minutes"], 178.25)
        self.assertEqual(rows.loc["202", "Pos"], "C")

    def test_fails_closed_when_an_official_total_or_position_is_missing(self):
        with self.assertRaisesRegex(EnrichmentError, "official minutes missing"):
            enrich_player_totals(
                self.pbp,
                self.official_totals.iloc[:1],
                self.positions,
            )

        with self.assertRaisesRegex(EnrichmentError, "official positions missing"):
            enrich_player_totals(
                self.pbp,
                self.official_totals,
                self.positions.iloc[:1],
            )

    def test_rejects_negative_or_nonfinite_official_minutes(self):
        broken = self.official_totals.copy()
        broken.loc[0, "MIN"] = -1
        with self.assertRaisesRegex(EnrichmentError, "invalid official minutes"):
            enrich_player_totals(self.pbp, broken, self.positions)

        broken.loc[0, "MIN"] = float("nan")
        with self.assertRaisesRegex(EnrichmentError, "invalid official minutes"):
            enrich_player_totals(self.pbp, broken, self.positions)

    def test_rejects_minutes_above_regulation_plus_explicit_overtime_allowance(self):
        broken = self.official_totals.copy()
        broken.loc[0, "MIN"] = 361.0
        with self.assertRaisesRegex(EnrichmentError, "plausible maximum"):
            enrich_player_totals(self.pbp, broken, self.positions)

    def test_reconciles_league_minutes_to_completed_games_and_overtime(self):
        player_totals = pd.DataFrame([{"MIN": 225.0}, {"MIN": 225.0}])
        team_totals = pd.DataFrame([{"TEAM_ID": 1, "GP": 1}, {"TEAM_ID": 2, "GP": 1}])

        result = validate_league_minutes(player_totals, team_totals)

        self.assertEqual(result["completed_games"], 1)
        self.assertEqual(result["overtime_periods"], 1)
        self.assertAlmostEqual(result["official_player_minutes"], 450.0)

        broken = pd.DataFrame([{"MIN": 430.0}])
        with self.assertRaisesRegex(EnrichmentError, "do not reconcile"):
            validate_league_minutes(broken, team_totals)

    def test_parses_only_the_expected_official_result_set(self):
        payload = {
            "resultSets": [
                {"name": "Other", "headers": ["IGNORED"], "rowSet": [[1]]},
                {
                    "name": "LeagueDashPlayerStats",
                    "headers": ["PLAYER_ID", "MIN"],
                    "rowSet": [[101, 231.5]],
                },
            ]
        }

        result = result_set_frame(payload, "LeagueDashPlayerStats")

        self.assertEqual(result.to_dict("records"), [{"PLAYER_ID": 101, "MIN": 231.5}])

    def test_rejects_missing_or_malformed_official_result_sets(self):
        with self.assertRaisesRegex(EnrichmentError, "result set PlayerIndex missing"):
            result_set_frame({"resultSets": []}, "PlayerIndex")

        malformed = {
            "resultSets": [
                {"name": "PlayerIndex", "headers": ["PERSON_ID", "POSITION"], "rowSet": [[101]]}
            ]
        }
        with self.assertRaisesRegex(EnrichmentError, "could not be parsed"):
            result_set_frame(malformed, "PlayerIndex")

    def test_rejects_a_large_drop_from_the_last_published_artifact(self):
        validate_publish_row_count(new_count=192, previous_count=213)

        with self.assertRaisesRegex(EnrichmentError, "row count dropped"):
            validate_publish_row_count(new_count=190, previous_count=213)


if __name__ == "__main__":
    unittest.main()
