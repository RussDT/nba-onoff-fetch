import unittest

import pandas as pd

from fetch_onoff import _combine_split_halves


class CombineSplitHalvesTests(unittest.TestCase):
    def test_preserves_team_id_and_groups_by_entity(self):
        first_half = pd.DataFrame(
            [
                {
                    "EntityId": "A",
                    "TeamId": 1610612739,
                    "Points": 20,
                    "FG2A": 10,
                    "FG2M": 5,
                    "FG3A": 2,
                    "FG3M": 1,
                    "FTA": 4,
                    "TsPct": 0.55,
                },
                {
                    "EntityId": "B",
                    "TeamId": 1610612744,
                    "Points": 15,
                    "FG2A": 8,
                    "FG2M": 4,
                    "FG3A": 4,
                    "FG3M": 1,
                    "FTA": 2,
                    "TsPct": 0.48,
                },
            ]
        )
        second_half = pd.DataFrame(
            [
                {
                    "EntityId": "A",
                    "TeamId": 1610612739,
                    "Points": 18,
                    "FG2A": 8,
                    "FG2M": 4,
                    "FG3A": 6,
                    "FG3M": 2,
                    "FTA": 5,
                    "TsPct": 0.58,
                }
            ]
        )

        result = _combine_split_halves([first_half, second_half]).set_index("EntityId")

        self.assertEqual(result.loc["A", "TeamId"], 1610612739)
        self.assertEqual(result.loc["A", "Points"], 38)
        self.assertEqual(result.loc["B", "TeamId"], 1610612744)
        self.assertEqual(result.loc["B", "Points"], 15)

    def test_recomputes_core_rates_from_summed_counts(self):
        split_rows = pd.DataFrame(
            [
                {
                    "EntityId": "A",
                    "TeamId": 1610612739,
                    "Points": 27,
                    "FG2A": 10,
                    "FG2M": 5,
                    "FG3A": 10,
                    "FG3M": 4,
                    "FTA": 6,
                    "2pt And 1 Free Throw Trips": 1,
                    "3pt And 1 Free Throw Trips": 0,
                    "OffRebounds": 3,
                    "OffFGReboundPct": 3 / 11,
                    "AtRimFGA": 8,
                    "AtRimFGM": 6,
                    "AtRimFrequency": 8 / 20,
                    "DefAtRimReboundPct": 1.0,
                    "ShotQualityAvg": 0.9,
                    "TotalPoss": 50,
                    "SecondsPlayed": 600,
                    "TsPct": 27 / (2 * (20 + 1 + 0.44 * 5)),
                    "EfgPct": (5 + 1.5 * 4) / 20,
                    "Fg2Pct": 0.5,
                    "Fg3Pct": 0.4,
                    "FG3APct": 0.5,
                    "ShortMidRangeFGA": 4,
                    "ShortMidRangeFGM": 2,
                    "ShortMidRangeFrequency": 0.2,
                    "ShortMidRangeAccuracy": 0.5,
                    "LongMidRangeFGA": 2,
                    "LongMidRangeFGM": 1,
                    "LongMidRangeFrequency": 0.1,
                    "LongMidRangeAccuracy": 0.5,
                    "Arc3FGA": 7,
                    "Arc3FGM": 3,
                    "Arc3Frequency": 0.35,
                    "Arc3Accuracy": 3 / 7,
                    "Corner3FGA": 3,
                    "Corner3FGM": 1,
                    "Corner3Frequency": 0.15,
                    "Corner3Accuracy": 1 / 3,
                },
                {
                    "EntityId": "A",
                    "TeamId": 1610612739,
                    "Points": 36,
                    "FG2A": 30,
                    "FG2M": 15,
                    "FG3A": 10,
                    "FG3M": 2,
                    "FTA": 10,
                    "2pt And 1 Free Throw Trips": 0,
                    "3pt And 1 Free Throw Trips": 0,
                    "OffRebounds": 4,
                    "OffFGReboundPct": 4 / 23,
                    "AtRimFGA": 10,
                    "AtRimFGM": 5,
                    "AtRimFrequency": 10 / 40,
                    "DefAtRimReboundPct": 0.2,
                    "ShotQualityAvg": 0.5,
                    "TotalPoss": 70,
                    "SecondsPlayed": 900,
                    "TsPct": 36 / (2 * (40 + 0.44 * 10)),
                    "EfgPct": (15 + 1.5 * 2) / 40,
                    "Fg2Pct": 0.5,
                    "Fg3Pct": 0.2,
                    "FG3APct": 0.25,
                    "ShortMidRangeFGA": 8,
                    "ShortMidRangeFGM": 4,
                    "ShortMidRangeFrequency": 0.2,
                    "ShortMidRangeAccuracy": 0.5,
                    "LongMidRangeFGA": 7,
                    "LongMidRangeFGM": 2,
                    "LongMidRangeFrequency": 0.175,
                    "LongMidRangeAccuracy": 2 / 7,
                    "Arc3FGA": 5,
                    "Arc3FGM": 1,
                    "Arc3Frequency": 0.125,
                    "Arc3Accuracy": 0.2,
                    "Corner3FGA": 5,
                    "Corner3FGM": 1,
                    "Corner3Frequency": 0.125,
                    "Corner3Accuracy": 0.2,
                },
            ]
        )

        result = _combine_split_halves([split_rows]).iloc[0]

        total_fga = 10 + 10 + 30 + 10
        total_tsa = total_fga + 1 + 0.44 * (6 - 1) + 0.44 * 10
        expected_ts = 63 / (2 * total_tsa)
        expected_off_fg_reb = 7 / ((10 + 10 - 5 - 4) + (30 + 10 - 15 - 2))
        expected_at_rim_freq = 18 / total_fga
        expected_shot_quality = ((0.9 * 20) + (0.5 * 40)) / 60
        expected_efg = (20 + 1.5 * 6) / 60
        expected_fg2 = 20 / 40
        expected_fg3 = 6 / 20
        expected_fg3a_pct = 20 / 60
        expected_short_mid_freq = 12 / 60
        expected_short_mid_acc = 6 / 12
        expected_long_mid_freq = 9 / 60
        expected_long_mid_acc = 3 / 9
        expected_arc3_freq = 12 / 60
        expected_arc3_acc = 4 / 12
        expected_corner3_freq = 8 / 60
        expected_corner3_acc = 2 / 8

        self.assertAlmostEqual(result["TsPct"], expected_ts)
        self.assertAlmostEqual(result["OffFGReboundPct"], expected_off_fg_reb)
        self.assertAlmostEqual(result["AtRimFrequency"], expected_at_rim_freq)
        self.assertAlmostEqual(result["ShotQualityAvg"], expected_shot_quality)
        self.assertAlmostEqual(result["EfgPct"], expected_efg)
        self.assertAlmostEqual(result["Fg2Pct"], expected_fg2)
        self.assertAlmostEqual(result["Fg3Pct"], expected_fg3)
        self.assertAlmostEqual(result["FG3APct"], expected_fg3a_pct)
        self.assertAlmostEqual(result["ShortMidRangeFrequency"], expected_short_mid_freq)
        self.assertAlmostEqual(result["ShortMidRangeAccuracy"], expected_short_mid_acc)
        self.assertAlmostEqual(result["LongMidRangeFrequency"], expected_long_mid_freq)
        self.assertAlmostEqual(result["LongMidRangeAccuracy"], expected_long_mid_acc)
        self.assertAlmostEqual(result["Arc3Frequency"], expected_arc3_freq)
        self.assertAlmostEqual(result["Arc3Accuracy"], expected_arc3_acc)
        self.assertAlmostEqual(result["Corner3Frequency"], expected_corner3_freq)
        self.assertAlmostEqual(result["Corner3Accuracy"], expected_corner3_acc)

    def test_noncritical_rates_fall_back_to_weighted_average(self):
        first_half = pd.DataFrame(
            [
                {
                    "EntityId": "A",
                    "TeamId": 1610612739,
                    "SecondsPlayed": 600,
                    "Pace": 100.0,
                }
            ]
        )
        second_half = pd.DataFrame(
            [
                {
                    "EntityId": "A",
                    "TeamId": 1610612739,
                    "SecondsPlayed": 900,
                    "Pace": 120.0,
                }
            ]
        )

        result = _combine_split_halves([first_half, second_half]).iloc[0]
        expected = ((100.0 * 600) + (120.0 * 900)) / 1500

        self.assertAlmostEqual(result["Pace"], expected)


if __name__ == "__main__":
    unittest.main()
