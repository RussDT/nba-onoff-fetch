import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import fetch_wnba_onoff
from fetch_wnba_onoff import pull_block, season_plan


class SeasonPlanTests(unittest.TestCase):
    def test_default_plan_excludes_playoffs(self):
        self.assertEqual(season_plan(include_playoffs=False), [("Regular Season", False)])

    def test_playoffs_can_be_enabled_explicitly(self):
        self.assertEqual(
            season_plan(include_playoffs=True),
            [("Regular Season", False), ("Playoffs", True)],
        )


class PullBlockTests(unittest.TestCase):
    def test_initial_api_failure_gets_one_recovery_pass(self):
        rows = pd.DataFrame([{"EntityId": "A"}, {"EntityId": "B"}, {"EntityId": "C"}])
        team_id = fetch_wnba_onoff.WNBA_TEAM_IDS[0]

        with tempfile.TemporaryDirectory() as temp_dir:
            original_dir = Path.cwd()
            try:
                os.chdir(temp_dir)
                with (
                    patch.object(
                        fetch_wnba_onoff.nba_fetch,
                        "_api_call",
                        side_effect=[TimeoutError("upstream slow"), rows, rows],
                    ),
                    patch.object(fetch_wnba_onoff.time, "sleep"),
                ):
                    failures = pull_block(
                        [team_id],
                        2026,
                        season_type="Regular Season",
                        playoffs=False,
                        leverage=False,
                    )
            finally:
                os.chdir(original_dir)

            output_dir = Path(temp_dir) / "output" / "wnba_data" / "2026"
            self.assertEqual(failures, [])
            self.assertTrue((output_dir / f"{team_id}.csv").exists())
            self.assertTrue((output_dir / f"{team_id}_vs.csv").exists())


if __name__ == "__main__":
    unittest.main()
