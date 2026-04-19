# nba-onoff-fetch

Daily PBPStats on-off (wowy) data fetch via GitHub Actions.

## Current 2026 State

This repo now publishes explicit regular-season and playoff files for the 2025-26 postseason handoff.

- The regular-season on/off fetch path uses `SeasonType=Regular Season`.
- The playoff on/off fetch path uses `SeasonType=Playoffs` and writes `_ps` filenames into the same daily artifact.
- Regular-season season totals use `SeasonType=Regular Season`.
- Playoff season totals use `SeasonType=Playoffs`.
- The `season-totals-daily` workflow runs daily again so both RS and PS totals stay current.

## Important Warning

The daily on-off artifact now contains both regular-season and playoff files:

- Regular season:
  - `{team_id}.csv`
  - `{team_id}_vs.csv`
  - `{team_id}_leverage.csv`
  - `{team_id}_vs_leverage.csv`
- Playoffs:
  - `{team_id}_ps.csv`
  - `{team_id}_vs_ps.csv`
  - `{team_id}_ps_leverage.csv`
  - `{team_id}_vs_ps_leverage.csv`

Runs at 4:45 AM PST. Artifacts downloadable via:
```bash
gh run download --repo RussDT/nba-onoff-fetch -n "onoff-2026" --dir nba_rapm/on-off/
```
