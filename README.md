# nba-onoff-fetch

Daily PBPStats on-off (wowy) data fetch via GitHub Actions.

## Current 2026 State

This repo is in a temporary late-season transition state for the end of the 2025-26 regular season.

- The regular-season on/off fetch path is currently using `SeasonType=Regular Season`.
- The regular-season season-totals fetch path is also currently using `SeasonType=All`.
- This was done because the upstream PBPStats `Regular Season` totals stopped reflecting the final completed 2025-26 regular season correctly for some downstream use cases.
- The `season-totals-daily` workflow schedule is intentionally disabled for now, so season totals only change on manual runs.

## Important Warning

This is not the intended long-term behavior once playoff data needs to flow.

- The on/off artifact no longer uses `SeasonType=All`, but the regular-season season-totals path still does.
- `SeasonType=All` will still mix playoff data into the season-totals regular-season path once playoff games exist.
- Before playoff rollout, this repo should be revisited so playoff refreshes are explicit and regular-season artifacts can stay frozen or otherwise be split cleanly.
- Current expectation: regular-season snapshots are being stabilized now, and playoff-specific pull behavior will be turned on in a few days.

Runs at 4:45 AM PST. Artifacts downloadable via:
```bash
gh run download --repo RussDT/nba-onoff-fetch -n "onoff-2026" --dir nba_rapm/on-off/
```
