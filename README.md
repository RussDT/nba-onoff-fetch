# nba-onoff-fetch

Daily PBPStats WOWY (With Or Without You) on-off data fetcher for the NBA pipeline.

## What it does

Fetches player on-off court statistics from the PBPStats API for all 30 NBA teams. Makes 120 API calls total:

- **Block 1 (leverage)**: 30 teams x 2 sides (Team/Opponent) = 60 calls with `Leverage=Medium,High,VeryHigh`
- **Block 2 (non-leverage)**: 30 teams x 2 sides = 60 calls with all possessions

## Schedule

Runs daily at **4:45 AM PST** (12:45 UTC) via GitHub Actions. Can also be triggered manually.

## Output

Artifact `onoff-{year}` containing:

```
data/{year}/{team_id}.csv
data/{year}/{team_id}_vs.csv
data/{year}/{team_id}_leverage.csv
data/{year}/{team_id}_vs_leverage.csv
```

## Usage

### Run locally

```bash
pip install requests pandas
python fetch_onoff.py --year 2026
```

### Trigger manually

```bash
gh workflow run onoff-daily.yml -f year=2026
```

### Download artifact

```bash
gh run download --repo RussDT/nba-onoff-fetch -n onoff-2026 --dir /path/to/output/
```
