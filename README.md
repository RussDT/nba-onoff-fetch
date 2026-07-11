# nba-onoff-fetch

Daily PBPStats on-off (wowy) data fetch via GitHub Actions.

## WNBA player totals contract

`data/{year}_pbp.csv` keeps PBPStats possession and event metrics, but PBPStats
is not trusted for WNBA playing time or roster position. The daily WNBA totals
workflow enriches every PBP player by the official WNBA numeric player ID:

- `stats.wnba.com/stats/leaguedashplayerstats.MIN` replaces `Minutes` and
  `SecondsPlayed`.
- `stats.wnba.com/stats/playerindex.POSITION` populates `Pos`, `pos`, and
  `Pos2` with the official broad `G` / `F` / `C` vocabulary and hybrids.

The job fails before publishing if any PBP player is unmatched, any minute value
is negative or non-finite, or any position is blank/unsupported. CSV replacement
is atomic, and a 10%+ row-count drop is rejected, so a failed official refresh
leaves the last committed artifact intact. Each player artifact also publishes a
`.meta.json` sidecar with generation time, row count, content hash, and source URLs.

The official WNBA endpoints block GitHub-hosted runner IPs. The existing local
`2026_NBA_PIPELINE` launchd job therefore owns the player artifact refresh and
push. GitHub Actions continues to refresh league-wide WNBA season totals and run
the contract test suite, but it does not call the official player endpoints.

## Current 2026 State

This repo now freezes the completed regular-season snapshot and publishes playoff on-off files for the 2025-26 postseason handoff.

- The on/off fetch path now uses `SeasonType=Playoffs` only and writes `_ps` filenames into the daily artifact.
- Regular-season season totals use `SeasonType=Regular Season`.
- Playoff season totals use `SeasonType=Playoffs`.
- The `season-totals-daily` workflow runs daily again so both RS and PS totals stay current.

## Important Warning

The daily on-off artifact now contains playoff files only:

- `{team_id}_ps.csv`
- `{team_id}_vs_ps.csv`
- `{team_id}_ps_leverage.csv`
- `{team_id}_vs_ps_leverage.csv`

Runs at 4:45 AM PST. Artifacts downloadable via:
```bash
gh run download --repo RussDT/nba-onoff-fetch -n "onoff-2026" --dir nba_rapm/on-off/
```
