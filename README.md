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

The portable local entrypoint is `scripts/run_local_wnba_player_refresh.sh`. It
pulls `main`, runs the contract tests, refreshes the current UTC season with the
official minutes/position enrichment, and commits only the player artifact files.
It expects a repo-local `.venv` with `pandas`, `requests`, and
`curl_cffi==0.14.0`. Install or repair the launchd job with
`scripts/install_wnba_player_launchd.sh`; the generated plist explicitly invokes
Bash, runs at 5:15 AM local time, and also runs when it is loaded so a reboot or
missed calendar window catches up. The installer registers a write-enabled SSH
deploy key scoped only to this repository, so background pushes do not depend on
an interactive macOS Keychain prompt. A failed push leaves the local commit ahead
of `origin/main`, and the next run retries it even if the fetched data is unchanged.
The `Monitor WNBA Player Artifact Freshness`
workflow checks the committed CSV, metadata timestamp, row count, and hash twice
daily and fails once the artifact is more than 30 hours old.

## WNBA on/off schedule

`Daily WNBA On-Off Fetch` requests regular-season splits by default. Playoff
splits are opt-in so an empty, not-yet-started postseason cannot delay or fail
the daily regular-season artifact. Enable them with the `include_playoffs`
manual-dispatch input or by setting the repository variable
`WNBA_INCLUDE_PLAYOFFS=true` when the postseason begins.

Every PBPStats request keeps the shared five-attempt retry policy. If a request
still fails, the WNBA fetch makes one recovery pass after finishing that block;
only requests that also fail the recovery pass make the workflow fail. Partial
artifacts remain fail-closed and are never uploaded as a successful run.

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
