# Zleek Score: rules for Claude Code

## Non-negotiables
- Never restart, kill or reload the worker; never run docker, launchctl or git push. The human does these.
- Database: read-only SELECTs via zleek_web only. Any write goes in a script under apps/worker/scripts/ for the human to run.
- Repair scripts must be idempotent and safe to run before or after a worker restart. Recompute from stored source data (raw), never from the current value.
- Never commit secrets. .env files stay untracked.
- Stop and report if the brief contradicts what you find in the code or data.

## Workflow
- One change at a time: show the diff, explain in 2-3 bullets, wait for approval.
- Every fix adds or updates a test or an acceptance query that would have caught the bug.
- After each task, update infra/TODO.md.

## Priorities (in order)
1. Data correctness: wrong numbers in the product
2. Data loss
3. Reliability and monitoring
4. Performance
5. Features. Nothing outside the v1 scope without asking.

## Context
- The worker runs live under launchd from ~/Projects/Zleek_Score-main; the heartbeat runs every 15 min.
- Backlog with file references: infra/TODO.md.

## Known gotchas
- Gamma clobTokenIds: index 0 = outcome 0 ("Yes" on Yes/No markets). Labels were stored reversed until 2026-10-30.
- Prices: store YES-equivalent prices. NO-token trades and book data must be converted (1 - p, bids/asks swapped).
- The REST backfill re-fetches the last hour every 5 min, and REST_QUERY's ON CONFLICT (tx_hash) overwrites price. Any repair must account for this.
- Postgres secrets use hex-only passwords; base64 characters break connection URLs.
- Deploy order for data fixes: human restarts the worker first, then runs repairs, then rebuilds.

## Definition of done
- Diff approved, test or acceptance query added, infra/TODO.md updated, and the deploy steps written out for the human.