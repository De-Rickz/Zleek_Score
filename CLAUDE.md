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
- Never use `git checkout --`, `git restore` or `git reset --hard` on files with uncommitted work: they discard it. Commit (or stash) before making a temporary edit, then revert the temporary edit with a normal commit or a reverse patch, and confirm `git diff` is empty afterwards. (2026-11-03: a `git checkout -- app/page.tsx` meant to undo a one-line screenshot edit would have thrown away a whole uncommitted feature.)

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
- The websocket loads the token mapping (load_asset_maps) only when it connects. After any repair of markets.token_* columns, live trades and order books keep the old mapping until the next "Subscribed to N assets" in worker.log. Trades can be re-repaired from raw; ob_snapshots can't, so their cutoff is that subscribe time, not the restart (2026-10-30: restart 17:06:04, subscribe 17:24:17).
- 2026-11-02, Oracle Cloud deploy: the VCN's default Security List only allowed ingress on 22. Ports 80/443 must be opened in the Security List (cloud firewall) as well as in ufw on the instance; with only ufw open, Caddy's ACME challenges (HTTP-01 and TLS-ALPN-01) timed out.
- 2026-11-02, DNS: zleekin.com had a stale second A record (GoDaddy "WebsiteBuilder Site" parking page) next to the Oracle server's record. DNS round-robined between them, giving intermittent 403s during certificate issuance. Before relying on ACME, check that the domain resolves only to the server (`dig +short A zleekin.com`, and AAAA).

## Definition of done
- Diff approved, test or acceptance query added, infra/TODO.md updated, and the deploy steps written out for the human.