# Task queue for agents

Work an agent can do without the human: code, tests and docs only. Filled on
2026-11-16 from infra/TODO.md and ROADMAP.md with everything that doesn't touch
production and doesn't wait on a measurement or a decision. Production steps,
measurements and decisions stay with the human (see "Not for agents" at the
end).

## Rules for agents

1. **Take the top task marked `ready`** that has no open pull request yet
   (check `gh pr list --state open` for a branch `task/<id>-…`). Skip `blocked`
   and `in review` tasks.
2. **At most 3 open pull requests at once.** If 3 are already open (any
   author), stop: don't start another task.
3. **One task per branch, branched from the current `main`**:
   `git checkout main && git pull --ff-only && git checkout -b task/<id>-<short-name>`.
4. **Never touch production**: no ssh or scp to the server, no deploys, no
   production database, no `docker compose -f docker-compose.prod.yml up`, no
   real credentials. Editing files that production uses (e.g.
   docker-compose.prod.yml, worker code) is fine; deploying them is the
   human's job. Tests must not call live APIs or databases (CI has none).
5. **Never edit ROADMAP.md.** Record results, measurements and anything
   worth keeping in the pull request description; the human moves them into
   the ROADMAP.
6. **Done means verified**: run every check in the task's "Verify" line, plus
   `apps/worker/tests/run_offline.sh` for worker changes and the web checks
   (`cd apps/web && npm run lint && npx tsc --noEmit && npm test && npm run build`)
   for web changes. CI (.github/workflows/ci.yml) must be green.
7. **In the same pull request, change the task's status line** in this file to
   `in review (PR #n)`. The human sets `done` when merging.
8. **If a task needs a decision from the human** (a choice the task doesn't
   settle, a behaviour change it didn't ask for, anything that would change
   what users see or what is stored), stop. Change its status to
   `blocked: needs decision`, add a "Decision needed" line under it with the
   options, and open a pull request with only that TASKS.md change. Then take
   the next ready task (rule 2 still applies).
9. Commit messages and PR descriptions follow the repo's style: what changed,
   why, how it was verified. No secrets in code, tests, logs or PRs.

## Ready

### T1. Keep events when Redis is down (data loss)
- **Status:** in review (PR #5)
- **What:** `EliteZleekWriter.add_item` does a bare `redis.lpush`; when Redis
  is unreachable the trade or book event is lost (infra/TODO.md, "events are
  dropped while Redis is down"). Add a small bounded in-memory fallback buffer
  in the writer: when `lpush` fails, keep the item (oldest dropped first when
  full, with a count logged); drain it into Redis once Redis answers again.
  Same queue keys and order as today. Don't change what is written to Postgres.
- **Files:** apps/worker/ingestion/writer.py (add_item, flush_loop or a
  drain step), a new test file apps/worker/tests/test_redis_fallback.py with a
  fake Redis that fails and recovers.
- **Verify:** new tests show (a) items survive a Redis outage and reach the
  queue in order after recovery, (b) the bound holds and the drop count is
  logged, (c) normal operation is unchanged; `run_offline.sh` passes.
- **Note:** phase4/storage-trades-archive (not merged yet) also edits writer.py (REST_QUERY,
  newest_stored_trade); stay out of those parts.

### T2. Redis persistence (small data-loss risk)
- **Status:** in review (PR #7)
- **What:** production Redis runs without persistence (infra/TODO.md). Add
  `--appendonly yes` to the redis command in docker-compose.prod.yml (data
  already persists to ./redis_data). The human applies it on the next Redis
  restart.
- **Files:** docker-compose.prod.yml; infra/TODO.md (mark the item fixed in
  git, "applies on the next Redis restart").
- **Verify:** `docker compose -f docker-compose.prod.yml config --no-interpolate`
  shows the new command; a local `docker run --rm redis:7-alpine redis-server
  --appendonly yes` starts and `CONFIG GET appendonly` answers `yes`. Put the
  output in the PR.

### T3. Storage: retention and compression for the other time-series tables
- **Status:** in review (PR #8)
- **What:** the storage step's decided retention (docs/storage-plan.md,
  "Retention"): features_5m 30 days, signals_history 90 days, ob_snapshots 30
  days; bars_1m kept, compressed; 1-day chunks for new hypertables. Write a
  migration that converts ob_snapshots and signals_history to hypertables
  (`migrate_data => true`; their keys already include the time), sets
  compression (segment by market_id, order by time DESC) with
  `compress_after => 3 days`, and adds the retention policies. Creates and
  schedules only; running it on production is the human's.
- **Files:** infra/sql/migrations/storage_retention.sql; a test
  infra/sql/migrations/test_storage_retention.sh in the style of
  test_trades_archive.sh (throwaway TimescaleDB 2.31.0, or
  ZLEEK_TEST_DATABASE_URL); a CI job step for it if it runs in under ~5 min.
- **Verify:** the test seeds rows older and newer than each retention, runs
  the migration twice (idempotent), runs the policies (`CALL run_job(...)`),
  and checks: old rows gone only where retention applies, bars_1m untouched,
  chunks compressed, the dashboard/detail queries in apps/web/lib/queries.ts
  still return the same rows for recent data. Report timings in the PR.
- **Decision needed if:** a reader needs features_5m/signals_history/ob_snapshots
  older than its retention (grep the repo first; list what you found).

### T4. Storage: stop rewriting unchanged markets, and shrink markets.raw
- **Status:** ready
- **What:** the Gamma refresher rewrites all ~2,100 fetched markets every 10
  minutes with their full raw JSON (~7.3 KB), ~300,000 row versions a day
  (ROADMAP step 4; storage-plan). Every row differs between refreshes in
  `events`, liquidity and volume, so a whole-row comparison won't help:
  update a row only when a field the code uses changed (status, title, token
  ids, condition id, event id/title, category, liquidity_usd and volume24hr
  rounded so small moves don't count, closed, acceptingOrders, endDate), and
  store raw trimmed to the keys the code reads. Keep the selections' 2-hour
  freshness check (`updated_at > now() - 2 h` in load_asset_ids /
  load_condition_ids_for_backfill) working: choose between refreshing a
  freshness stamp cheaply and a separate column, and explain the choice in
  the PR.
- **Files:** apps/worker/ingestion/writer.py (upsert_markets),
  apps/worker/ingestion/gamma_client.py (parse_markets), any reader of
  markets.raw (today: apps/web/lib/queries.ts reads closed, acceptingOrders,
  endDate; writer.py reads volume24hr; acceptance/scripts read clobTokenIds:
  grep `raw->>`, `raw ->>` and `raw ?` across the repo and list every key in
  the PR); tests.
- **Verify:** offline tests with recorded Gamma rows: an unchanged refresh
  writes nothing, a status/liquidity change writes once, every key any reader
  uses survives the trim; a SQL test on a throwaway TimescaleDB that counts
  row versions (n_tup_upd) over two identical refreshes; `run_offline.sh`.
- **Decision needed if:** a raw key is read only by one-off scripts (keep or
  drop?), or the freshness design changes which markets the selections pick.

### T5. Log token-id parse failures in gamma_client
- **Status:** in review (PR #10)
- **What:** `parse_token_ids` (gamma_client.py, the `except Exception:` after
  `json.loads(raw_ids)`) swallows malformed `clobTokenIds`, and a list whose
  length isn't 2 falls through silently; the market can then never be mapped
  (infra/TODO.md). Log the market id and the raw value in both cases, as
  parse_markets already does for whole-row failures. No behaviour change.
- **Files:** apps/worker/ingestion/gamma_client.py; a test (e.g.
  apps/worker/tests/test_gamma_parse.py).
- **Verify:** tests for malformed JSON, 1 and 3 ids, and a good pair (no log);
  `run_offline.sh`.

### T6. Gamma keyset pagination (markets past ~2,100 never refresh)
- **Status:** blocked: needs decision
- **Decision needed:** the keyset response shape is unverified and can't be recorded from the agent environment (gamma-api.polymarket.com was unreachable, curl HTTP 000, and a second request was denied by the session's permission check). Options: (a) the human records one read-only fixture, e.g. `curl 'https://gamma-api.polymarket.com/markets/keyset?active=true&closed=false&liquidity_num_min=10000&volume_num_min=1000&order=volume24hr&ascending=false&limit=2'` and `…&limit=100` plus the next-cursor page, into apps/worker/tests/fixtures/, then T6 goes back to `ready`; (b) allow the agent environment network access to gamma-api.polymarket.com; (c) implement from the docs alone with a guessed cursor field name (not recommended: a wrong guess ends the fetch silently at the first page).
- **What:** fetch_markets pages by offset and Gamma rejects offset >= 2100
  with 422 "use /markets/keyset" (infra/TODO.md; the 422 also shows in every
  worker log). Switch to keyset pagination with the same filters and order
  (volume24hr desc), stopping cleanly at the end.
- **Files:** apps/worker/ingestion/gamma_client.py; a test with recorded
  pages (apps/worker/tests/fixtures/).
- **Verify:** offline test over recorded keyset pages (cursor followed, end
  detected, same parsed rows as offset paging for the overlap). A one-off
  read-only request to the public Gamma API to record the fixture is fine;
  tests themselves must not call it. Note in the PR how many markets a full
  fetch returns now and that the worker will refresh more rows (more upserts:
  coordinate with T4 if both are open).
- **Decision needed if:** keyset returns far more markets than today (more
  than ~2x): the refresher's cost changes.

### T7. Multi-fill check reads docker logs
- **Status:** ready
- **What:** apps/worker/scripts/check_multifill_collisions.py Check 1 reads
  ingestion/worker.log, which doesn't exist on the server; there the worker
  logs to docker (`docker compose -f docker-compose.prod.yml logs worker`)
  (infra/TODO.md). Accept that output: `--log -` for stdin, and docker's line
  prefixes (container name, timestamps) stripped before parsing.
- **Files:** apps/worker/scripts/check_multifill_collisions.py; a test with a
  few recorded log lines in both formats.
- **Verify:** the test parses both formats to the same events; the script's
  usage text shows the server command (`… logs --no-color worker | python
  scripts/check_multifill_collisions.py --log -`); `run_offline.sh`.

### T8. Grant SELECT on bars_state to zleek_web (script only)
- **Status:** ready
- **What:** the read-only role can't read bars_state (infra/TODO.md). Write
  the one-line SQL for the human to run, and add it to the role setup in
  docs/parallel-run.md section 6 so new databases get it.
- **Files:** apps/worker/scripts/grant_bars_state_zleek_web.sql;
  docs/parallel-run.md; infra/TODO.md (fixed in git, to run on production).
- **Verify:** on a throwaway TimescaleDB with schema.sql and a zleek_web role:
  SELECT on bars_state is denied before, allowed after, and zleek_web still
  can't write. Paste the output in the PR.

### T9. Tidy infra/TODO.md
- **Status:** ready
- **What:** mark entries that later work already fixed, with where: "backfill
  market selection is stale" (fixed by ROADMAP phase 4 step 2, ranked by 24h
  volume with the 2-hour freshness filter, writer.py
  load_condition_ids_for_backfill); "IN PROGRESS (Phase 1): outcome-token
  orientation" (phase 1 done 2026-10-30 per ROADMAP; confirm from the repair
  scripts in apps/worker/scripts/ and git log); the 2b entry's "STILL OPEN"
  recovery (applied 2026-11-07, ROADMAP step 2b). Only status lines change;
  no item is deleted.
- **Files:** infra/TODO.md.
- **Verify:** each change cites the commit or ROADMAP line that fixed it.
- **Decision needed if:** the evidence for an entry is unclear: leave it and
  list it in the PR.

## Blocked

- **Mark closed markets as closed.** All 43,314 markets rows say 'open'
  (only ~2,600 refreshed): the refresher fetches only `closed=false`, so a
  market that closes is never updated (ROADMAP step 4 baseline, 2026-11-16).
  **blocked: needs decision**: fetch the known ids' state from Gamma
  periodically (as gen_status_cleanup.py did once), or mark markets unseen
  for N days as stale/closed.
- **Exact prices instead of float noise** (infra/TODO.md). **blocked: needs
  decision**: Decimal(str(value)) or rounding to the tick size; either way
  2a's "price changed" guard rewrites recent rows once.
- **Imbalance saturates on whole-book depth** (infra/TODO.md). **blocked:
  needs decision** (depth near the mid, shares vs USD, …).
- **Order books: top 10 levels, snapshot on top-of-book change** (ROADMAP
  step 6). **blocked by** the imbalance decision above (it changes what
  imbalance is computed from).
- **Scoring floor for frozen and penny markets** (infra/TODO.md).
  **blocked: needs decision.**
- **Bot/dust market rule** (infra/TODO.md, before the cap raise).
  **blocked: needs decision.**
- **Per-fill unique key for multi-fill transactions** (infra/TODO.md).
  **blocked: needs decision** (schema change; observed 0 times so far).
- **Stale scores look fresh** (ROADMAP step 6 leftovers: store the newest bar
  time with each score). **blocked: needs decision.**
- **Websocket cap raise** (ROADMAP step 5). **blocked by** the storage step
  and the bot/dust decision; it's a production change.
- **README architecture diagram** (ROADMAP phase 5). **blocked by** the
  ROADMAP order (finish phase 4 first).
- **Build and publish images in CI** (infra/TODO.md). **blocked:** post-v1.

## Not for agents (human only)

The trades_archive rehearsal on a production dump (the Mac's
com.zleek.rehearsal job does it nightly), the trades_archive production run,
every deploy, backups and their install, the nightly per-day measurements
and anything that reads the production database, the off-site retention
review due 2026-12-01, and all decisions above.
