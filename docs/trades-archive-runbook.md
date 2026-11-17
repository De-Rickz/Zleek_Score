# trades_archive: production runbook

Design: docs/trades-archive-design.md. Run by the human, on the server, in
this order. Each step says what to check before going on, and how to undo it.
Timings are from the rehearsal on the 2026-11-17 production dump (3.6 M
trades, 5.6 GB database).

**Before starting**
- The trades_archive pull request is merged, and its CI run (including the
  trades-archive integration job) was green.
- The nightly Mac rehearsal (com.zleek.rehearsal) passed on the newest dump.
- Not during a measurement day, nor while a backup (03:20 UTC) or the
  restore test runs. About 30 minutes, most of it the backup.
- Decided: run `VACUUM FULL trades` at the end (step 9), yes or no.

On the server, every step below starts from:

    cd ~/zleek && PROD="docker compose -f docker-compose.prod.yml"
    psql_rw() { $PROD exec -T db psql -U postgres -d postgres -X -v ON_ERROR_STOP=1 "$@"; }

## 0. Snapshot (read-only)

    df -h / ; psql_rw -c "SELECT count(*) AS trades, max(ts) AS newest, pg_size_pretty(pg_total_relation_size('trades')) AS size FROM trades"

Write the numbers down: steps 3, 5 and 10 compare against them.

## 1. Fresh backup, confirmed off-site, and the restore test

Runs the crontab's own backup command, then checks it:

    bash -c "$(crontab -l | grep zleek-db-backup | sed 's/^20 3 \* \* \* //; s/ *>>.*//')"; tail -n 1 ~/backups/zleek-db/backup.log
    D=$(ls -1 ~/backups/zleek-db/zleek-*.dump | sort | tail -n 1); rclone check ~/backups/zleek-db zleek-offsite:zleek_db_backups/daily --size-only --one-way --include "$(basename "$D")"
    ~/bin/zleek-restore-test.sh; echo "exit=$?"; tail -n 1 ~/backups/zleek-db/restore-test.log

Go on only with `OK … offsite=ok`, `0 differences found`, and the restore
test's `OK` (~10 min, ~3 min of it the restore). If the restore test refuses
for disk space, stop: don't migrate without a proven backup.

## 2. Code on the server

    git status --short; git pull --ff-only && git log --oneline -1

`git status` must show no modified tracked files (infra/TODO.md workflow
rule). Then install the updated host scripts (restore test reads trades_all):

    install -m 700 infra/backup/zleek-restore-test.sh ~/bin/

## 3. Migration (creates objects, moves nothing): ~1 s

    psql_rw -1 < infra/sql/migrations/trades_archive.sql && psql_rw -c "SELECT archive_cutoff(), (SELECT count(*) FROM trades_all) AS trades_all, (SELECT count(*) FROM trades_archive) AS archived"

Check: archived = 0, trades_all = step 0's count (plus trades since).
Undo: `psql_rw -c "DROP VIEW trades_all; DROP TABLE trades_archive; DROP FUNCTION zleek_store_trade, archive_trades_move, archive_cutoff; DROP PROCEDURE archive_trades_job, archive_trades_backlog, archive_trades_unarchive"`
(nothing uses them yet).

## 4. Deploy worker, web and heartbeat (they read trades_all, write through zleek_store_trade)

The worker must not start before step 3: its writes call zleek_store_trade.
This deploy also ships T1 (Redis fallback buffer) from main.

    STAMP=$(date -u +%Y%m%dT%H%MZ); docker tag zleek-worker:latest zleek-worker:rollback-$STAMP && docker images --format '{{.Repository}}:{{.Tag}}' | grep -E 'web' | head -3

Tag the web image the same way (its name is in that list, e.g.
`docker tag zleek-web:latest zleek-web:rollback-$STAMP`), then:

    $PROD up -d --build worker web && $PROD --profile cron build heartbeat

Check after ~3 minutes:

    docker ps --format '{{.Names}} {{.Status}}' | grep -E 'zleek_(worker|web)'; docker logs --since 5m zleek_worker 2>&1 | grep -iE 'error|exception' | grep -v 'offset=2100' | head; psql_rw -c "SELECT count(*) FROM trades WHERE ts > now() - interval '3 minutes'"

Want: both Up, no errors, trades arriving (the restart also runs the gap
backfill and step 2c's 24-hour catch-up for 200 markets: lots of
`Backfill catch-up on entry` is expected). The dashboard and a market page
load in the browser.
Undo: `docker tag zleek-worker:rollback-$STAMP zleek-worker:latest && docker tag zleek-web:rollback-$STAMP zleek-web:latest && $PROD up -d --no-build worker web`
(no rows have moved yet, so the old code sees everything).

## 5. Backlog move, one day per transaction: ~1.5 min

    psql_rw -c "CALL archive_trades_backlog()"

Prints one NOTICE per day moved (~11 days). Check:

    psql_rw -c "SELECT (SELECT count(*) FROM trades WHERE ts < archive_cutoff()) AS left_behind, (SELECT count(*) FROM trades t JOIN trades_archive a USING (tx_hash)) AS in_both, (SELECT count(*) FROM trades_all) AS total"

Want: left_behind 0, in_both 0, total = step 0's count plus new trades.
Peak extra disk in the rehearsal: +2.6 GB (moved rows exist twice until
autovacuum and step 9). Re-runnable: each day is moved or not.
Undo (only if the code must also be rolled back after this point):
`psql_rw -c "CALL archive_trades_unarchive()"` (moves every archived row back,
one day per transaction; refuses while the job from step 7 is scheduled),
then step 4's undo.

## 6. Compress the archive: ~1 min

    psql_rw -c "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('trades_archive', older_than => interval '5 days') c" -c "ANALYZE trades_archive" -c "SELECT pg_size_pretty(sum(before_compression_total_bytes)) AS before, pg_size_pretty(sum(after_compression_total_bytes)) AS after FROM chunk_compression_stats('trades_archive')"

Rehearsal: 2,156 MB -> 409 MB (5.3x).

## 7. Compression policy and the nightly move (04:00 UTC)

    psql_rw < infra/sql/migrations/trades_archive_enable.sql && psql_rw -c "SELECT job_id, proc_name, next_start FROM timescaledb_information.jobs WHERE proc_name IN ('archive_trades_job', 'policy_compression')"

Undo: `psql_rw -c "SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE proc_name = 'archive_trades_job'"`.

## 8. Readers, once more

Open the dashboard and two market pages (one active, one whose trades are
all older than 4 days) in the browser; then run the heartbeat once by hand,
as its cron line does (it now logs `archive=oldest…,both0,dupes0`):

    $PROD run --rm --no-deps heartbeat 2>&1 | tail -n 2

## 9. Give the space back (if decided): ~20 s, exclusive lock on trades

    psql_rw -c "VACUUM FULL trades" && df -h / && psql_rw -c "SELECT pg_size_pretty(pg_total_relation_size('trades'))"

Rehearsal: 17 s, trades 3.9 GB -> 1.2 GB. The worker's writes wait for the
lock; Redis buffers them and the next flush writes them. Skipped: the space is
reused by new trades instead (no disk back).

## 10. Afterwards

- Record in ROADMAP step 4: the times, sizes and checks from steps 0-9.
- Tomorrow after 04:00 UTC: the job ran and moved one day:
  `psql_rw -c "SELECT last_run_status, last_successful_finish, total_runs, total_failures FROM timescaledb_information.job_stats WHERE proc_name = 'archive_trades_job'"`.
- After 03:20 UTC: the nightly dump is ~18% larger than without the archive
  (rehearsal: 681 -> 801 MB): watch the off-site total (review due
  2026-12-01, docs/storage-plan.md).
- The next monthly restore test (2nd, 04:30 UTC) counts trades_all.

## Never

- `ORDER BY ts DESC, <another column> … LIMIT` on trades_all: TimescaleDB
  2.31.0 errors once the archive is analyzed (docs/trades-archive-design.md).
- Writing to trades_archive except through zleek_store_trade or the
  procedures above (no unique key there: the dedupe is in the function).
