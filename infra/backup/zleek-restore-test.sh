#!/usr/bin/env bash
# zleek-restore-test.sh: prove the newest nightly dump restores. Monthly.
#
# The 2026-11-02 manual restore test (docs/parallel-run.md section 4), scripted:
#  1. Newest dump from $BACKUP_DIR (zleek-db-backup.sh writes them).
#  2. A throwaway container from the SAME image as the running zleek_db (so the
#     TimescaleDB version matches), with no network, removed afterwards even
#     on failure. Production is only read (one row count, as zleek_web, in a
#     read-only transaction).
#  3. CREATE EXTENSION timescaledb; timescaledb_pre_restore(); pg_restore
#     --no-owner --no-acl; timescaledb_post_restore().
#  4. Checks: pg_restore succeeded; trades, bars_1m and markets have rows; the
#     copy's trade count is within 0.1% of production's up to the copy's newest
#     trade (later backfills can add a few older rows to production); token
#     labels are right (acceptance C); the acceptance file runs on the copy.
# One log line per run in $BACKUP_DIR/restore-test.log; a FAIL is also pushed
# to ntfy (the heartbeat's topic).
#
# Installed as ~/bin/zleek-restore-test.sh, run by cron monthly
# (docs/storage-plan.md). Needs free disk of about 15x the dump size (a 54 MB
# dump restored to ~600 MB on 2026-11-04); refuses to start otherwise.
set -euo pipefail
umask 077

DB_CONTAINER="${ZLEEK_DB_CONTAINER:-zleek_db}"
BACKUP_DIR="${ZLEEK_BACKUP_DIR:-$HOME/backups/zleek-db}"
REPO="${ZLEEK_REPO:-$HOME/zleek}"
TEST_CONTAINER="zleek-restore-test"
LOG="$BACKUP_DIR/restore-test.log"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../ops/zleek-ntfy.sh
. "${ZLEEK_NTFY_HELPER:-$SCRIPT_DIR/zleek-ntfy.sh}"

mkdir -p "$BACKUP_DIR"
log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$LOG"; }
fail() {
    log "FAIL $1"
    zleek_ntfy high warning "Restore test FAILED: $1 (see $LOG)" || true
    exit 1
}

exec 9>"$BACKUP_DIR/.restore-test.lock"
flock -n 9 || { log "SKIP another restore test is still running"; exit 0; }

DUMP="$(ls -1 "$BACKUP_DIR"/zleek-*.dump 2>/dev/null | sort | tail -n 1 || true)"
[ -n "$DUMP" ] || fail "no dump found in $BACKUP_DIR"
dump_bytes="$(stat -c %s "$DUMP" 2>/dev/null || stat -f %z "$DUMP")"
free_bytes="$(df -Pk "$BACKUP_DIR" | awk 'NR == 2 { print $4 * 1024 }')"
[ "$free_bytes" -gt $(( dump_bytes * 15 )) ] \
    || fail "not enough free disk: $((free_bytes / 1048576)) MB free, need ~$((dump_bytes * 15 / 1048576)) MB for $(basename "$DUMP")"

IMAGE="$(docker inspect --format '{{.Config.Image}}' "$DB_CONTAINER")" || fail "cannot read the image of $DB_CONTAINER"
start="$(date +%s)"
trap 'docker rm -f "$TEST_CONTAINER" >/dev/null 2>&1 || true' EXIT
docker rm -f "$TEST_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$TEST_CONTAINER" --network none \
    -e POSTGRES_PASSWORD="$(head -c 18 /dev/urandom | od -An -tx1 | tr -d ' \n')" "$IMAGE" >/dev/null \
    || fail "could not start $IMAGE"

q() { docker exec -i "$TEST_CONTAINER" psql -U postgres -X -v ON_ERROR_STOP=1 -tA "$@"; }
for _ in $(seq 1 60); do
    # ready = accepting connections AND the image's first-start init has finished
    docker exec "$TEST_CONTAINER" pg_isready -U postgres -q 2>/dev/null \
        && q -c 'SELECT 1' >/dev/null 2>&1 && break
    sleep 2
done
q -c 'SELECT 1' >/dev/null 2>&1 || fail "test container never became ready"
sleep 5   # the image restarts Postgres once after initdb; let that settle
for _ in $(seq 1 30); do q -c 'SELECT 1' >/dev/null 2>&1 && break; sleep 2; done

q -d postgres -c 'CREATE DATABASE restored' >/dev/null || fail "CREATE DATABASE failed"
q -d restored -c 'CREATE EXTENSION IF NOT EXISTS timescaledb' -c 'SELECT timescaledb_pre_restore()' >/dev/null \
    || fail "timescaledb_pre_restore failed"
ERR="$(mktemp)"
docker exec -i "$TEST_CONTAINER" pg_restore -U postgres -d restored --no-owner --no-acl < "$DUMP" 2> "$ERR" \
    || fail "pg_restore exited non-zero: $(grep -i error "$ERR" | head -n 3 | tr '\n' ' ')"
rm -f "$ERR"
q -d restored -c 'SELECT timescaledb_post_restore()' >/dev/null || fail "timescaledb_post_restore failed"

trades="$(q -d restored -c 'SELECT count(*) FROM trades')"
bars="$(q -d restored -c 'SELECT count(*) FROM bars_1m')"
markets="$(q -d restored -c 'SELECT count(*) FROM markets')"
[ "$trades" -gt 0 ] && [ "$bars" -gt 0 ] && [ "$markets" -gt 0 ] \
    || fail "empty tables in the copy: trades=$trades bars_1m=$bars markets=$markets"

newest="$(q -d restored -c "SELECT to_char(max(ts) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM trades")"
prod="$(docker exec -e PGOPTIONS='-c default_transaction_read_only=on' "$DB_CONTAINER" \
        psql -U zleek_web -d postgres -X -tA -c "SELECT count(*) FROM trades WHERE ts <= '$newest+00'")" \
    || fail "could not count production trades (read-only)"
# copy must hold at least 99.9% of production's trades up to its newest trade
[ $(( trades * 1000 )) -ge $(( prod * 999 )) ] && [ "$trades" -le "$prod" ] \
    || fail "trade count mismatch: copy $trades vs production $prod up to $newest UTC"

labels_wrong="$(q -d restored -c "SELECT count(*) FROM markets WHERE jsonb_typeof((raw->>'clobTokenIds')::jsonb) = 'array' AND token_yes_id IS DISTINCT FROM (raw->>'clobTokenIds')::jsonb->>0")"
[ "$labels_wrong" = 0 ] || fail "acceptance C: $labels_wrong token labels wrong in the copy"
q -d restored < "$REPO/apps/worker/scripts/acceptance_price_orientation.sql" >/dev/null \
    || fail "acceptance_price_orientation.sql did not run on the copy"

log "OK $(basename "$DUMP") image=$IMAGE trades=$trades (production $prod up to $newest UTC) bars_1m=$bars markets=$markets seconds=$(( $(date +%s) - start ))"
