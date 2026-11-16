#!/usr/bin/env bash
# Nightly backup of the production Zleek Score database.
#
# Runs pg_dump inside the running db container (so the dump tool always matches
# the server's Postgres/TimescaleDB), writes a compressed custom-format dump to
# the server's own disk, checks the dump is readable (pg_restore --list) before
# keeping it, keeps the newest $KEEP dumps, optionally copies every local dump
# not yet off the instance to an rclone remote (ZLEEK_BACKUP_REMOTE), and
# appends one line per run to $BACKUP_DIR/backup.log. Read-only for the
# database: pg_dump takes a consistent snapshot and never blocks the worker's
# inserts.
#
# Installed on the server as ~/bin/zleek-db-backup.sh and run by cron (see
# docs/parallel-run.md, section 9). Exit 0 on success or when another run is
# still going; 1 on failure (the failed run's partial file is removed, and
# no older dump is deleted). A failed off-site copy also exits 1, but keeps
# the new local dump, and the next run retries every dump the remote lacks.
#
# Every failure alerts on two channels (added 2026-11-16, after off-site
# copies failed unnoticed from 2026-11-09 to 11-16): healthchecks.io
# (ZLEEK_BACKUP_HEALTHCHECK_URL, now REQUIRED: without it the dump still runs
# but the run counts as failed) and an ntfy push on the heartbeat's topic
# (zleek-ntfy.sh, installed next to this script). healthchecks.io also alerts
# when no run reports at all.
set -euo pipefail
umask 077   # dumps hold all production data: readable by this user only

CONTAINER="${ZLEEK_DB_CONTAINER:-zleek_db}"
DB_NAME="${ZLEEK_DB_NAME:-postgres}"
DB_USER="${ZLEEK_DB_USER:-postgres}"
BACKUP_DIR="${ZLEEK_BACKUP_DIR:-$HOME/backups/zleek-db}"
KEEP="${ZLEEK_BACKUP_KEEP:-7}"
HEALTHCHECK_URL="${ZLEEK_BACKUP_HEALTHCHECK_URL:-}"   # REQUIRED: the healthchecks.io check for this job
REMOTE="${ZLEEK_BACKUP_REMOTE:-}"   # optional: rclone destination, e.g. zleek-offsite:zleek_db_backups/daily

mkdir -p "$BACKUP_DIR"
LOG="$BACKUP_DIR/backup.log"
log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$LOG"; }
hc() { if [ -n "$HEALTHCHECK_URL" ]; then curl -fsS -m 10 --retry 3 -o /dev/null --data-raw "$2" "$HEALTHCHECK_URL$1" || true; fi; }
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../ops/zleek-ntfy.sh
if [ -r "${ZLEEK_NTFY_HELPER:-$SCRIPT_DIR/zleek-ntfy.sh}" ]; then
    . "${ZLEEK_NTFY_HELPER:-$SCRIPT_DIR/zleek-ntfy.sh}"
else
    zleek_ntfy() { echo "zleek_ntfy: helper not found next to $0; not sent: $3" >&2; return 1; }
fi
push() { zleek_ntfy urgent rotating_light "Backup FAILED: $1" > /dev/null 2>&1 || log "WARN ntfy push failed"; }

# One run at a time (a slow dump must not overlap the next night's).
exec 9>"$BACKUP_DIR/.lock"
if ! flock -n 9; then log "SKIP another backup is still running"; exit 0; fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
FINAL="$BACKUP_DIR/zleek-$STAMP.dump"
TMP="$FINAL.partial"
ERR="$FINAL.stderr"
trap 'rm -f "$TMP" "$ERR"' EXIT

fail() {
    local detail
    detail="$(tail -c 400 "$ERR" 2>/dev/null | tr '\n' ' ')"
    log "FAIL $1${detail:+ | $detail}"
    hc /fail "FAIL $1 $detail"
    push "$1${detail:+ | $detail}"
    exit 1
}

hc /start ""
start="$(date +%s)"

# -Fc: custom format, compressed (zlib level 6), restorable with pg_restore.
docker exec "$CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" -Fc -Z 6 > "$TMP" 2> "$ERR" \
    || fail "pg_dump exited with status $?"
[ -s "$TMP" ] || fail "dump is empty"
docker exec -i "$CONTAINER" pg_restore --list < "$TMP" > /dev/null 2>> "$ERR" \
    || fail "dump is not readable (pg_restore --list failed)"

mv "$TMP" "$FINAL"

size="$(du -h "$FINAL" | cut -f1)"
warnings="$(grep -ci 'warning' "$ERR" || true)"   # TimescaleDB dumps print harmless circular-FK warnings
if [ "$warnings" -gt 0 ]; then
    log "WARN $(grep -i -A2 'warning' "$ERR" | head -c 400 | tr '\n' ' ')"
fi

# Off-site copy: every local dump the remote doesn't have yet (so a failed
# upload is retried by the next run). Dumps never change after they're
# written, so --size-only is enough, and --immutable refuses to overwrite a
# differing object instead of replacing it. The bucket's credentials can't
# delete or overwrite; the bucket's own lifecycle rule handles retention.
offsite="off"
if [ -n "$REMOTE" ]; then
    : > "$ERR"
    if rclone copy "$BACKUP_DIR" "$REMOTE" --include 'zleek-*.dump' \
            --size-only --immutable --s3-no-check-bucket \
            --retries 3 --low-level-retries 10 --stats 0 2>> "$ERR"; then
        offsite="ok"
    else
        offsite="FAIL"
    fi
fi

# Retention: keep the newest $KEEP dumps (by the UTC timestamp in the name),
# but delete an older one ONLY if the remote holds it at the same size, so a
# dump that never reached off-site is never deleted (until 2026-11-16 retention
# ran before the copy and deleted 2026-11-09's dump, which off-site lacked).
# Only reached after a good dump and a successful copy.
not_offsite=0
if [ "$offsite" = "ok" ]; then
    remote_list="$(rclone lsl "$REMOTE" --include 'zleek-*.dump' 2>> "$ERR" | awk '{ print $NF, $1 }')" || remote_list=""
    for old in $(ls -1 "$BACKUP_DIR"/zleek-*.dump | sort -r | tail -n +"$((KEEP + 1))"); do
        if printf '%s\n' "$remote_list" | grep -qxF "$(basename "$old") $(stat -c %s "$old" 2>/dev/null || stat -f %z "$old")"; then
            rm -f -- "$old"
        else
            not_offsite=$((not_offsite + 1))
        fi
    done
fi
kept="$(ls -1 "$BACKUP_DIR"/zleek-*.dump | wc -l | tr -d ' ')"
free="$(df -Ph "$BACKUP_DIR" | awk 'NR == 2 { print $4 }')"
[ "$not_offsite" -gt 0 ] && log "WARN $not_offsite dump(s) past retention kept: not found off-site at the same size"
details="$(basename "$FINAL") size=$size seconds=$(( $(date +%s) - start )) kept=$kept disk_free=$free pg_dump_warnings=$warnings offsite=$offsite"
# A run only counts as OK when the dump is good AND it reached off-site AND
# it can alert: anything less is a FAIL line, a failed healthcheck ping, a
# push, and exit 1.
if [ -z "$REMOTE" ]; then
    : > "$ERR"; fail "no off-site copy: ZLEEK_BACKUP_REMOTE is not set (local dump kept: $details)"
fi
if [ "$offsite" = "FAIL" ]; then
    fail "off-site copy to $REMOTE failed (local dump kept: $details)"
fi
if [ -z "$HEALTHCHECK_URL" ]; then
    : > "$ERR"; fail "ZLEEK_BACKUP_HEALTHCHECK_URL is not set, so a failing backup can't alert ($details)"
fi
log "OK $details"
hc "" "OK $details"
