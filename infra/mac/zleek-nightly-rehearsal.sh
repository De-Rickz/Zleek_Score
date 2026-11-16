#!/usr/bin/env bash
# zleek-nightly-rehearsal.sh: rehearse the trades_archive migration on last
# night's production dump, on the Mac (launchd job com.zleek.rehearsal, 04:30
# daily; launchd runs a missed time on wake). Installed as
# ~/bin/zleek-nightly-rehearsal.sh; source in infra/mac/.
#
# READ-ONLY on the server: only `ls`/`stat` over ssh and `scp` from it. Nothing
# there is written, changed or deleted.
#
#  1. ssh: newest backups/zleek-db/zleek-*.dump and its size.
#  2. scp it to ~/zleek-rehearsal/ (skipped if already there at the same size);
#     the local size must match the server's.
#  3. run infra/sql/migrations/rehearse_trades_archive.sh on it, from the repo
#     at $ZLEEK_REHEARSAL_REF (exported with git archive, so the working tree's
#     state doesn't matter); full output to ~/zleek-rehearsal/reports/ and
#     ~/Desktop/rehearsal-YYYY-MM-DD.txt.
#  4. ntfy push (the heartbeat's topic, from ~/.zleek/ntfy.env): a one-line
#     summary, or the failure reason.
#  5. delete downloaded dumps older than 2 days.
#
# Settings (environment): ZLEEK_SSH (ubuntu@79.72.91.54), ZLEEK_REPO
# (~/Projects/Zleek_Score-main), ZLEEK_REHEARSAL_REF (main if it has the
# rehearsal script, else phase4/storage-trades-archive), ZLEEK_REHEARSAL_DIR
# (~/zleek-rehearsal), ZLEEK_NTFY_ENV (~/.zleek/ntfy.env).
set -uo pipefail
export PATH="$PATH:/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"   # launchd starts with a minimal PATH; docker is in /usr/local/bin
SSH_HOST="${ZLEEK_SSH:-ubuntu@79.72.91.54}"
REPO="${ZLEEK_REPO:-$HOME/Projects/Zleek_Score-main}"
DIR="${ZLEEK_REHEARSAL_DIR:-$HOME/zleek-rehearsal}"
export ZLEEK_NTFY_ENV="${ZLEEK_NTFY_ENV:-$HOME/.zleek/ntfy.env}"
TODAY="$(date -u +%Y-%m-%d)"
DESK="${ZLEEK_REHEARSAL_OUT:-$HOME/Desktop/rehearsal-$TODAY.txt}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=20)
mkdir -p "$DIR/reports"
LOG="$DIR/rehearsal.log"
# The report is written here, then copied to the Desktop (macOS privacy
# controls can stop a background job writing to ~/Desktop; then the push
# names this path instead).
OUT="$DIR/reports/rehearsal-$TODAY.txt"
WHERE="$OUT"
publish() { cp "$OUT" "$DESK" 2>/dev/null && WHERE="$DESK"; }
log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../ops/zleek-ntfy.sh
. "${ZLEEK_NTFY_HELPER:-$SCRIPT_DIR/zleek-ntfy.sh}" 2>/dev/null \
    || zleek_ntfy() { echo "zleek_ntfy: helper not found; not sent: $3" >&2; return 1; }

fail() {
    [ -f "$OUT" ] && publish
    log "FAIL $1"
    zleek_ntfy high warning "Rehearsal FAILED: $1" > /dev/null 2>&1 || log "WARN ntfy push failed"
    exit 1
}

# One run at a time (macOS has no flock): a lock directory.
LOCK="$DIR/.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
    if [ -n "$(find "$LOCK" -maxdepth 0 -mmin +360 2>/dev/null)" ]; then rmdir "$LOCK"; mkdir "$LOCK"   # stale (> 6 h)
    else log "SKIP another rehearsal is still running"; exit 0; fi
fi
TMP="$(mktemp -d "$DIR/run.XXXXXX")"
trap 'rm -rf "$TMP"; rmdir "$LOCK" 2>/dev/null' EXIT
start=$(date +%s)

# 1. newest dump on the server (read-only: ls + stat)
remote="$(ssh "${SSH_OPTS[@]}" "$SSH_HOST" \
    'f=$(ls -1 backups/zleek-db/zleek-*.dump 2>/dev/null | sort | tail -n 1); [ -n "$f" ] && printf "%s %s\n" "$f" "$(stat -c %s "$f")"' 2>&1)" \
    || fail "ssh to $SSH_HOST failed: $(echo "$remote" | tail -n 1)"
read -r RPATH RSIZE <<< "$remote"
[ -n "${RPATH:-}" ] && [ -n "${RSIZE:-}" ] || fail "no dump found in backups/zleek-db on the server"
NAME="$(basename "$RPATH")"
stamp="${NAME#zleek-}"; stamp="${stamp%.dump}"                     # YYYYMMDDTHHMMSSZ
made="$(date -u -j -f %Y%m%dT%H%M%SZ "$stamp" +%s 2>/dev/null || date -u +%s)"
age_h=$(( ( $(date -u +%s) - made ) / 3600 ))
log "newest dump on the server: $NAME ($RSIZE bytes)"

# 2. download (read-only on the server), and check the size
LOCAL="$DIR/$NAME"
if [ ! -f "$LOCAL" ] || [ "$(stat -f %z "$LOCAL")" != "$RSIZE" ]; then
    rm -f "$LOCAL.part"
    scp -p -q "${SSH_OPTS[@]}" "$SSH_HOST:$RPATH" "$LOCAL.part" || fail "scp of $NAME failed"
    mv "$LOCAL.part" "$LOCAL"
fi
LSIZE="$(stat -f %z "$LOCAL")"
[ "$LSIZE" = "$RSIZE" ] || fail "size mismatch for $NAME: local $LSIZE, server $RSIZE"

# 3. the rehearsal, from the repo at a fixed ref
docker info > /dev/null 2>&1 || { open -a Docker 2>/dev/null; for _ in $(seq 36); do sleep 5; docker info > /dev/null 2>&1 && break; done; }
docker info > /dev/null 2>&1 || fail "Docker is not running (Docker Desktop did not start)"
REF="${ZLEEK_REHEARSAL_REF:-}"
if [ -z "$REF" ]; then
    if git -C "$REPO" cat-file -e main:infra/sql/migrations/rehearse_trades_archive.sh 2>/dev/null; then REF=main
    else REF=phase4/storage-trades-archive; fi
fi
git -C "$REPO" archive "$REF" infra/sql/migrations | tar -x -C "$TMP" \
    || fail "could not export infra/sql/migrations from $REPO at $REF"
COMMIT="$(git -C "$REPO" rev-parse --short "$REF")"
free_gb=$(( $(df -k "$DIR" | awk 'NR == 2 { print $4 }') / 1048576 ))
[ "$free_gb" -ge 25 ] || fail "only ${free_gb} GB free on the Mac (need ~25 GB)"

{
    echo "nightly rehearsal $TODAY: dump $NAME ($RSIZE bytes, ~${age_h} h old), repo $REF @ $COMMIT"
    echo
} > "$OUT"
(cd "$TMP" && infra/sql/migrations/rehearse_trades_archive.sh "$LOCAL") >> "$OUT" 2>&1
rc=$?
mins=$(( ( $(date +%s) - start ) / 60 ))

# 4. one-line summary
backlog_s="$(grep -m1 '^backlog move' "$OUT" | awk '{print $(NF-7)}')"
ratio="$(grep -m1 '^   compression:' "$OUT" | sed -n 's/.*(\([0-9.]*x\)).*/\1/p')"
dumps="$(grep -m1 'dump after:' "$OUT" | sed -n 's/.*dump after: \([0-9]*\) MB (before: \([0-9]*\) MB).*/\2->\1 MB/p')"
peak="$(grep -m1 'peak data dir during the move' "$OUT" | sed -n 's/.*: \([0-9]*\) MB/\1 MB/p')"
fails="$(grep -c '^FAIL ' "$OUT" || true)"
stale=""; [ "$age_h" -gt 30 ] && stale=" (dump is ${age_h} h old: did last night's backup run?)"
publish
if [ "$rc" = 0 ] && grep -q '^REHEARSAL PASSED' "$OUT"; then
    summary="Rehearsal PASSED on $NAME in ${mins} min: backlog move ${backlog_s:-?}, compression ${ratio:-?}, peak ${peak:-?}, dump ${dumps:-?}$stale. $WHERE"
    log "OK $summary"
    zleek_ntfy default white_check_mark "$summary" > /dev/null 2>&1 || log "WARN ntfy push failed"
else
    first="$(grep -m1 -E '^FAIL |error|failed' "$OUT" | cut -c1-160)"
    fail "on $NAME after ${mins} min, exit $rc, $fails FAIL check(s): ${first:-see the report}$stale. $WHERE"
fi

# 5. downloaded dumps older than 2 days
find "$DIR" -maxdepth 1 -name 'zleek-*.dump' -mmin +2880 -print -delete | sed 's/^/deleted old dump: /' >> "$LOG"
exit 0
