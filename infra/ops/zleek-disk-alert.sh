#!/usr/bin/env bash
# zleek-disk-alert.sh: push an ntfy alert (the heartbeat's topic) when disk use
# on / crosses 75% (warning) or 90% (critical), once per crossing, not every
# run. The level is remembered in $STATE_DIR/disk-alert.state; going back below
# 75% sends one "back to normal" note and re-arms both alerts. If a push fails,
# the state isn't updated, so the next run tries again.
#
# Installed as ~/bin/zleek-disk-alert.sh, run by cron every 15 minutes
# (docs/storage-plan.md). Read-only apart from its own state and log files.
set -euo pipefail

WARN_PCT="${ZLEEK_DISK_WARN_PCT:-75}"
CRIT_PCT="${ZLEEK_DISK_CRIT_PCT:-90}"
MOUNT="${ZLEEK_DISK_MOUNT:-/}"
STATE_DIR="${ZLEEK_STATE_DIR:-$HOME/.zleek}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=zleek-ntfy.sh
. "${ZLEEK_NTFY_HELPER:-$SCRIPT_DIR/zleek-ntfy.sh}"

mkdir -p "$STATE_DIR"
STATE="$STATE_DIR/disk-alert.state"
LOG="$STATE_DIR/disk-alert.log"
log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$LOG"; }

read -r size used avail pct < <(df -Ph "$MOUNT" | awk 'NR == 2 { gsub("%", "", $5); print $2, $3, $4, $5 }')
level=ok
[ "$pct" -ge "$WARN_PCT" ] && level=warn
[ "$pct" -ge "$CRIT_PCT" ] && level=crit
prev="$(cat "$STATE" 2>/dev/null || echo ok)"
rank() { case "$1" in crit) echo 2 ;; warn) echo 1 ;; *) echo 0 ;; esac; }

detail="disk $MOUNT ${pct}% used ($used of $size, $avail free) on $(hostname)"
if [ "$(rank "$level")" -gt "$(rank "$prev")" ]; then
    if [ "$level" = crit ]; then
        msg="CRITICAL: $detail. Over ${CRIT_PCT}%: free space now (docker builder prune -f, old dumps), or Postgres may stop writing."
        prio=urgent; tags=rotating_light
    else
        msg="Warning: $detail. Over ${WARN_PCT}%: check apps/worker/scripts/storage-report.sql and docker system df."
        prio=high; tags=warning
    fi
    if zleek_ntfy "$prio" "$tags" "$msg"; then echo "$level" > "$STATE"; log "ALERT $level $detail"
    else log "FAIL could not send $level alert: $detail"; exit 1; fi
elif [ "$level" = ok ] && [ "$prev" != ok ]; then
    if zleek_ntfy default white_check_mark "Back to normal: $detail."; then echo ok > "$STATE"; log "RECOVERED $detail"
    else log "FAIL could not send recovery note: $detail"; exit 1; fi
elif [ "$(rank "$level")" -lt "$(rank "$prev")" ]; then
    echo "$level" > "$STATE"   # crit -> warn: re-arm the critical alert quietly
    log "DOWN to $level $detail"
fi
