#!/usr/bin/env bash
# zleek-docker-prune.sh: monthly docker cleanup on the production server.
#   docker builder prune -f  build cache not used by any image (it had reached
#                            5.7 GB by 2026-11-04 from web and worker rebuilds)
#   docker image prune -f    dangling images only (untagged, left behind by
#                            rebuilds). Tagged images, including any kept for
#                            rollback, are never touched: no -a.
# Running containers, volumes and the database are not affected.
# One log line per run with the disk use before and after.
#
# Installed as ~/bin/zleek-docker-prune.sh, run by cron monthly
# (docs/storage-plan.md).
set -euo pipefail
LOG="${ZLEEK_STATE_DIR:-$HOME/.zleek}/docker-prune.log"
mkdir -p "$(dirname "$LOG")"
used() { df -Ph / | awk 'NR == 2 { print $3 " (" $5 ")" }'; }

before="$(used)"
cache="$(docker builder prune -f 2>&1 | tail -n 1)"
images="$(docker image prune -f 2>&1 | tail -n 1)"
printf '%s OK before=%s after=%s builder: %s | images: %s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$before" "$(used)" "$cache" "$images" >> "$LOG"
