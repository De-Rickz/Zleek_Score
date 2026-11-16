#!/usr/bin/env bash
# Run every worker test file that works offline, one Python process each (the
# files are plain scripts with a __main__ runner). Used by CI; works locally too:
#   apps/worker/tests/run_offline.sh            # from the repo root
# ZLEEK_OFFLINE=1 is set for the run: tests with a live part (a database, the
# Data API) run only their offline part and print a "SKIPPED (...)" line,
# which is listed at the end (and as a GitHub notice in CI), never dropped.
set -uo pipefail
cd "$(dirname "$0")/.."
PYTHON="${ZLEEK_TEST_PYTHON:-python}"
[ -x .venv/bin/python ] && [ -z "${ZLEEK_TEST_PYTHON:-}" ] && PYTHON=.venv/bin/python
export ZLEEK_OFFLINE=1 PYTHONUNBUFFERED=1
failed=() skipped=()
for t in tests/test_*.py; do
    start=$(date +%s)
    out="$("$PYTHON" "$t" 2>&1)"; rc=$?
    secs=$(( $(date +%s) - start ))
    while IFS= read -r line; do skipped+=("$(basename "$t"): ${line#SKIPPED }"); done < <(printf '%s\n' "$out" | grep '^SKIPPED' || true)
    if [ "$rc" = 0 ]; then
        printf 'ok    %-34s %3ss  (%s ok)\n' "$(basename "$t")" "$secs" "$(printf '%s\n' "$out" | grep -c '^ok' || true)"
    else
        printf 'FAIL  %-34s %3ss\n' "$(basename "$t")" "$secs"
        printf '%s\n' "$out" | tail -n 30 | sed 's/^/      /'
        failed+=("$(basename "$t")")
    fi
done
echo
for s in "${skipped[@]+"${skipped[@]}"}"; do
    echo "SKIPPED $s"
    [ -n "${GITHUB_ACTIONS:-}" ] && echo "::notice title=Skipped in CI::$s"
done
if [ "${#failed[@]}" -gt 0 ]; then echo "FAILED: ${failed[*]}"; exit 1; fi
echo "all offline worker tests passed (${#skipped[@]} skipped part(s) listed above)"
