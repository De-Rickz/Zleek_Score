#!/usr/bin/env bash
# Rehearsal of the trades_archive migration (docs/trades-archive-design.md) on a
# restored PRODUCTION dump, on the Mac, never on production: a throwaway
# TimescaleDB 2.31.0 container (production's version, no volume, port on
# 127.0.0.1 only) that is removed at the end.
#
# Runs every production step in order and reports, per step, how long it took
# and the database's size; peak disk of the data directory; whether the data
# and every reader's results are identical before and after; the dedupe on real
# archived trades (Data API and websocket timestamps, compressed chunks); a
# simulated nightly move and its repeat; VACUUM FULL trades (time, space back);
# and the dump size before and after.
#
#   infra/sql/migrations/rehearse_trades_archive.sh ~/Desktop/zleek-<stamp>.dump
# Output: the console, and the same report in ./rehearsal-<stamp>.txt.
set -euo pipefail
DUMP="${1:?usage: $0 <production dump (.dump, pg_dump -Fc)>}"
[ -r "$DUMP" ] || { echo "cannot read $DUMP" >&2; exit 1; }
cd "$(dirname "$0")/../../.."
IMAGE="${ZLEEK_TEST_IMAGE:-timescale/timescaledb:2.31.0-pg16}"
NAME="zleek_rehearsal_$$"
PORT="${ZLEEK_TEST_PORT:-55434}"
REPORT="rehearsal-$(date -u +%Y%m%dT%H%MZ).txt"
DATA=/var/lib/postgresql/data

docker run -d --rm --name "$NAME" -p "127.0.0.1:$PORT:5432" -e POSTGRES_PASSWORD=test \
    --shm-size=1g "$IMAGE" -c maintenance_work_mem=512MB -c max_wal_size=4GB -c client_min_messages=warning > /dev/null
SAMPLER=""
cleanup() {
    [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null || true
    docker rm -f "$NAME" > /dev/null 2>&1 || true
    rm -f "$REPORT.stop" "$REPORT.samples"
}
trap cleanup EXIT
for _ in $(seq 60); do docker exec "$NAME" pg_isready -U postgres -q 2>/dev/null && break; sleep 1; done
sleep 3

q() { docker exec -i -e PGOPTIONS='-c client_min_messages=warning' "$NAME" psql -U postgres -d restored -X -q -v ON_ERROR_STOP=1 -tA "$@"; }
say() { printf '%s\n' "$*" | tee -a "$REPORT"; }
datadir_mb() { docker exec "$NAME" du -sm "$DATA" | cut -f1; }
db_mb() { q -c "SELECT pg_database_size('restored') / 1048576"; }
step() {  # step LABEL COMMAND...: run, time, report sizes
    local label="$1"; shift
    local t0; t0=$(date +%s)
    "$@"
    say "$(printf '%-34s %6ss   database %6s MB   data dir %6s MB' "$label" "$(( $(date +%s) - t0 ))" "$(db_mb)" "$(datadir_mb)")"
}
# Peak data-directory size while a step runs: sampled every 5 s until told to stop.
peak_start() {
    : > "$REPORT.samples"; rm -f "$REPORT.stop"
    ( while [ ! -e "$REPORT.stop" ]; do datadir_mb >> "$REPORT.samples" 2>/dev/null || true; sleep 5; done ) > /dev/null 2>&1 &
    SAMPLER=$!
}
peak_stop() { touch "$REPORT.stop"; wait "$SAMPLER" 2>/dev/null || true; SAMPLER=""; datadir_mb >> "$REPORT.samples"; sort -n "$REPORT.samples" | tail -n 1; }
qq() { q "$@" > /dev/null; }
check() {  # check NAME EXPECTED GOT
    if [ "$2" = "$3" ]; then say "ok   $1"; else say "FAIL $1: expected '$2', got '$3'"; FAILED=1; fi
}
FAILED=0

say "trades_archive rehearsal, $(date -u '+%Y-%m-%d %H:%M UTC'), dump $(basename "$DUMP") ($(du -h "$DUMP" | cut -f1)), image $IMAGE"
say ""

# --- 1. restore, as zleek-restore-test.sh does ---
restore() {
    docker exec "$NAME" psql -U postgres -X -q -c 'CREATE DATABASE restored' > /dev/null
    q -c 'CREATE EXTENSION IF NOT EXISTS timescaledb' -c 'SELECT timescaledb_pre_restore()' > /dev/null
    docker cp "$DUMP" "$NAME:/tmp/prod.dump" > /dev/null
    docker exec "$NAME" pg_restore -U postgres -d restored --no-owner --no-acl /tmp/prod.dump 2> "$REPORT.restore.err" \
        || { echo "pg_restore failed:"; grep -i error "$REPORT.restore.err" | head; exit 1; }
    q -c 'SELECT timescaledb_post_restore()' > /dev/null
    docker exec "$NAME" rm -f /tmp/prod.dump
    q -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'zleek_web') THEN CREATE ROLE zleek_web; END IF; END \$\$" > /dev/null
    q -c 'VACUUM ANALYZE' > /dev/null
}
step "restore (+ VACUUM ANALYZE)" restore
say "   trades: $(q -c "SELECT count(*) || ' rows, ' || min(ts)::date || ' .. ' || max(ts)::date || ', ' || pg_size_pretty(pg_total_relation_size('trades')) FROM trades")"

# --- 2. baseline: what every reader sees before ---
DAY="$(q -c "SELECT (date_trunc('day', min(ts)) + interval '1 day')::date FROM trades")"   # oldest full day
SAMPLE_MARKETS="$(q -c "(SELECT market_id FROM trades WHERE ts >= '$DAY' AND ts < '$DAY'::date + 1 GROUP BY 1 ORDER BY count(*) DESC LIMIT 10)
                        UNION (SELECT market_id FROM trades WHERE ts > now() - interval '1 day' GROUP BY 1 ORDER BY count(*) DESC LIMIT 10)" | tr '\n' ' ')"
fingerprint() {  # fingerprint TABLE: one line that changes if any reader's answer changes
    local t="$1" m out=""
    out+="rows=$(q -c "SELECT count(*) FROM $t") "
    out+="distinct_tx=$(q -c "SELECT count(DISTINCT tx_hash) FROM $t") "
    out+="ids=$(q -c "SELECT count(DISTINCT id) FROM $t") "
    out+="sum=$(q -c "SELECT sum(hashtextextended(id::text || coalesce(tx_hash, '') || ts::text || price::text || size_usd::text || coalesce(raw::text, ''), 0)::numeric) FROM $t") "
    out+="last_per_market=$(q -c "SELECT md5(string_agg(market_id || '@' || m::text, ',' ORDER BY market_id)) FROM (SELECT market_id, max(ts) m FROM $t GROUP BY 1) s") "
    out+="bars_$DAY=$(q -c "SELECT md5(string_agg(concat_ws('|', market_id, b, o, h, l, c, v, n), ',' ORDER BY market_id, b)) FROM (
             SELECT market_id, date_trunc('minute', ts) b, (array_agg(price ORDER BY ts, id))[1] o, max(price) h, min(price) l,
                    (array_agg(price ORDER BY ts DESC, id DESC))[1] c, sum(size_usd) v, count(*) n
             FROM $t WHERE ts >= '$DAY' AND ts < '$DAY'::date + 1 GROUP BY 1, 2) s") "
    for m in $SAMPLE_MARKETS; do
        # The market's newest 50 trades, plus any that tie with the 50th (trades
        # sharing a second come back in plan-dependent order), ordered by
        # (ts, id) only inside the aggregate. Deliberately NOT
        # "ORDER BY ts DESC, id DESC LIMIT 50" on trades_all: that pattern hits
        # a TimescaleDB 2.31.0 bug ("retrieved too many tuples in a bounded
        # sort") once trades_archive is analyzed (docs/trades-archive-design.md).
        out+="$m=$(q -c "SELECT md5(string_agg(id::text || ':' || ts::text || ':' || price::text || ':' || size_usd::text, ',' ORDER BY ts DESC, id DESC))
             FROM $t WHERE market_id = '$m'
               AND ts >= (SELECT min(ts) FROM (SELECT ts FROM $t WHERE market_id = '$m' ORDER BY ts DESC LIMIT 50) x)") "
    done
    printf '%s' "$out"
}
t0=$(date +%s); BEFORE="$(fingerprint trades)"
say "$(printf '%-34s %6ss' "baseline fingerprint (trades)" "$(( $(date +%s) - t0 ))")"
DUMP_BEFORE="$(docker exec "$NAME" sh -c "pg_dump -U postgres -d restored -Fc -Z 6 2>/dev/null | wc -c")"
say "   dump before: $(( DUMP_BEFORE / 1048576 )) MB (pg_dump -Fc -Z 6, as the nightly backup)"
time_query() {  # time_query LABEL SQL: execution time of the reader's query, ms
    say "   $(printf '%-44s' "$1") $(q -c "EXPLAIN (ANALYZE, FORMAT JSON) $2" | sed -n 's/.*"Execution Time": \([0-9.]*\).*/\1/p') ms"
}
BUSY="$(echo "$SAMPLE_MARKETS" | awk '{print $NF}')"
DASH_BEFORE="SELECT s.market_id, lt.ts FROM signals_latest s LEFT JOIN LATERAL (SELECT max(ts) AS ts FROM trades t WHERE t.market_id = s.market_id) lt ON true"
time_query "dashboard newest trade (old query)" "$DASH_BEFORE"
time_query "recent 50 trades, busy market (trades)" "SELECT ts, price FROM trades WHERE market_id = '$BUSY' ORDER BY ts DESC LIMIT 50"

# --- 3. migration (creates objects, moves nothing) ---
step "migration trades_archive.sql" sh -c "docker exec -i -e PGOPTIONS='-c client_min_messages=warning' $NAME psql -U postgres -d restored -X -q -v ON_ERROR_STOP=1 -1 < infra/sql/migrations/trades_archive.sql > /dev/null"

# --- 4. backlog move, one day per transaction ---
peak_start
backlog() {
    docker exec -e PGOPTIONS='-c client_min_messages=notice' "$NAME" \
        psql -U postgres -d restored -X -v ON_ERROR_STOP=1 -c 'CALL archive_trades_backlog()' > "$REPORT.days.raw" 2>&1 \
        || { echo "backlog move failed:"; cat "$REPORT.days.raw"; return 1; }
    grep -o 'archive_trades_backlog: .*' "$REPORT.days.raw" > "$REPORT.days" || true
}
step "backlog move (1 day per txn)" backlog
say "   peak data dir during the move: $(peak_stop) MB"
sed 's/^/   /' "$REPORT.days" | tee -a "$REPORT" > /dev/null

# --- 5. compress the archive ---
peak_start
step "compress archive chunks" qq -c "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('trades_archive', older_than => interval '5 days') c"
say "   peak data dir during compression: $(peak_stop) MB"
say "   archive: $(q -c "SELECT count(*) || ' chunks, ' || count(*) FILTER (WHERE is_compressed) || ' compressed' FROM timescaledb_information.chunks WHERE hypertable_name = 'trades_archive'"), $(q -c "SELECT pg_size_pretty(hypertable_size('trades_archive'))"); trades now $(q -c "SELECT pg_size_pretty(pg_total_relation_size('trades'))")"
say "   compression: $(q -c "SELECT pg_size_pretty(sum(before_compression_total_bytes)) || ' -> ' || pg_size_pretty(sum(after_compression_total_bytes)) || ' (' || round(sum(before_compression_total_bytes)::numeric / nullif(sum(after_compression_total_bytes), 0), 1) || 'x)' FROM chunk_compression_stats('trades_archive')")"

# --- 6. verify: identical answers through trades_all ---
say ""
qq -c "ANALYZE trades_archive"   # as autovacuum will on production: the plans below then match it
AFTER="$(fingerprint trades_all)"
check "every reader's answer identical (trades -> trades_all)" "$BEFORE" "$AFTER"
check "no row older than the cutoff left in trades" "0" "$(q -c "SELECT count(*) FROM trades WHERE ts < archive_cutoff()")"
check "no tx_hash in both tables" "0" "$(q -c "SELECT count(*) FROM trades t JOIN trades_archive a USING (tx_hash)")"
check "web newest-trade query = max over trades_all" "0" "$(q -c "SELECT count(*) FROM signals_latest s
    WHERE coalesce((SELECT max(ts) FROM trades t WHERE t.market_id = s.market_id), (SELECT max(ts) FROM trades_archive a WHERE a.market_id = s.market_id))
          IS DISTINCT FROM (SELECT max(ts) FROM trades_all x WHERE x.market_id = s.market_id)")"
time_query "dashboard newest trade (new query)" "SELECT s.market_id, coalesce((SELECT max(ts) FROM trades t WHERE t.market_id = s.market_id), (SELECT max(ts) FROM trades_archive a WHERE a.market_id = s.market_id)) FROM signals_latest s"
time_query "recent 50 trades, busy market (trades_all)" "SELECT ts, price FROM trades_all WHERE market_id = '$BUSY' ORDER BY ts DESC LIMIT 50"
OLDM="$(echo "$SAMPLE_MARKETS" | awk '{print $1}')"
time_query "recent 50 trades, archived market" "SELECT ts, price FROM trades_all WHERE market_id = '$OLDM' ORDER BY ts DESC LIMIT 50"
check "web recent-trades query (ORDER BY ts DESC LIMIT 50) works on every sample market" "0" "$(for m in $SAMPLE_MARKETS; do q -c "SELECT count(*) FROM (SELECT ts, price, size_usd, side FROM trades_all WHERE market_id = '$m' ORDER BY ts DESC LIMIT 50) s" > /dev/null 2>&1 || echo x; done | grep -c x)"
time_query "bars rebuild select, archived day" "SELECT market_id, date_trunc('minute', ts), count(*) FROM trades_all WHERE ts >= '$DAY' AND ts < '$DAY'::date + 1 GROUP BY 1, 2"

# --- 7. dedupe on real archived trades (compressed chunks) ---
# an archived trade with a tx_hash, in a compressed chunk
read -r A_MKT A_TS A_TX <<< "$(q -F ' ' -c "SELECT market_id, replace(ts::text, ' ', 'T'), tx_hash FROM trades_archive
    WHERE ts < archive_cutoff() - interval '2 days' AND tx_hash IS NOT NULL ORDER BY ts DESC LIMIT 1")"
say "   dedupe sample: market $A_MKT, $A_TS, chunk compressed: $(q -c "SELECT is_compressed FROM timescaledb_information.chunks WHERE hypertable_name = 'trades_archive' AND range_start <= '$A_TS' AND range_end > '$A_TS'")"
store() { q -c "SELECT zleek_store_trade('$A_MKT', 'a', '$1'::timestamptz, 0.5, 1, 'BUY', null, null, '$2', '{}'::jsonb, null, null, null)"; }
check "archived trade re-written, same ts: refused" "duplicate" "$(store "$A_TS" "$A_TX")"
check "archived trade re-written, ts -4 s: refused" "duplicate" "$(q -c "SELECT zleek_store_trade('$A_MKT', 'a', '$A_TS'::timestamptz - interval '4 seconds', 0.5, 1, 'BUY', null, null, '$A_TX', '{}'::jsonb, null, null, null)")"
check "new old trade: into the compressed archive" "archive" "$(store "$A_TS" "0xrehearsal-new")"
check "  and refused the second time" "duplicate" "$(store "$A_TS" "0xrehearsal-new")"
check "  read back once through trades_all" "1" "$(q -c "SELECT count(*) FROM trades_all WHERE tx_hash = '0xrehearsal-new'")"
q -c "DELETE FROM trades_archive WHERE tx_hash = '0xrehearsal-new'" > /dev/null

# --- 8. the nightly move (simulated next night), then again ---
say ""
t0=$(date +%s); moved="$(q -c "SELECT archive_trades_move(archive_cutoff() + interval '1 day')")"
say "$(printf '%-34s %6ss   %s rows moved (one day: the nightly job)' "nightly move (next night's day)" "$(( $(date +%s) - t0 ))" "$moved")"
check "nightly move again moves nothing" "0" "$(q -c "SELECT archive_trades_move(archive_cutoff() + interval '1 day')")"
check "total unchanged after the nightly move" "$(echo "$BEFORE" | sed -n 's/^rows=\([0-9]*\).*/\1/p')" "$(q -c "SELECT count(*) FROM trades_all")"
say "   heartbeat archive queries: $(q -c "\\timing on" -c "SELECT count(*) FROM trades t WHERE t.ts >= archive_cutoff() - interval '1 hour' AND t.ts < archive_cutoff() + interval '1 hour' AND EXISTS (SELECT 1 FROM trades_archive a WHERE a.tx_hash = t.tx_hash AND a.ts >= archive_cutoff() - interval '1 hour')" -c "SELECT count(*) - count(DISTINCT tx_hash) FROM trades_archive WHERE ts >= archive_cutoff() - interval '1 day' AND tx_hash IS NOT NULL" | tr '\n' ' ')"

# --- 9. space back to the OS ---
say ""
step "VACUUM FULL trades (exclusive lock)" q -c "VACUUM FULL trades"
say "   trades after VACUUM FULL: $(q -c "SELECT pg_size_pretty(pg_total_relation_size('trades'))")"

# --- 10. dumps ---
DUMP_AFTER="$(docker exec "$NAME" sh -c "pg_dump -U postgres -d restored -Fc -Z 6 2>/dev/null | wc -c")"
say "   dump after: $(( DUMP_AFTER / 1048576 )) MB (before: $(( DUMP_BEFORE / 1048576 )) MB)"

# --- 11. enable (policy + job) ---
step "trades_archive_enable.sql" sh -c "docker exec -i -e PGOPTIONS='-c client_min_messages=warning' $NAME psql -U postgres -d restored -X -q -v ON_ERROR_STOP=1 < infra/sql/migrations/trades_archive_enable.sql > /dev/null"
check "nightly job scheduled" "1" "$(q -c "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'archive_trades_job'")"

rm -f "$REPORT.samples" "$REPORT.days" "$REPORT.days.raw" "$REPORT.restore.err" "$REPORT.stop"
say ""
[ "$FAILED" = 0 ] && say "REHEARSAL PASSED" || say "REHEARSAL FAILED (see FAIL lines)"
exit "$FAILED"
