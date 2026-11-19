#!/usr/bin/env bash
# Integration test for storage_retention.sql on a throwaway TimescaleDB 2.31.0
# (production's version). Never touches a real database: starts its own
# container (no volume; port on 127.0.0.1 only), loads infra/sql/schema.sql and
# domain_model.sql, seeds rows older and newer than each retention, runs the
# migration twice and then the policy jobs.
#
#   infra/sql/migrations/test_storage_retention.sh     # from the repo root
#
# Or against an EMPTY throwaway database that already runs (CI's service
# container): ZLEEK_TEST_DATABASE_URL=postgresql://postgres:...@host:port/db,
# a superuser, TimescaleDB available; psql on the PATH.
set -euo pipefail
cd "$(dirname "$0")/../../.."
IMAGE="${ZLEEK_TEST_IMAGE:-timescale/timescaledb:2.31.0-pg16}"
if [ -n "${ZLEEK_TEST_DATABASE_URL:-}" ]; then
    URL="$ZLEEK_TEST_DATABASE_URL"
    IMAGE="the database at ZLEEK_TEST_DATABASE_URL"
    q() { PGOPTIONS='-c client_min_messages=warning' psql "$URL" -X -q -v ON_ERROR_STOP=1 -tA "$@"; }
    [ "$(q -c "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")" = 0 ] \
        || { echo "ZLEEK_TEST_DATABASE_URL must point at an empty throwaway database" >&2; exit 1; }
else
    NAME="zleek_storage_retention_test_$$"
    PORT="${ZLEEK_TEST_PORT:-55434}"
    docker run -d --rm --name "$NAME" -p "127.0.0.1:$PORT:5432" -e POSTGRES_PASSWORD=test "$IMAGE" > /dev/null
    trap 'docker rm -f "$NAME" > /dev/null 2>&1 || true' EXIT
    for _ in $(seq 60); do docker exec "$NAME" pg_isready -U postgres -q 2>/dev/null && break; sleep 1; done
    sleep 2
    q() { docker exec -i -e PGOPTIONS='-c client_min_messages=warning' "$NAME" psql -U postgres -d postgres -X -q -v ON_ERROR_STOP=1 -tA "$@"; }
fi
pass=0
check() {  # check NAME EXPECTED SQL
    local got; got="$(q -c "$3")"
    if [ "$got" = "$2" ]; then pass=$((pass + 1)); echo "ok   $1"; else echo "FAIL $1: expected '$2', got '$got'"; exit 1; fi
}
timed() { local t0=$SECONDS; "$@"; echo "time $((SECONDS - t0)) s: $*" | cut -c1-100 >&2; }

q < infra/sql/schema.sql > /dev/null
q < infra/sql/migrations/domain_model.sql > /dev/null

# Seed: two markets; rows aged 1, 2, 5, 20, 40, 100 days (plus 400 for bars_1m),
# relative to one fixed anchor so the before/after digests compare like with like.
ANCHOR="$(q -c "SELECT date_trunc('hour', now())")"
q -v anchor="$ANCHOR" > /dev/null <<'SQL'
INSERT INTO markets(id, title) VALUES ('m1', 'one'), ('m2', 'two');
INSERT INTO ob_snapshots(market_id, ts, best_bid, best_ask, bid_depth_usd, ask_depth_usd, bids, asks)
SELECT m, :'anchor'::timestamptz - d * interval '1 day' - i * interval '5 seconds', 0.4, 0.6, 100, 100,
       '[{"p":0.4,"q":10}]', '[{"p":0.6,"q":10}]'
FROM unnest(ARRAY['m1','m2']) m, unnest(ARRAY[1,2,5,20,40,100]) d, generate_series(0, 49) i;
INSERT INTO signals_history(market_id, ts, tremor_score)
SELECT m, :'anchor'::timestamptz - d * interval '1 day' - i * interval '5 minutes', 10
FROM unnest(ARRAY['m1','m2']) m, unnest(ARRAY[1,2,5,20,40,100]) d, generate_series(0, 49) i;
INSERT INTO features_5m(market_id, bucket, price_z_score, ob_imbalance)
SELECT m, :'anchor'::timestamptz - d * interval '1 day' - i * interval '5 minutes', 0.5, 0.1
FROM unnest(ARRAY['m1','m2']) m, unnest(ARRAY[1,2,5,20,40,100]) d, generate_series(0, 49) i;
INSERT INTO bars_1m(market_id, bucket, open, high, low, close, volume_usd, trades_cnt)
SELECT m, :'anchor'::timestamptz - d * interval '1 day' - i * interval '1 minute', 0.5, 0.6, 0.4, 0.55, 10, 1
FROM unnest(ARRAY['m1','m2']) m, unnest(ARRAY[1,2,5,20,40,100,400]) d, generate_series(0, 49) i;
SQL

# What the dashboard and detail queries (apps/web/lib/queries.ts) see: the
# newest features row per market, and every row younger than 25 days (inside
# every retention) in each table. Digest before, compare after.
digest() {
    q -c "
      SELECT md5(string_agg(x, '|' ORDER BY x)) FROM (
        SELECT 'o' || market_id || ts::text || best_bid::text || bids::text AS x FROM ob_snapshots WHERE ts >= '$ANCHOR'::timestamptz - interval '25 days'
        UNION ALL SELECT 's' || market_id || ts::text || tremor_score::text FROM signals_history WHERE ts >= '$ANCHOR'::timestamptz - interval '25 days'
        UNION ALL SELECT 'f' || market_id || bucket::text || price_z_score::text || ob_imbalance::text FROM features_5m WHERE bucket >= '$ANCHOR'::timestamptz - interval '25 days'
        UNION ALL SELECT 'b' || market_id || bucket::text || close::text FROM bars_1m WHERE bucket >= '$ANCHOR'::timestamptz - interval '25 days'
        UNION ALL SELECT 'l' || (SELECT bucket::text || price_z_score::text || ob_imbalance::text FROM features_5m WHERE market_id = m ORDER BY bucket DESC LIMIT 1)
                   FROM unnest(ARRAY['m1','m2']) m
      ) s"
}
before="$(digest)"
# Rows inside every retention window; the scheduler may already have run a
# policy by the time we look, so older rows are checked after the jobs, below.
counts() { q -c "SELECT (SELECT count(*) FROM ob_snapshots WHERE ts >= '$ANCHOR'::timestamptz - interval '25 days'), (SELECT count(*) FROM signals_history WHERE ts >= '$ANCHOR'::timestamptz - interval '25 days'), (SELECT count(*) FROM features_5m WHERE bucket >= '$ANCHOR'::timestamptz - interval '25 days'), (SELECT count(*) FROM bars_1m WHERE bucket >= '$ANCHOR'::timestamptz - interval '25 days')"; }
rows_before="$(counts)"
check "tables start as plain tables except features_5m and bars_1m" "bars_1m,features_5m" \
  "SELECT string_agg(hypertable_name, ',' ORDER BY hypertable_name) FROM timescaledb_information.hypertables"

timed q -1 < infra/sql/migrations/storage_retention.sql > /dev/null
timed q -1 < infra/sql/migrations/storage_retention.sql > /dev/null      # idempotent
check "four hypertables" "4" "SELECT count(*) FROM timescaledb_information.hypertables"
check "migrate_data kept every row inside the retention windows" "$rows_before" "SELECT '$(counts)'"
check "new hypertables use 1-day chunks" "1 day,1 day,1 day" \
  "SELECT string_agg(d::text, ',') FROM (SELECT (SELECT time_interval FROM timescaledb_information.dimensions WHERE hypertable_name = t) d FROM unnest(ARRAY['ob_snapshots','signals_history','features_5m']) t) s"
check "four compression policies, after 3 days" "4|3 days" \
  "SELECT count(*) || '|' || min(config->>'compress_after') FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' HAVING count(DISTINCT config->>'compress_after') = 1"
check "retention policies: 30, 90, 30 days and none on bars_1m" "features_5m=30 days,ob_snapshots=30 days,signals_history=90 days" \
  "SELECT string_agg(hypertable_name || '=' || (config->>'drop_after'), ',' ORDER BY hypertable_name) FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention'"
check "digest of recent rows unchanged after migration" "$before" "SELECT '$(digest)'"

# Run the policies once (what the scheduler does). Compression first, then retention.
for t in ob_snapshots signals_history features_5m bars_1m; do
    job="$(q -c "SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = '$t'")"
    timed q -c "CALL run_job($job)" > /dev/null
done
for t in ob_snapshots signals_history features_5m; do
    job="$(q -c "SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention' AND hypertable_name = '$t'")"
    timed q -c "CALL run_job($job)" > /dev/null
done

# Old rows gone only where retention applies.
age() { echo "SELECT count(*) FROM $1 WHERE $2 < '$ANCHOR'::timestamptz - interval '$3 days' AND $2 >= '$ANCHOR'::timestamptz - interval '$4 days'"; }
check "ob_snapshots: nothing older than 30 days (the 40 and 100 day rows are gone)" "0" "$(age ob_snapshots ts 30 1000)"
check "ob_snapshots: the 20 day rows are kept" "100" "$(age ob_snapshots ts 19 21)"
check "signals_history: the 40 day rows are kept" "100" "$(age signals_history ts 39 41)"
check "signals_history: the 100 day rows are gone" "0" "$(age signals_history ts 99 101)"
check "features_5m: nothing older than 30 days" "0" "$(age features_5m bucket 30 1000)"
check "features_5m: the 20 day rows are kept" "100" "$(age features_5m bucket 19 21)"
check "bars_1m untouched (every row, including 400 days old)" "700" "SELECT count(*) FROM bars_1m"

# Compression.
for t in ob_snapshots signals_history features_5m bars_1m; do
    check "$t: old chunks compressed" "t" \
      "SELECT count(*) FILTER (WHERE is_compressed) > 0 FROM timescaledb_information.chunks WHERE hypertable_name = '$t'"
    check "$t: the chunk holding today's data is not" "f" \
      "SELECT is_compressed FROM timescaledb_information.chunks WHERE hypertable_name = '$t' ORDER BY range_end DESC LIMIT 1"
done

# The same recent rows come back from compressed and uncompressed chunks alike.
after="$(digest)"
check "digest of recent rows unchanged after compression and retention" "$before" "SELECT '$after'"
check "a repair can still UPDATE a compressed row" "1" \
  "WITH u AS (UPDATE features_5m SET price_z_score = 0.5 WHERE market_id = 'm1' AND bucket = '$ANCHOR'::timestamptz - interval '20 days' RETURNING 1) SELECT count(*) FROM u"
check "an insert after the migration still dedupes on (market_id, ts)" "0" \
  "WITH i AS (INSERT INTO ob_snapshots(market_id, ts, best_bid) VALUES ('m1', '$ANCHOR'::timestamptz - interval '1 day', 0.4) ON CONFLICT DO NOTHING RETURNING 1) SELECT count(*) FROM i"

echo "all $pass checks passed ($IMAGE)"
