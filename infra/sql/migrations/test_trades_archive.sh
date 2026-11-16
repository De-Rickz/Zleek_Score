#!/usr/bin/env bash
# Integration test for trades_archive.sql / trades_archive_enable.sql on a
# throwaway TimescaleDB 2.31.0 (production's version). Never touches a real
# database: starts its own container (no volume; port on 127.0.0.1 only), loads
# infra/sql/schema.sql and the migration, and checks the design's guarantees.
#
#   infra/sql/migrations/test_trades_archive.sh        # from the repo root
#
# Or against an EMPTY throwaway database that already runs (CI's service
# container): ZLEEK_TEST_DATABASE_URL=postgresql://postgres:...@host:port/db,
# a superuser, TimescaleDB available; psql on the PATH. Never a real database:
# the test creates tables and writes.
set -euo pipefail
cd "$(dirname "$0")/../../.."
IMAGE="${ZLEEK_TEST_IMAGE:-timescale/timescaledb:2.31.0-pg16}"
PYTHON="${ZLEEK_TEST_PYTHON:-apps/worker/.venv/bin/python}"
if [ -n "${ZLEEK_TEST_DATABASE_URL:-}" ]; then
    URL="$ZLEEK_TEST_DATABASE_URL"
    IMAGE="the database at ZLEEK_TEST_DATABASE_URL"
    q() { PGOPTIONS='-c client_min_messages=warning' psql "$URL" -X -q -v ON_ERROR_STOP=1 -tA "$@"; }
    [ "$(q -c "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")" = 0 ] \
        || { echo "ZLEEK_TEST_DATABASE_URL must point at an empty throwaway database" >&2; exit 1; }
else
    NAME="zleek_trades_archive_test_$$"
    PORT="${ZLEEK_TEST_PORT:-55433}"   # localhost only, for the Python end-to-end checks
    URL="postgresql://postgres:test@127.0.0.1:$PORT/postgres"
    docker run -d --rm --name "$NAME" -p "127.0.0.1:$PORT:5432" -e POSTGRES_PASSWORD=test "$IMAGE" > /dev/null
    trap 'docker rm -f "$NAME" > /dev/null 2>&1 || true' EXIT
    for _ in $(seq 60); do docker exec "$NAME" pg_isready -U postgres -q 2>/dev/null && break; sleep 1; done
    sleep 2
    q() { docker exec -i -e PGOPTIONS='-c client_min_messages=warning' "$NAME" psql -U postgres -d postgres -X -q -v ON_ERROR_STOP=1 -tA "$@"; }
fi
q -c "SELECT default_version FROM pg_available_extensions WHERE name = 'timescaledb'" | sed 's/^/timescaledb available: /'
pass=0
check() {  # check NAME EXPECTED SQL
    local got; got="$(q -c "$3")"
    if [ "$got" = "$2" ]; then pass=$((pass + 1)); echo "ok   $1"; else echo "FAIL $1: expected '$2', got '$got'"; exit 1; fi
}

q < infra/sql/schema.sql > /dev/null
q -c "CREATE ROLE zleek_web LOGIN" > /dev/null
q -1 < infra/sql/migrations/trades_archive.sql > /dev/null
q -1 < infra/sql/migrations/trades_archive.sql > /dev/null          # idempotent
check "migration is idempotent" "1" "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'trades_archive'"

# Data: 8 days of trades for two markets, 24 per day each (hourly), ts on the
# hour; days 7..1 ago plus today.
q > /dev/null <<'SQL'
INSERT INTO markets(id, title) VALUES ('m1', 'one'), ('m2', 'two');
INSERT INTO trades(market_id, asset_id, ts, price, size_usd, side, tx_hash, raw)
SELECT m, 'a', date_trunc('day', now()) - d * interval '1 day' + h * interval '1 hour', 0.5, 10, 'BUY',
       format('0x%s-%s-%s', m, d, h), '{"proxy_wallet": "w"}'
FROM unnest(ARRAY['m1','m2']) m, generate_series(0, 7) d, generate_series(0, 23) h
WHERE date_trunc('day', now()) - d * interval '1 day' + h * interval '1 hour' < now();
SQL
total="$(q -c "SELECT count(*) FROM trades")"
check "trades_all equals trades before the move" "$total" "SELECT count(*) FROM trades_all"

# Writes through zleek_store_trade, before the move.
check "recent new trade goes to trades" "trades" \
  "SELECT zleek_store_trade('m1','a',now() - interval '1 hour' + interval '7 minutes',0.5,1,'BUY',null,null,'0xnew-recent','{}'::jsonb,null,null,null)"
x1="$(q -c "SELECT xmin FROM trades WHERE tx_hash='0xnew-recent'")"
q -c "SELECT zleek_store_trade('m1','a',now() - interval '1 hour' + interval '7 minutes',0.5,1,'BUY',null,null,'0xnew-recent','{}'::jsonb,null,null,null)" > /dev/null
check "recent re-fetch, same price: row not rewritten (2a guard kept)" "$x1" "SELECT xmin FROM trades WHERE tx_hash='0xnew-recent'"
q -c "SELECT zleek_store_trade('m1','a',now() - interval '1 hour' + interval '7 minutes',0.6,1,'BUY',null,null,'0xnew-recent','{}'::jsonb,null,null,null)" > /dev/null
check "recent re-fetch, new price: price updated" "0.6" "SELECT price FROM trades WHERE tx_hash='0xnew-recent'"
check "old trade still in trades (not moved yet) is not copied to the archive" "duplicate" \
  "SELECT zleek_store_trade('m1','a',date_trunc('day', now()) - interval '6 days' + interval '3 hours 2 seconds',0.5,10,'BUY',null,null,'0xm1-6-3','{}'::jsonb,null,null,null)"
total=$((total + 1))

# Backlog move, one day per transaction.
q -c "CALL archive_trades_backlog()" > /dev/null 2>&1
check "backlog: nothing older than the cutoff left in trades" "0" "SELECT count(*) FROM trades WHERE ts < archive_cutoff()"
check "backlog: every row in exactly one table" "$total" "SELECT count(*) FROM trades_all"
check "backlog: no tx_hash in both tables" "0" "SELECT count(*) FROM trades t JOIN trades_archive a USING (tx_hash)"
check "backlog: distinct tx_hash = rows" "$total" "SELECT count(DISTINCT tx_hash) FROM trades_all"
check "backlog: archive holds days 7..5 ago (3 days x 2 markets x 24)" "144" "SELECT count(*) FROM trades_archive"
check "backlog again moves nothing" "0" "SELECT archive_trades_move(archive_cutoff())"

# Compress the archive, then old writes into compressed chunks.
q -c "SELECT count(compress_chunk(c)) FROM show_chunks('trades_archive') c" > /dev/null
check "archive chunks compressed" "3" "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name='trades_archive' AND is_compressed"
check "archived trade re-written with the Data API ts: refused" "duplicate" \
  "SELECT zleek_store_trade('m1','a',date_trunc('day', now()) - interval '6 days' + interval '3 hours',0.5,10,'BUY',null,null,'0xm1-6-3','{}'::jsonb,null,null,null)"
check "archived trade re-written with a websocket ts 4 s earlier: refused" "duplicate" \
  "SELECT zleek_store_trade('m1','a',date_trunc('day', now()) - interval '6 days' + interval '3 hours' - interval '4 seconds',0.5,10,'BUY',null,null,'0xm1-6-3','{}'::jsonb,null,null,null)"
check "new old trade goes into the (compressed) archive" "archive" \
  "SELECT zleek_store_trade('m2','a',date_trunc('day', now()) - interval '6 days' + interval '3 hours 30 minutes',0.4,5,'SELL',null,null,'0xold-new','{}'::jsonb,null,null,null)"
total=$((total + 1))
check "it reads back through trades_all" "0.4" "SELECT price FROM trades_all WHERE tx_hash='0xold-new'"
check "and only once" "1" "SELECT count(*) FROM trades_all WHERE tx_hash='0xold-new'"
check "written again: refused" "duplicate" \
  "SELECT zleek_store_trade('m2','a',date_trunc('day', now()) - interval '6 days' + interval '3 hours 30 minutes',0.4,5,'SELL',null,null,'0xold-new','{}'::jsonb,null,null,null)"

# Nightly move with a later cutoff (simulates the next night).
check "nightly move: moves the next day" "48" "SELECT archive_trades_move(archive_cutoff() + interval '1 day')"
check "nightly move: total unchanged" "$total" "SELECT count(*) FROM trades_all"
check "nightly move: no tx_hash in both tables" "0" "SELECT count(*) FROM trades t JOIN trades_archive a USING (tx_hash)"
check "ids unique across both tables" "$total" "SELECT count(DISTINCT id) FROM trades_all"

# Reads the dashboard and bars use.
check "latest trade per market via trades_all" "2" \
  "SELECT count(*) FROM (SELECT market_id, max(ts) FROM trades_all GROUP BY 1 HAVING max(ts) > now() - interval '1 day') s"
check "a market's newest trades span both tables" "30" \
  "SELECT count(*) FROM (SELECT ts FROM trades_all WHERE market_id='m1' ORDER BY ts DESC LIMIT 30) s"
check "zleek_web can read the view" "t" \
  "SET ROLE zleek_web; SELECT count(*) > 0 FROM trades_all; RESET ROLE"

# Enable: compression policy and nightly job.
q < infra/sql/migrations/trades_archive_enable.sql > /dev/null
q < infra/sql/migrations/trades_archive_enable.sql > /dev/null      # idempotent
check "one nightly job" "1" "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name='archive_trades_job'"
check "one compression policy" "1" "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name='policy_compression' AND hypertable_name='trades_archive'"
job="$(q -c "SELECT job_id FROM timescaledb_information.jobs WHERE proc_name='archive_trades_job'")"
q -c "CALL run_job($job)" > /dev/null
check "job runs (nothing left to move at today's cutoff)" "0" "SELECT count(*) FROM trades WHERE ts < archive_cutoff()"

echo "all $pass SQL checks passed ($IMAGE)"

# The worker's own code against this database (apps/worker/tests/test_trades_archive_db.py).
ZLEEK_TEST_DATABASE_URL="$URL" "$PYTHON" apps/worker/tests/test_trades_archive_db.py
