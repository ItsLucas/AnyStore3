#!/usr/bin/env bash
# Runs every test level defined by AnyStore_Architecture_v1.md section 24.
#
#   1. domain and application unit tests
#   2. adapter conformance suites (in-memory, PostgreSQL, local filesystem)
#   3. the HTTP contract suite against a live server
#
# Usage: tests/run_all.sh

set -euo pipefail

cd "$(dirname "$0")/.."

DATABASE_URL="${ANYSTORE_TEST_DATABASE_URL:-postgres://postgres:anystore@127.0.0.1:54329/anystore}"
BASE_URL="${ANYSTORE_TEST_BASE_URL:-http://127.0.0.1:8088/api/v1}"
PORT="${ANYSTORE_TEST_PORT:-8088}"
BLOB_ROOT="$(mktemp -d)"
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$BLOB_ROOT"
}
trap cleanup EXIT

echo "==> 1/3 unit and adapter tests"
ANYSTORE_TEST_DATABASE_URL="$DATABASE_URL" cargo test --workspace

echo "==> 2/3 building the server"
cargo build --workspace

echo "==> 3/3 HTTP contract suite"
ANYSTORE_DATABASE_BACKEND=postgres \
ANYSTORE_DATABASE_URL="$DATABASE_URL" \
ANYSTORE_BLOB_BACKEND=local_fs \
ANYSTORE_LOCAL_BLOB_ROOT="$BLOB_ROOT" \
ANYSTORE_LOCAL_BLOB_SECRET=contract-test-secret \
ANYSTORE_PORT="$PORT" \
ANYSTORE_PUBLIC_BASE_URL="http://127.0.0.1:$PORT" \
ANYSTORE_MIGRATE_ON_START=true \
ANYSTORE_MAINTENANCE_INTERVAL_SECONDS=0 \
    ./target/debug/anystore-server >"$BLOB_ROOT/server.log" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 40); do
    if curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.25
done

python3 tests/contract/contract_suite.py "$BASE_URL"

if command -v npx >/dev/null 2>&1; then
    echo "==> Postman collection (newman)"
    python3 tests/contract/normalize_collection.py \
        AnyStore.postman_collection.json "$BLOB_ROOT/collection.json"
    npx --yes newman@6 run "$BLOB_ROOT/collection.json" \
        --env-var "baseUrl=$BASE_URL" --reporters cli --color off
else
    echo "==> skipping newman: npx is not installed"
fi

echo "ALL TEST LEVELS PASSED"
