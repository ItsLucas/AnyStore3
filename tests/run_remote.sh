#!/usr/bin/env bash
# Runs repeatable acceptance checks against the deployed AnyStore service.

set -euo pipefail

cd "$(dirname "$0")/.."

SECRETS_FILE="${ANYSTORE_SECRETS_FILE:-secrets.env}"
if [[ ! -f "$SECRETS_FILE" ]]; then
    echo "missing $SECRETS_FILE" >&2
    exit 2
fi

set -a
# shellcheck disable=SC1090
source "$SECRETS_FILE"
set +a

export ANYSTORE_TEST_BASE_URL="${ANYSTORE_TEST_BASE_URL:-https://your-api.example.com/api/v1}"
export ANYSTORE_TEST_AUTH_TOKEN="${ANYSTORE_TEST_AUTH_TOKEN:-${ANYSTORE_AUTH_TOKEN:-}}"

if [[ -z "$ANYSTORE_TEST_AUTH_TOKEN" ]]; then
    echo "ANYSTORE_AUTH_TOKEN or ANYSTORE_TEST_AUTH_TOKEN is required" >&2
    exit 2
fi

echo "==> Remote smoke test"
python3 tests/remote_smoke.py

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

echo "==> Postman/Newman core collection"
python3 tests/contract/normalize_collection.py \
    AnyStore.postman_collection.json "$tmp_dir/collection.json"
umask 077
jq -n \
    --arg base_url "$ANYSTORE_TEST_BASE_URL" \
    --arg auth_token "$ANYSTORE_TEST_AUTH_TOKEN" \
    '{
        name: "AnyStore remote",
        values: [
            {key: "baseUrl", value: $base_url, enabled: true},
            {key: "authToken", value: $auth_token, enabled: true}
        ],
        _postman_variable_scope: "environment"
    }' > "$tmp_dir/environment.json"
npx --yes newman@6 run "$tmp_dir/collection.json" \
    --environment "$tmp_dir/environment.json" \
    --reporters cli --color off

if [[ "${1:-}" == "--full" ]]; then
    echo "==> Full HTTP contract suite"
    echo "warning: uploads a multipart object larger than 64 MiB"
    python3 tests/contract/contract_suite.py "$ANYSTORE_TEST_BASE_URL"
fi

echo "REMOTE TESTS PASSED"
