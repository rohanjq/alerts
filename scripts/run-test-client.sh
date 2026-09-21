#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
signals_env_file=${SIGNALS_ENV_FILE:-"$project_dir/../signals/.env"}

# Complete test-client configuration. Every value can be overridden by the
# caller, for example: ALERTS_TEST_SYMBOL=ETHUSDT ./scripts/run-test-client.sh
export ALERTS_API_URL=${ALERTS_API_URL:-http://127.0.0.1:18100}
export ALERTS_API_TOKEN=${ALERTS_API_TOKEN:-}
export ALERTS_NATS_URL=${ALERTS_NATS_URL:-nats://127.0.0.1:4222}
export ALERTS_TEST_DATASET=${ALERTS_TEST_DATASET:-live}
export ALERTS_TEST_SYMBOL=${ALERTS_TEST_SYMBOL:-BTCUSDT}
export ALERTS_TEST_TIMEFRAME=${ALERTS_TEST_TIMEFRAME:-1m}
export ALERTS_TEST_DURABLE=${ALERTS_TEST_DURABLE:-ALERTS_TEST_CLIENT_V1}

# The local Signals and Alerts services share one broker token. Keep the secret
# in Signals' ignored .env file instead of copying it into this script.
if [ -z "${ALERTS_NATS_TOKEN:-}" ]; then
    if [ ! -r "$signals_env_file" ]; then
        echo "missing ALERTS_NATS_TOKEN and cannot read $signals_env_file" >&2
        exit 1
    fi
    while IFS='=' read -r key value; do
        if [ "$key" = "NATS_AUTH_TOKEN" ]; then
            ALERTS_NATS_TOKEN=$value
            break
        fi
    done < "$signals_env_file"
fi
if [ -z "${ALERTS_NATS_TOKEN:-}" ]; then
    echo "NATS_AUTH_TOKEN is missing from $signals_env_file" >&2
    exit 1
fi
export ALERTS_NATS_TOKEN

cd "$project_dir"
exec go run ./cmd/testclient "$@"
