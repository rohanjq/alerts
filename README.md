# alertsd

`alertsd` is a pure alert-generation engine. Operators register reusable alert
definitions such as “BTCUSDT 15m crossed below EMA 200.” The engine consumes
normalized market observations, evaluates definitions, and emits immutable
`io.ytstack.alert.triggered.v1` events when they match.

It has no users, user subscriptions, device tokens, notification destinations,
or mobile/web delivery logic. A later subscription/notification service will
consume all generated alert events, identify interested users, and deliver to
WebSocket, APNs, FCM, Pushover, email, or other channels.

## Implemented production flow

- Alert-definition create/list/enable/disable/delete API.
- Exact dataset/symbol/timeframe routing.
- `all`, `any`, and `not` composition.
- FVG/OB approach and touch rules, EMA crossings, candle colors/streaks, and patterns.
- Stateful intrabar candle-colour flips after a configurable percentage of the
  candle lifetime, fed by the authoritative ordered evaluation stream.
- Deterministic input deduplication, evaluator state, rising-edge semantics,
  cooldowns, immutable alert occurrences, and an asynchronous log publisher.
- Partition-local in-memory shared feature state, compiled/validated definition
  cache, dependency filtering, and minimal per-definition temporal state.
- Batched PostgreSQL checkpoints for definitions, input inbox, evaluator state,
  alert events, exact audit provenance, and the transactional publication outbox.
- Direct conversion of complete Signals `smc.market_state` events, including
  candle OHLC, EMA values, zone lifecycle events, and pattern occurrences.
- One durable `MARKET_EVALUATION` ordering domain; tiered confirmed, transition,
  replaceable-snapshot, and correction-reset subjects.
- 256 logical partitions decoupled from process count, database-backed exclusive
  leases, batched acknowledgements, bounded retry, and dead-letter handling.
- Cross-replica definition activation boundaries with fail-closed stale caches.
- Historical-correction state rebuilds without alert retraction, bounded
  retention maintenance, health/readiness probes, and Prometheus metrics.

PostgreSQL is required by default. The in-memory repository is deliberately
available only when `ALERTS_ALLOW_MEMORY_STORE=true`; restarting it loses all
state and it must never be used in production. The first publisher logs alert
events as well as publishing them to JetStream. A later user delivery service
consumes `alerts.events.>` without being coupled to alert evaluation.

## Run and verify

```bash
make test
ALERTS_INSECURE_NO_AUTH=true ALERTS_ALLOW_MEMORY_STORE=true make run
```

Register an alert definition:

```bash
curl -sS -X POST http://127.0.0.1:8100/v1/definitions \
  -H 'Content-Type: application/json' \
  -d '{"name":"Three red 15m candles","series":{"symbol":"BTCUSDT","timeframe":"15m"},"rule":{"type":"candle_streak","direction":"red","count":3},"evaluation_mode":"confirmed_close","trigger_mode":"on_occurrence","cooldown_seconds":900}'
```

Post observations using [`docs/observations.md`](docs/observations.md). The
third matching observation produces an alert event and the first publisher
adapter logs `alert event published`. Reposting a `source_event_id` is a no-op.

`POST /v1/internal/signal-events` accepts the complete market-state CloudEvent
from Signals; `POST /v1/internal/observations` accepts the equivalent normalized
internal contract.

Production mode requires separate 32+ character control-plane and ingest
tokens. These are service credentials, not user authentication.

## Connect Signals to Alerts

Signals and Alerts must use the same NATS JetStream cluster and authentication
token. Start the Signals stack first; it creates the shared `signald_backend`
network and publishes confirmed facts. Then copy this project's `.env.example`
to `.env`, set `ALERTS_NATS_TOKEN` to the same value as Signals'
`NATS_AUTH_TOKEN`, set all database/API secrets, and run:

```bash
docker compose up --build -d
curl http://127.0.0.1:8100/readyz
```

The Alerts Compose file deliberately does not start another NATS server. It
joins the shared network and consumes `signals.evaluation.000.>` through
`signals.evaluation.255.>`; generated alert facts are published to
`alerts.events.000` through `alerts.events.255`. See
[`docs/broker.md`](docs/broker.md) for topology, failure behavior, scaling, and
production cluster requirements. See
[`docs/production-readiness.md`](docs/production-readiness.md) for SLOs,
capacity certification, HA/PITR requirements, and failure-injection gates.

## Live test subscriber

`cmd/testclient` creates five definitions for one series/timeframe as test
setup and then durably subscribes to **every** generated `alerts.events.>`
message:

- one green candle;
- one red candle;
- two consecutive red candles;
- two consecutive green candles.
- a forming candle that flips colour after 50% of its lifetime.

A doji (`open == close`) is intentionally neither green nor red, so those two
single-candle definitions emit no color alert for that minute.

For the current local stack, the complete environment and run command are in
one launcher:

```bash
./scripts/run-test-client.sh
```

It reads the shared broker token from `../signals/.env` without copying the
secret. Environment variables and command flags remain overrideable.

```bash
ALERTS_API_URL=http://127.0.0.1:8100 \
ALERTS_API_TOKEN="$ALERTS_API_TOKEN" \
ALERTS_NATS_URL=nats://127.0.0.1:4222 \
ALERTS_NATS_TOKEN="$ALERTS_NATS_TOKEN" \
go run ./cmd/testclient -symbol BTCUSDT -timeframe 1m
```

Use `-max-events 1 -timeout 5m` for an automated smoke test. Reusing the same
durable resumes after the last acknowledged alert, and recreating equivalent
definitions is idempotent. The registered definition IDs are never used as a
client-side filter. Add `-json` to print the complete alert payload.
