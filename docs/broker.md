# Production broker flow

## Streams

| Stream | Subjects | Retention | Semantics |
| --- | --- | ---: | --- |
| `MARKET_EVALUATION` | `signals.evaluation.>` | 7 days | One ordered alert-input domain; snapshot subjects roll up per series |
| `MARKET_FACTS` | `signals.facts.>` | 7 days | Confirmed reusable Signals facts for non-alert consumers |
| `ALERT_EVENTS` | `alerts.events.>` | 30 days | Immutable generated alerts |
| `ALERTS_DLQ` | `alerts.dlq.signals` | 30 days | Invalid inputs and exhausted evaluations |

`MARKET_EVALUATION` has 256 logical partitions. Alerts uses durable
`ALERTS_EVALUATOR_V3_<partition>` pull consumers filtered by
`signals.evaluation.<partition>.>`. A process only opens consumers for the
partitions it owns and holds matching PostgreSQL leases.

## Input acknowledgement

The consumer fetches a bounded batch (32 by default), preserves broker order,
evaluates in memory, and atomically checkpoints all touched state plus generated
alerts/outbox rows. It then `AckSync`s the final message under `AckAll`.

- A database/evaluator failure NAKs messages with exponential delay capped at
  five minutes.
- Invalid payloads are written to `ALERTS_DLQ` before source termination.
- A message exceeding `ALERTS_NATS_MAX_DELIVERIES` is dead-lettered with its
  original subject and reason.
- A process crash before acknowledgement causes safe redelivery.

## Output publication

The Alerts transaction creates an outbox row but does no network I/O. Publisher
workers claim pending rows, publish with `Nats-Msg-Id: <alert_id>`, wait for the
JetStream acknowledgement, and mark the row published. Failures use bounded
exponential retry. `alertsd_outbox_pending` and
`alertsd_outbox_oldest_age_seconds` expose backlog health.

Downstream subscription services use a small number of shared durable
consumers over `alerts.events.>`. They atomically deduplicate `alert_id` with
their own fan-out jobs. Never use one consumer per user.

## Recovery and upgrades

Signals flushes its confirmed outbox before accepting new OHLC input after a
restart. Direct confirmed/reset frames and outbox retries share deterministic
IDs. Alerts source inbox and series revision/reset cursors provide permanent
deduplication beyond JetStream's short duplicate window.

Changing partition count, hash inputs, or subject shape is a protocol migration
requiring a new durable prefix. V3 is the current layout. During worker-count
changes, start new workers only after old leases are released or expire; the
lease table prevents overlapping generations from corrupting checkpoints.

## Production infrastructure

The included Compose files are single-node development examples. Production
requires a three-node JetStream cluster across failure zones with stream
replicas set to three, TLS and scoped accounts, and monitored disk capacity.
PostgreSQL requires HA, TLS, automated backups, tested point-in-time recovery,
connection pooling, and storage/IOPS alarms. Application code cannot substitute
for those infrastructure guarantees.

Monitor consumer pending/ack-pending/redelivery counts from JetStream, Alerts
evaluation latency/failures/stale events, definition-cache age, DLQ volume,
outbox count/age, PostgreSQL locks/IOPS/connections, lease loss, and end-to-end
event-time-to-alert latency.
