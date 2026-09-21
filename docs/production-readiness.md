# Production readiness, SLOs, and capacity certification

The code implements the production correctness architecture. A specific scale
claim still requires certification on the exact managed PostgreSQL, JetStream,
node size, series mix, rule catalog, and market burst profile used in
production.

## Initial product SLOs

| Measurement | Initial objective |
| --- | ---: |
| Confirmed event to generated alert p99 | under 500 ms |
| Intrabar transition to generated alert p99 | under 500 ms at the current 1 Hz source cadence |
| Intentional confirmed/transition loss | zero inside the 7-day recovery window |
| Duplicate business alert | zero through deterministic identity/idempotency |
| Evaluator availability | 99.99% target after HA infrastructure is installed |
| Definition cache age | below 1 second |
| Output outbox oldest age | below 30 seconds normally |

Sub-250 ms intrabar SLOs require a source cadence faster than the present
one-second snapshots. Delivery latency to FCM/APNs/WebSocket is a separate
downstream SLO.

## Capacity matrix

Test at least 100, 1,000, and 10,000 active series; 10, 100, and 1,000
canonical definitions per series; realistic mixed confirmed timeframes; 1 Hz
intrabar state; realistic and burst tick-derived feature rates; and 1, 2, 4,
and 8 evaluator workers. Record p50/p95/p99/p99.9 latency, throughput, memory per
series, PostgreSQL CPU/IOPS/connections, lock time, JetStream lag/redeliveries,
outbox age, hot-series behavior, and hot-partition skew.

Run the checked-in evaluator microbenchmark with:

```bash
go test ./internal/runtime -run '^$' \
  -bench BenchmarkCompiledEvaluatorBatch -benchmem
```

This isolates evaluator/checkpoint allocation behavior with the memory
repository. It is not a database or cluster capacity claim.

## Failure-injection gates

Before launch, automate and pass:

- evaluator kill after DB commit but before input acknowledgement;
- Signals kill after commit but before direct/outbox publication;
- PostgreSQL primary failover during a batch;
- one JetStream node loss and leader election;
- broker disconnect/reconnect storm;
- duplicate and out-of-order delivery;
- poison payload and DLQ replay;
- definition cache/database outage across an activation boundary;
- worker lease expiry and takeover;
- authoritative OHLC correction/reset followed by live traffic;
- seven-day stream replay at the maximum supported lag;
- backup restore and point-in-time recovery in an isolated environment.

Unit tests cover failed checkpoint rollback, duplicate/stale handling, reset
reconstruction, cross-replica activation boundaries, exact fact provenance,
partition ownership, and lease exclusion. An opt-in real PostgreSQL test
validates migrations and the checkpoint+alert+outbox transaction:

```bash
ALERTS_TEST_DATABASE_URL='postgres://.../isolated_test?sslmode=require' \
  go test ./internal/store \
  -run TestPostgresRuntimeTransactionAndPartitionLeases -v
```

## Retention and data growth

Defaults are 48 hours for unreferenced provisional inbox rows, 30 days for
unreferenced confirmed inbox rows, one year for published alert history, seven
days for evaluation input, and 30 days for generated alert/DLQ streams.
Maintenance deletes bounded batches and never deletes unpublished alert events.

Track table/index size and autovacuum. Before one PostgreSQL cluster approaches
its measured IOPS/storage envelope, move alert history to time partitions and
assign evaluator partition ranges to database shards. PostgreSQL partitioning
does not create global unique indexes, so the migration must preserve source
and alert idempotency keys in a routing/identity table; it must not be improvised
during an incident.

## Deployment gate

Use at least two evaluator processes, unique stable `ALERTS_INSTANCE_ID`s,
correct worker indices/count, three JetStream replicas, PostgreSQL HA/PITR,
private TLS networking, scoped credentials, dashboards, alerts, DLQ/replay
runbooks, and tested rollback. Do not advertise a numeric supported capacity
until the matrix above passes on production-equivalent infrastructure.
