# Alert-generation architecture

## Scope and ownership

```text
OHLC -> Signals feature runtime -> MARKET_EVALUATION -> Alerts evaluators
                                                     -> ALERT_EVENTS
                                                     -> future subscription/fan-out
```

- OHLC owns canonical bars.
- Signals owns reusable market facts and algorithm versions: EMA, FVG/OB,
  structure, swings, liquidity, levels, and future tick-derived features.
- Alerts owns canonical rule definitions, temporal trigger policy, shared
  evaluation state, and immutable generated alert events.
- A future service owns users, subscriptions, entitlements, devices, and
  WebSocket/FCM/APNs delivery. User count never multiplies market evaluation.

Signals is not merged into Alerts. A fact is computed once; a canonical alert
definition is evaluated once; downstream fan-out may then serve millions of
users.

## Authoritative evaluation order

Signals publishes all alert inputs to one `MARKET_EVALUATION` stream with 256
logical partitions. Dataset, symbol, and timeframe are hashed with null-byte
separators using FNV-1a. Each partition consumer sees this order:

```text
provisional transition -> replaceable snapshot -> confirmed -> next bar
```

Subjects distinguish priority without creating a second ordering domain:

```text
signals.evaluation.<partition>.confirmed.<series-hash>
signals.evaluation.<partition>.transition.<series-hash>
signals.evaluation.<partition>.snapshot.<series-hash>
signals.evaluation.<partition>.reset.<series-hash>
```

Confirmed frames and transitions are lossless within the seven-day recovery
window. Only one steady one-second snapshot per series is retained using a
JetStream subject rollup. Processing every stale heartbeat during overload is
therefore unnecessary, while a colour transition or confirmed close is never
intentionally coalesced.

## Hot-path runtime

PostgreSQL is the durable authority, not the per-predicate execution engine.
At startup each worker loads only its owned partition checkpoints and compiles
definitions once.

```text
PartitionRuntime (one mutex/ordered worker per logical partition)
  SeriesEvaluationState
    confirmed feature state
    intrabar feature state
    last 500 confirmed candles
    candle colours and EMA baselines
    active zones
    source revision/reset cursor
  DefinitionIndex
    exact dataset/symbol/timeframe routes
    dataset-wildcard routes
    compiled, validated rules
    dependency masks
  DefinitionEvaluationState
    confirmed/intrabar truth edge
    last trigger time
    last alerted bar
```

An event updates shared series features once. All applicable compiled rules
reuse that state; candle history and indicators are not copied per definition.
Provisional observations skip definitions that cannot depend on candle or
indicator changes. JSON rule parsing and definition queries are absent from the
event hot path.

## Durable batch boundary

One fetched partition batch is evaluated in memory, then one PostgreSQL
transaction bulk-writes:

1. input inbox IDs and payloads;
2. final touched series checkpoints;
3. minimal touched definition states;
4. immutable alert events with full provenance;
5. alert publication outbox rows.

Only after commit does the process update its in-memory checkpoint and
acknowledge the final JetStream message with `AckAll`. A failed transaction
changes neither memory nor the broker acknowledgement. Redelivery is safe via
source IDs, deterministic alert IDs, unique business keys, and stale revision
checks.

This is at-least-once transport plus idempotent business effects—not a claim of
distributed exactly-once delivery.

## Definitions and millions of subscribers

Definition identity is a SHA-256 hash of normalized series, rule, mode,
trigger semantics, and cooldown. Equivalent requests return one canonical
definition ID. Semantic definitions are immutable; changed semantics create a
new hash/ID. `version=1` is recorded today and the alert event also carries the
hash.

Definition enable/disable changes use a future `effective_at` boundary. Every
replica refreshes its compiled cache before that boundary. If the cache becomes
stale, ingestion fails closed and JetStream redelivers instead of silently
missing a newly active rule.

The future subscription service maps one generated `alert_id` to any number of
users. It must not create a NATS consumer per user or evaluate a market rule per
subscriber.

## Ownership and horizontal scaling

The 256 logical partitions are independent of the number of processes.
`worker_index/worker_count` assigns them deterministically. PostgreSQL leases
each actual partition to one process and renews the lease every five seconds.
Overlapping worker generations or duplicate indices cannot evaluate a series
concurrently; a process stops if renewal fails.

Within one series ordering remains sequential by design. A single extremely
hot series cannot be accelerated by adding replicas; its feature algorithms
must be optimized or split into a dedicated upstream feature path.

## Corrections and replay

Live alerts state what the system knew at the original event time. Historical
OHLC corrections do not retract already delivered alerts. Signals publishes an
authoritative reset containing the corrected current frame and up to 500 prior
confirmed bars. Alerts rebuilds future shared state and truth edges without
emitting historical alerts. Reset IDs/timestamps make direct/outbox duplicates
harmless.

Offline replay/backtest may use corrected history and therefore differ from the
original live run. This policy is deterministic, testable, and avoids pretending
that an already displayed push notification can be undone.

## Audit identity

Every alert records definition ID/version/hash, feature algorithm name/version/
config hash, source event and candle IDs, source revision/status, event time,
evaluation time, exact matched zone/pattern/indicator references, reasons, and
the normalized source observation. This is sufficient to answer why an alert
fired without relying on mutable current configuration.

## Planned feature modules

New market calculations belong in Signals when reusable (liquidity sweeps,
swings, relative volume inputs, ATR, key levels). Alert-specific comparisons,
AND/OR/NOT, thresholds, cooldowns, and temporal sequences belong in Alerts.
Raw ticks should feed a dedicated tick-feature runtime that emits bounded
versioned features such as realized volatility, velocity, spread, and ticks/s;
a one-second candle snapshot is not tick history.

Multi-timeframe rules must select the newest confirmed higher-timeframe feature
whose confirmation time is not later than the triggering event. Future modules
must preserve this event-time no-lookahead rule.
