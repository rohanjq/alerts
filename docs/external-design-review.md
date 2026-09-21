# Alert Evaluation System — External Design Review

Status: implementation snapshot reviewed on 2026-09-08  
Scope: `ohlc`, `signals`, and `alerts`; user subscriptions and notification delivery are intentionally out of scope

## 1. Purpose and review questions

The system creates reusable market-alert events. It does not contain users,
mobile devices, entitlements, or per-user delivery preferences. A future
subscription/delivery service will consume every generated alert, decide which
users are interested, and deliver through WebSocket, APNs, FCM, Pushover, and
other channels.

This document describes the code that exists today, its evaluation semantics,
data structures, timing, durability model, scaling characteristics, known
limitations, and a proposed path for adding substantially more rule types.

The main questions for external review are:

1. Is the boundary between market facts and alert decisions correct?
2. Are ordering, replay, deduplication, and intrabar semantics correct?
3. Is the current PostgreSQL-per-observation evaluation path sufficient for
   the expected number of distinct definitions and active series?
4. Is the proposed shared feature engine the right way to add windowed,
   multi-timeframe, and tick-based rules?
5. Which correctness or operational gaps must be closed before production?

## 2. Executive assessment

The current implementation has a sound reliability foundation:

- deterministic identifiers;
- ordered, sharded event processing;
- durable confirmed facts;
- explicit provisional-fact semantics;
- inbox deduplication;
- transactional alert creation and output outbox;
- at-least-once delivery with exactly-once business effects;
- centralized reusable alert definitions, independent of users.

It is suitable for functional validation and a bounded catalog of shared alert
definitions. It should not yet be described as proven at Internet scale. The
main scaling issue is not the number of subscribers: millions of users can
reference the same generated alert without causing repeated market
computation. The important dimensions are:

```text
active market series
  × observation frequency
  × distinct alert definitions for each series
  × state/database work per definition
```

Today, each observation queries PostgreSQL for definitions, deserializes their
JSON rules, locks and updates one JSONB state row per applicable definition,
and does all work in one database transaction. That is robust and simple but
needs caching, shared feature computation, partitioning, retention, and load
testing before supporting a very large rule catalog or high-frequency feeds.

## 3. Ownership boundaries

### 3.1 Current boundary

```text
Market/exchange
      |
      v
OHLC service
  canonical OHLCV candles and revisions
      |
      v
Signals service
  reusable market facts: EMA, FVG, order block, patterns, swings,
  structure, liquidity, key levels, premium/discount
      |
      | confirmed facts + provisional snapshots
      v
Alerts service
  alert definitions, thresholds, combinations, temporal trigger state,
  cooldowns, immutable generated alert events
      |
      v
Future subscription/delivery service
  user subscriptions, fan-out, WebSocket/push/email delivery
```

Signals currently calculates reusable market features. Alerts is the only
component that decides whether an alert should fire. For example, Signals says
“these FVG zones exist,” while Alerts says “generate an alert when price is
within 100 basis points of one.” Signals never knows about alert definitions or
users.

This separation is recommended as long as it remains strict:

- reusable, deterministic market facts belong to Signals;
- every `when ... then generate alert` decision belongs to Alerts;
- user selection and notification delivery belong downstream;
- the same calculation must not be independently implemented in both Signals
  and Alerts.

If the product requirement literally means that FVG detection, swing
detection, EMA calculation, and every other market feature must live inside the
Alerts repository, the existing Signals market reducer should be moved as one
versioned feature-engine module rather than duplicated. The current split is a
layered design, not fully centralized source code.

### 3.2 Subscriber boundary

`ALERT_EVENTS` is the complete generated-alert feed. A downstream service uses
one durable consumer identity and consumes `alerts.events.>`. It does not
create one broker consumer per definition or per user. Replicas of the same
downstream service share that durable identity. A different logical downstream
service uses a different durable identity so it also receives every event.

The downstream database maps `alert_definition_id -> users/channels`. This is
where millions of users are handled; they do not multiply Signals or Alerts
evaluation work.

## 4. Current event flow

### 4.1 Confirmed path

```text
OHLC confirms candle
  -> per-series Signals lane
  -> calculate EMA and market state
  -> one Signals PostgreSQL transaction:
       confirmed events + reducer snapshots + signal outbox
  -> Signals outbox publisher
  -> MARKET_FACTS / signals.facts.<00..63>
  -> Alerts durable shard consumer
  -> one Alerts PostgreSQL transaction:
       alert_inbox
       + alert_rule_state
       + alert_events
       + alert_event_outbox
  -> acknowledge MARKET_FACTS message
  -> Alerts outbox publisher
  -> ALERT_EVENTS / alerts.events.<00..63>
  -> every downstream alert consumer
```

Confirmed facts survive process restarts and broker interruption. The input
message is acknowledged only after the Alerts transaction commits.

### 4.2 Intrabar path

```text
OHLC forming-candle revision
  -> per-series Signals lane
  -> project EMA and attach last confirmed market structure
  -> MARKET_LIVE / signals.live.<00..63>
  -> Alerts durable live-shard consumer
  -> same Alerts transaction and alert outbox as confirmed path
```

Provisional facts are not stored in Signals PostgreSQL because they repaint.
They are stored in the short-retention `MARKET_LIVE` JetStream for two hours.
Once Alerts accepts one, its inbox, state update, and any generated alert are
durable in Alerts PostgreSQL.

Confirmed frames are also sent through `MARKET_LIVE` in series order. The same
confirmed event later arrives from durable `MARKET_FACTS`; `alert_inbox` drops
the duplicate by `source_event_id`.

## 5. Evaluation frequency and latency

Alerts does not poll prices or run all rules on a timer. It is event-driven.

| Input/evaluator class | Current frequency | Notes |
| --- | --- | --- |
| Confirmed 1m definition | Once per confirmed 1m candle | Normally at the minute boundary |
| Confirmed 15m definition | Once per confirmed 15m candle | Normally every 15 minutes |
| Other confirmed timeframe | Once per confirmed candle | Determined by the series timeframe |
| Provisional/intrabar definition | At most one unchanged frame per second per active series | Driven by incoming forming-candle changes |
| New forming candle | Immediate | Bypasses the one-second coalescer |
| Candle color transition | Immediate | Bypasses the one-second coalescer |
| Alert outbox publication | Immediate wake-up, plus a 250 ms safety poll | Broker publish waits for acknowledgement |
| Test/downstream pull consumer | Fetch waits up to one second when idle | Available messages return immediately |

The one-second provisional interval is not appropriate for every future rule.
A key-level touch can be detected safely from a forming candle's accumulated
high/low even if price has moved away. True tick-volatility rules cannot be
derived correctly from one-second OHLC snapshots and require a tick or
tick-feature feed.

No evaluation can happen without an input event. For example, a 50%-lifetime
condition is checked on the first observation at or after 50%, not by a timer
scheduled for exactly 30.000 seconds. In a liquid market this is normally
immediate; the semantic difference should remain explicit.

## 6. Current data contracts and structures

### 6.1 Signals series lane

Signals uses one actor-like lane per `(dataset, symbol, timeframe)`. A bounded
mailbox serializes seed, forming, and confirmed commands. The lane holds:

```text
last confirmed bar and revision
forming bar and revision
one incremental EMA reducer per configured period
projected forming EMA values
Market reducer with at most 500 confirmed bars
periodic durable reducer snapshots
```

EMA is incremental. The current market reducer retains a bounded bar slice and
recalculates the larger market-state result on each confirmed candle. That is
simple and deterministic but is approximately O(history window) per close and
should be benchmarked across the intended universe.

The Signals public bar already contains:

```text
open_time, open, high, low, close, volume, trades, revision, status
```

### 6.2 Alerts observation

The current normalized Alerts observation contains:

```text
Observation
  schema
  source_event_id             deterministic deduplication identity
  correlation_id              source-candle identity
  occurred_at                 event time
  status                      confirmed | provisional
  revision                    monotonic within the candle
  series                      dataset, symbol, timeframe
  candle                      open_time, open, high, low, close
  indicators[]                name, period, value, ready
  active_zones[]              FVG/OB geometry and state
  zone_transitions[]          created/touched/mitigated/invalidated/expired
  patterns[]                  current-bar pattern occurrences
```

Important current gap: Signals emits candle `volume` and `trades`, but the
Alerts adapter and candle model currently discard them.

### 6.3 Alert definition and rule AST

An alert definition contains:

```text
id, version, content hash, name, enabled
series selector
rule tree
evaluation_mode              confirmed_close | intrabar | both
trigger_mode                 on_occurrence | once_per_bar | every_match
cooldown_seconds
timestamps
```

Equivalent definitions have the same canonical content hash and deterministic
ID. The display name is not part of the semantic identity. This allows one
evaluation to serve many downstream subscribers.

Rules are currently represented by a recursive `Rule` structure. `all`, `any`,
and `not` form a small AST. Nesting is limited to eight levels and a composite
node can have at most twenty children.

### 6.4 Per-definition evaluation state

State is keyed by `(definition_id, series_key)` and stored as JSONB:

```text
last_truth                    rising-edge detection
last_trigger_at               cooldown
last_close                    crossing detection
indicators[name:period]       previous indicator value
candle_colors[<=100]          confirmed streak history
last_open_time
last_revision
last_status                   stale/duplicate rejection
last_alert_bar                once-per-bar gate
forming_bar
forming_color                 intrabar color-transition detection
```

This is small for current rules. Long histories should not be copied into each
definition state. Future rolling data must be shared once per series.

### 6.5 PostgreSQL tables

| Table | Purpose |
| --- | --- |
| `alert_definitions` | Durable reusable rule catalog |
| `alert_inbox` | Deduplicates accepted source observations |
| `alert_rule_state` | Stateful evaluator checkpoint per definition and series |
| `alert_events` | Immutable generated alert occurrences and monotonic cursor |
| `alert_event_outbox` | Transactional publication work |

The in-memory repository is test/development-only and must not be used in
production.

## 7. Current evaluator pseudocode

### 7.1 Signals

```text
on forming_bar(series, bar):
    execute in the series lane
    validate monotonic candle time and OHLC
    projected_emas = project each EMA using current close
    market = last confirmed market state
    clear market.pattern_occurrences and market.zone_transitions
    frame = complete_market_frame(bar, projected_emas, market, provisional=true)

    if first frame of candle
       or candle color differs from last published color
       or at least 1 second elapsed:
        publish frame to MARKET_LIVE

on confirmed_bar(series, bar):
    execute in the series lane
    reject gaps, conflicts, and out-of-order bars
    clone reducers
    advance EMAs and market reducer
    make one complete confirmed market frame
    transactionally persist bar, facts, snapshots, and Signals outbox
    publish confirmed frame to MARKET_LIVE in lane order
    atomically replace in-memory reducer state
    outbox eventually publishes same frame to MARKET_FACTS
```

### 7.2 Alerts ingestion and evaluation

```text
on market_fact(message):
    decode and validate complete Signals market-state event
    adapt it to one Alerts Observation

    begin PostgreSQL transaction

    inserted = INSERT alert_inbox(source_event_id, payload)
               ON CONFLICT DO NOTHING
    if not inserted:
        commit
        acknowledge broker message
        return

    definitions = SELECT enabled definitions
                  WHERE symbol/timeframe/dataset match
                    AND definition.created_at <= observation.occurred_at

    for definition in deterministic ID order:
        if evaluation_mode does not accept observation.status:
            continue

        lock(definition_id, series_key) using transaction advisory lock
        create state row if absent
        SELECT state FOR UPDATE

        if observation is older than state candle/revision/status:
            continue

        truth, reasons = evaluate(rule_AST, observation, prior_state)
        trigger = apply trigger_mode(truth, prior_state, rule occurrence type)
        trigger = trigger AND cooldown_allows(observation.occurred_at)
        next_state = advance(prior_state, observation, truth)
        UPDATE alert_rule_state

        if trigger:
            alert_id = hash(definition_id, source_event_id)
            INSERT alert_events with unique business keys
            INSERT alert_event_outbox in the same transaction

    commit PostgreSQL transaction
    acknowledge broker message
```

### 7.3 Alert publication

```text
loop on wake-up or every 250 ms:
    claim up to 100 unpublished outbox rows
        using FOR UPDATE SKIP LOCKED and a retry lease
    for each row:
        log alert
        publish to ALERT_EVENTS with Nats-Msg-Id = alert_id
        wait for broker persistence acknowledgement
        mark row published
    on failure:
        retain row and exponentially delay the next attempt, capped at 5 minutes
```

## 8. Implemented alert rules

| Rule | Input and matching semantics | Typical cadence |
| --- | --- | --- |
| `candle_color` | `close > open` is green; `close < open` is red; equal is doji | Confirmed close |
| `candle_streak` | Last N confirmed colors all equal configured red/green | Confirmed close |
| `candle_color_flip` | Current forming color differs from remembered non-doji color in the same candle, at/after configured lifetime percentage | Intrabar |
| `price_crosses_indicator` | Prior close/indicator and current close/indicator cross above, below, or either | Confirmed or intrabar, as configured |
| `price_near_zone` | Current close is outside but within configured basis points of an active FVG/OB | Confirmed or intrabar |
| `price_hits_zone` | Matches a zone transition, defaulting to `touched` | Normally confirmed close |
| `zone_transition` | Matches FVG/OB `created`, `touched`, `mitigated`, `invalidated`, or `expired` | Confirmed close |
| `pattern` | Matches a current-bar pattern occurrence and optional side | Confirmed close |
| `all` / `any` / `not` | Recursively combines rule results | Child-dependent |

Signals currently recognizes pattern facts including bullish/bearish engulfing,
inside bar, morning/evening star, three soldiers/crows, and tweezer top/bottom.
Alerts can match any pattern occurrence that Signals includes in the normalized
observation.

Trigger modes are distinct from rule truth:

- `once_per_bar`: the first qualifying event for that candle;
- `every_match`: every matching observation, normally paired with a cooldown;
- `on_occurrence`: every pulse-like domain occurrence, but only the false-to-true
  edge for state-like conditions.

Example: a two-red-candle streak with `on_occurrence` fires when the streak is
first established. It does not fire on every additional red candle until the
condition first becomes false. Use `every_match` if every qualifying close is
the intended behavior.

## 9. Candle-flip example

Definition:

```json
{
  "series": {"dataset":"live","symbol":"BTCUSDT","timeframe":"1m"},
  "rule": {
    "type":"candle_color_flip",
    "direction":"either",
    "min_elapsed_percent":50
  },
  "evaluation_mode":"intrabar",
  "trigger_mode":"once_per_bar"
}
```

Evaluation:

```text
duration = parse("1m") = 60 seconds
elapsed_percent = (observation.occurred_at - candle.open_time) / duration * 100

at 10s: open=100, close=101 -> green
        remember forming_color=green; no alert

at 29s: close=99 -> red transition, but elapsed=48.3%
        no alert; remember forming_color=red

at 31s: close=101 -> green transition and elapsed=51.7%
        emit red_to_green alert

later transitions in the same candle:
        suppressed by once_per_bar
```

A momentary doji does not replace the remembered real color. Color from a
previous candle is never compared with a new candle.

If the desired rule is instead “the color observed before the midpoint differs
from the first color observed after the midpoint,” that is a different
checkpoint rule. It should explicitly store the pre-midpoint color and evaluate
once after the boundary; the current rule detects an actual transition at or
after the threshold.

## 10. Ordering, deduplication, and failure behavior

### 10.1 Ordering and parallelism

Dataset, symbol, and timeframe are hashed to one of 64 subjects. Alerts runs
one durable pull consumer per shard for confirmed facts and one per shard for
live facts. `MaxAckPending=1` serializes work within each shard; different
shards run concurrently and replicas share each durable consumer.

This preserves order for one series because all of its messages map to the
same shard. It also means one exceptionally hot series can saturate one shard
and cannot be accelerated merely by adding replicas. Shard count and hot-key
behavior require measurement.

There is one cross-stream qualification. `MARKET_LIVE` contains provisional
and confirmed frames in order, while `MARKET_FACTS` independently carries the
confirmed duplicate. The two streams are consumed concurrently. During normal
operation the live sequence arrives first, but after a restart a confirmed
fact could be processed before older provisional frames in the live backlog.
The stale-revision guard would then discard those older provisional frames and
could omit an intrabar occurrence during replay. Confirmed-only evaluation is
not affected. Production must either merge the streams by event time/revision,
make one ordered stream authoritative for the evaluator, or explicitly accept
that intrabar alerts are best-effort across extended outages.

### 10.2 Delivery guarantee

Transport is at least once. Exactly-once business effects come from:

```text
alert_inbox primary key(source_event_id)
alert_events unique(definition_id, source_event_id)
alert_event_outbox unique(alert_id)
deterministic alert ID = hash(definition_id, source_event_id)
NATS duplicate ID = alert ID
```

A downstream consumer must also store processed `alert_id` values atomically
with its own side effects. Seeing the same ID again is harmless redelivery, not
a new market occurrence.

### 10.3 Event-time behavior

Cooldowns use `observation.occurred_at`, not wall-clock processing time. A new
definition does not evaluate historical observations whose event time is
before the definition's creation time. Lower revisions, duplicate revisions,
and older candle times are ignored.

Historical correction semantics are incomplete: an observation for an older
candle is discarded after state has advanced to a later candle. Signals can
rebuild its reducers from authoritative OHLC, but Alerts does not currently
rewind and replay dependent rule state. A production correction policy must be
chosen and tested.

## 11. Current scaling model

For one accepted observation, current computational/database cost is roughly:

```text
O(number of matching definitions)
  + O(total nodes in their rule ASTs)
  + one locked state read/update per applicable definition
```

Current positive properties:

- user count does not enter this formula;
- histories in Signals are bounded;
- alert definition lookup has a partial route index;
- 64 shards allow concurrent processing;
- input and output retries are safe;
- definitions are reused by deterministic identity.

Current bottlenecks:

1. Definitions are selected from PostgreSQL and JSON-decoded for every input.
2. Every applicable definition state is locked and rewritten, even when no
   alert fires.
3. All definitions for one observation execute inside one database transaction.
4. Provisional observations can cause one database transaction per active
   series per second.
5. `alert_inbox` stores the full payload for provisional as well as confirmed
   observations and has no implemented retention/partition cleanup.
6. `alert_events` embeds the full source observation in every alert.
7. The 64-shard count is fixed in code and one hot series remains single-lane.
8. No capacity benchmark establishes events/second, definitions/series,
   database IOPS, p99 evaluation latency, or recovery time.

The architecture is plausible for millions of subscribers only when a bounded
number of reusable definitions serves those subscribers. A product allowing
every user to invent a unique rule/threshold changes the scaling problem and
requires definition normalization, quotas, compiled-rule indexing, and likely
additional partitioning.

## 12. Recommended rule/feature architecture

### 12.1 Do not add every new rule directly to one large switch

The current switch is clear for a small catalog. Continued growth should use a
registry of typed evaluators:

```text
RuleEvaluator
  Type() string
  Validate(parameters) error
  Dependencies() FeatureMask
  AllowedCadences() set
  Compile(parameters) CompiledRule
  Evaluate(context, compiled_rule, definition_state) Result
```

Each evaluator must declare:

- required input fields/features;
- confirmed, intrabar, or tick cadence;
- warm-up requirement;
- whether it is a pulse or persistent condition;
- state schema/version;
- repaint/finality semantics;
- maximum memory and CPU complexity.

### 12.2 Share rolling features by series

Windowed calculations must be computed once per series, not once per alert
definition. A target runtime structure is:

```text
SeriesRuntime[(dataset, symbol, timeframe)]
  confirmed_bars: fixed-capacity ring buffer
  forming_bar: latest revision
  rolling_volume[N]: ring + sum
  rolling_range[N]: ring + sum
  ATR state
  confirmed swings and liquidity levels
  premium/discount zones
  active FVG/OB/key-level interval index
  optional tick-window state

DefinitionIndex[(dataset, symbol, timeframe, cadence)]
  compiled definitions grouped by FeatureMask

DefinitionRuntime[definition_id]
  last_truth, last_trigger_at, last_alert_bar
  only rule-specific temporal state
```

Useful O(1) structures include:

- circular buffers plus rolling sum/sum-of-squares for averages and variance;
- monotonic deques for rolling maximum/minimum;
- Welford or exponentially weighted state for stable variance;
- sorted price arrays or interval trees for active levels/zones;
- time deques for tick windows;
- compact bit masks for rule dependency selection.

The series feature state should be checkpointed and reproducible from the
authoritative candle/tick log. Per-definition state remains small and durable.

### 12.3 Compile and index definitions

Load definitions into a versioned in-memory cache and update it using a durable
control-plane change event, CDC, or a database notification plus periodic
reconciliation. Compile and validate JSON once. Index by exact series, cadence,
and feature dependencies so an observation does not deserialize or examine
irrelevant rules.

PostgreSQL remains authoritative. On cache restart, rebuild from PostgreSQL
before consuming more data. Definition changes should create immutable
versions; old alert events continue to identify the exact version evaluated.

### 12.4 Separate cadence classes

Use explicit cadence rather than treating all provisional work alike:

```text
BAR_CLOSE       deterministic, durable, no repaint
INTRABAR_1S     current forming candle, may repaint
PRICE_EVENT     immediate quote/price transition or accumulated high/low change
TICK_WINDOW     raw or lossless tick-feature updates
```

Rules in a composite expression must have compatible timing semantics, or the
engine must define a temporal join explicitly. For example, “15m discount zone
AND 1m high-volume candle” means the latest confirmed 15m zone joined with the
current/confirmed 1m candle. It must never use an unconfirmed future 15m swing.

### 12.5 Multi-timeframe state

Use event time and immutable confirmed feature versions:

```text
on 15m confirmed frame:
    update confirmed 15m swings/zones
    version the feature snapshot by its source candle

on 1m frame:
    join only the latest 15m snapshot whose confirmation time <= 1m event time
    evaluate dependent 1m definitions
```

This avoids look-ahead bias in both live operation and replay.

## 13. Proposed future rules

The following rules are not implemented in Alerts today unless explicitly
noted.

### 13.1 “15m liquidity taken”

Current related data: Signals already calculates liquidity levels and
`liquidity_sweeps`, but the Alerts adapter does not map those outputs into its
observation. Therefore this is not currently an Alerts rule.

Required semantic decisions:

- level source: equal highs/lows, confirmed swings, previous day/session high,
  or a combination;
- minimum touches and tolerance;
- taken by wick beyond level, close beyond level, or wick plus close back
  inside (sweep/reclaim);
- confirmation on the 15m close or immediate intrabar occurrence;
- whether each level can fire once or once per excursion.

Recommended confirmed rule shape:

```json
{
  "type":"liquidity_sweep",
  "level_source":"equal_high_low",
  "timeframe":"15m",
  "min_touches":2,
  "breach_bps":2,
  "require_close_back_inside":true,
  "side":"either"
}
```

Pseudocode:

```text
for each active confirmed liquidity level:
    sell-side sweep = candle.high > level.price + tolerance
                      and candle.close < level.price
    buy-side sweep  = candle.low < level.price - tolerance
                      and candle.close > level.price
    emit once for (definition, level_id, excursion_id)
```

The level must be based only on swings confirmed before the evaluated candle.

### 13.2 “15m swing Fibonacci discount zone reached”

Current related data: Signals already computes a versioned premium/discount
structure, including high, low, equilibrium, and point-of-interest zone, but
the Alerts adapter currently ignores that output. No Alerts rule exists yet.

Required semantic decisions:

- which confirmed swing pair anchors the range;
- trend direction;
- exact Fibonacci bounds, for example 0.50–0.79;
- touch by wick, close, or current price;
- whether swing replacement invalidates an unfired zone;
- confirmed-only versus intrabar notification.

Pseudocode:

```text
anchor = latest fully confirmed directional swing pair
range = abs(anchor.high - anchor.low)

if bullish:
    zone_top    = high - range * lower_fib
    zone_bottom = high - range * upper_fib
else:
    zone_bottom = low + range * lower_fib
    zone_top    = low + range * upper_fib

reached = candle.high >= zone_bottom AND candle.low <= zone_top
emit once for (definition, anchor_version)
```

### 13.3 “1m volume much higher than average of last N candles”

Current status: not implemented. Volume exists in the Signals wire event but
is dropped by Alerts.

Recommended definition:

```json
{
  "type":"relative_volume",
  "lookback":20,
  "average":"sma",
  "minimum_ratio":2.5,
  "exclude_current":true
}
```

Confirmed-close pseudocode:

```text
require N prior confirmed candles
baseline = rolling_sum(previous N volumes) / N
match = current.volume >= baseline * minimum_ratio
after evaluation, evict oldest volume and add current volume
```

This is O(1) using a ring buffer and rolling sum. The current candle must be
excluded from its own baseline. An intrabar variant needs time-of-candle
normalization because comparing 20 seconds of accumulated volume with complete
historical candles is misleading.

### 13.4 “Candle made a certain wick”

Current status: not implemented, but existing OHLC data is sufficient.

```text
range      = high - low
body       = abs(close - open)
upper_wick = high - max(open, close)
lower_wick = min(open, close) - low

upper_ratio = upper_wick / max(range, epsilon)
lower_ratio = lower_wick / max(range, epsilon)
wick_to_body = selected_wick / max(body, epsilon)
```

The rule must specify side, minimum ratio, optional minimum absolute/basis-point
range, and whether dojis are allowed. Confirmed-close semantics are stable;
intrabar wick alerts can repaint and should be labeled provisional.

### 13.5 “Tick volatility increased”

Current status: not implementable correctly from the existing Alerts input.
One-second forming OHLC snapshots lose tick ordering and intermediate returns.

A production definition must name a metric, for example:

- standard deviation of log returns over the last N seconds;
- realized variance over a time/count window;
- high-low range normalized by price;
- ticks per second;
- spread widening;
- comparison with a rolling baseline or percentile.

Target state for a time-window realized-volatility rule:

```text
deque[(event_time, log_return)]
sum_returns
sum_squared_returns
baseline EWMA or longer-window distribution
```

Pseudocode:

```text
on ordered tick:
    r = log(mid_price / previous_mid_price)
    append(event_time, r)
    evict entries older than short_window
    short_variance = sum(r*r) - sum(r)^2 / count
    ratio = short_volatility / baseline_volatility
    emit on threshold crossing, subject to hysteresis and cooldown
```

This requires a partitioned tick stream or a dedicated upstream tick-feature
frame. It also needs an explicit late/out-of-order tick policy and much higher
capacity testing than candle-based rules.

### 13.6 “Price reached key level”

Current related data: Signals already calculates key-level outputs, but the
Alerts adapter ignores them. A static manually registered level can also be a
definition parameter. No generic key-level rule exists today.

Recommended match semantics use the candle's full range, not only its latest
close:

```text
tolerance = level.price * tolerance_bps / 10_000
reached = candle.high >= level.price - tolerance
          AND candle.low <= level.price + tolerance
```

For many levels, keep a price-sorted or interval index and inspect only levels
intersecting the changed price range. State should suppress repeats until price
leaves a configurable reset band, or key alerts will chatter around the level.

## 14. Required contract evolution

Before adding the planned rule family, evolve the normalized observation as a
versioned contract. At minimum it needs:

```text
candle.volume
candle.trades
confirmed swings with occurrence and confirmation timestamps
liquidity levels and sweep occurrences
premium/discount zones with anchor/version identity
key levels with stable IDs and source metadata
optional tick-feature frames or a separate tick contract
```

Do not infer feature meaning from display labels. Add typed internal fields and
stable identities. Preserve the original fact algorithm version/config hash in
the alert occurrence so a reviewer can reproduce why it fired.

## 15. Production gaps and recommended priorities

### P0 — correctness and operability before production

1. Define and test historical correction/replay behavior for stateful alerts.
2. Resolve confirmed/live cross-stream ordering during restart and backlog
   replay, or explicitly document weaker recovery guarantees for intrabar
   alerts.
3. Add retention/partitioning for provisional inbox data and long-lived event
   tables; storing every one-second payload forever is not viable.
4. Reconcile existing JetStream stream configuration at startup, not only
   create missing streams.
5. Fix consumer-policy reconciliation to update the stream passed to the
   helper; the current code uses `MARKET_FACTS` when reconciling a live
   consumer as well.
6. Enforce compatible rule/evaluation modes. For example, a rule whose reason
   says “candle closed” should not silently be allowed as intrabar without
   explicitly defined semantics.
7. Add input lag, per-shard lag, evaluation latency, rule-error, stale-event,
   outbox age, retry, dead-letter, and storage-growth alerts.
8. Deploy three-node JetStream and replicated PostgreSQL with backups/PITR;
   the current local deployment is single-node.
9. Run failure-injection tests for crashes before/after every database commit,
   publish acknowledgement, and source acknowledgement.

### P1 — performance before a large catalog

1. Compile/cache definitions and index them by series, cadence, and feature
   dependency.
2. Compute rolling features once per series and keep per-definition state
   minimal.
3. Batch or otherwise reduce state writes that do not generate alerts while
   retaining recoverability.
4. Partition PostgreSQL by series hash and/or event time when measured load
   requires it.
5. Make shard-count evolution a versioned routing migration rather than a
   constant change.
6. Benchmark hot-symbol and reconnect/replay workloads, not only uniform
   symbol distributions.

### P2 — rule-platform maintainability

1. Replace the expanding optional-field `Rule` struct with a typed evaluator
   registry and per-rule schema/version.
2. Add immutable definition revisions and migration rules for evaluator-state
   versions.
3. Build a deterministic replay harness using recorded input frames and golden
   alert events.
4. Add property tests for ordering, deduplication, cooldowns, no-lookahead,
   numeric edge cases, and composite rules.

## 16. Acceptance criteria for each new rule

Every new rule should provide:

1. a precise mathematical definition and parameter bounds;
2. its allowed cadence and finality/repaint behavior;
3. required fields/features and warm-up count;
4. event-time, late-data, and correction behavior;
5. pulse versus persistent-condition semantics;
6. deduplication and reset identity, such as bar, level, zone, or excursion;
7. bounded time and memory complexity;
8. deterministic unit vectors for positive, negative, boundary, and replay
   cases;
9. an integration test through broker, database transaction, outbox, and final
   alert stream;
10. observability and a capacity estimate.

## 17. Suggested capacity test matrix

Measure at least:

```text
series:                 100 / 1,000 / 10,000
definitions per series: 10 / 100 / 1,000
confirmed rate:         realistic mixed timeframes
intrabar rate:          1 frame/second/active series
tick rate:              representative median and burst percentiles
replicas:               1 / 2 / 4+
failure cases:          broker unavailable, DB failover, process crash,
                        reconnect replay, poison message, hot shard
```

Report sustained throughput, p50/p95/p99/p99.9 input-to-alert latency,
PostgreSQL IOPS/CPU/locks, broker lag, memory per active series, recovery time,
and duplicate-delivery rate.

## 18. Reviewer conclusion requested

The intended final shape is:

```text
one authoritative source of market facts
one centralized alert-definition and evaluation service
one immutable generated-alert feed
one separate user subscription and delivery layer
```

The current system implements this flow for a small rule catalog and correctly
keeps millions of user subscriptions outside market evaluation. The next major
engineering step should be a shared, incremental, versioned feature runtime
plus compiled definition indexing—not adding each windowed rule with its own
private history and database work.

External reviewers are specifically asked to challenge the feature/alert
boundary, event-time and correction semantics, the hot-shard model, PostgreSQL
write amplification, provisional-data retention, and the proposed
multi-timeframe/tick architecture.

## 19. Implementation source map

The principal implementation files reviewed are:

- [`internal/rules/evaluator.go`](../internal/rules/evaluator.go): validation,
  AST evaluation, and state advancement;
- [`internal/store/evaluate.go`](../internal/store/evaluate.go): stale checks,
  trigger modes, cooldown, and deterministic alert identity;
- [`internal/store/postgres.go`](../internal/store/postgres.go): transactional
  inbox, state, event, and outbox processing;
- [`internal/store/migrations/001_initial.sql`](../internal/store/migrations/001_initial.sql)
  and [`002_scale_indexes.sql`](../internal/store/migrations/002_scale_indexes.sql):
  database structures and indexes;
- [`internal/broker/jetstream.go`](../internal/broker/jetstream.go): fact
  consumers, sharding, retries, dead-letter handling, and alert publication;
- [`internal/signals/adapter.go`](../internal/signals/adapter.go): Signals to
  Alerts observation mapping;
- [`../../signals/internal/engine/lane.go`](../../signals/internal/engine/lane.go):
  ordered per-series calculations and persistence;
- [`../../signals/internal/indicator/market.go`](../../signals/internal/indicator/market.go):
  current market-feature algorithms;
- [`../../signals/internal/broker/jetstream.go`](../../signals/internal/broker/jetstream.go):
  confirmed and provisional fact publication; and
- [`../../signals/internal/api/contract.go`](../../signals/internal/api/contract.go):
  versioned public Signals event encoding.
