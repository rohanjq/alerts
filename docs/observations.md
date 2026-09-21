# Observation contract

The assembler emits one complete confirmed-bar observation after required
analyses are available. `source_event_id` is deterministic from series, bar,
source revision, and analysis configuration versions.

```json
{
  "schema": "alerts.observation.v1",
  "source_event_id": "live/BTCUSDT/15m/2026-09-07T10:00:00Z/r7",
  "correlation_id": "source-candle-id",
  "occurred_at": "2026-09-07T10:15:00Z",
  "status": "confirmed",
  "revision": 7,
  "series": {"dataset":"live","symbol":"BTCUSDT","timeframe":"15m"},
  "candle": {"open_time":"2026-09-07T10:00:00Z","open":101,"high":102,"low":98,"close":99},
  "indicators": [{"name":"ema","period":200,"value":100,"ready":true}],
  "active_zones": [{"id":"fvg-1","kind":"fvg","side":"up","state":"active","upper":102,"lower":100}],
  "zone_transitions": [{"id":"ob-1","kind":"order_block","side":"up","transition":"touched","upper":99.5,"lower":98}],
  "patterns": [{"kind":"three_crows","side":"down"}]
}
```

Examples of rule JSON:

```json
{"type":"price_crosses_indicator","name":"ema","period":200,"direction":"below"}
```

```json
{"type":"price_near_zone","kind":"fvg","side":"up","distance_bps":10}
```

```json
{"type":"candle_color_flip","direction":"either","min_elapsed_percent":50}
```

The flip rule is evaluated on `status: "provisional"` observations. It compares
the current forming close with that candle's open, remembers the last non-doji
colour, and emits when the side changes at or after the configured percentage
of the timeframe. Use `evaluation_mode: "intrabar"`; `once_per_bar` limits it
to the first qualifying flip in each candle.

```json
{"type":"all","rules":[
  {"type":"price_hits_zone","kind":"order_block","transition":"touched"},
  {"type":"pattern","kind":"bull_engulfing"}
]}
```

The preferred endpoint is `POST /v1/internal/signal-events`, which converts the
complete Signals `smc.market_state` event without an external join. The normalized
internal ingest endpoint is service-to-service and must never be exposed to
mobile or browser clients. Generated events are available from `GET /v1/events`
and are published to the durable `ALERT_EVENTS` stream for downstream subscribers.

Signals may send a confirmed observation with `"reset":true` and a bounded
`history` array after correcting authoritative OHLC history. Alerts uses it only
to rebuild future series/definition state. It emits no alerts for the reset and
does not retract immutable alerts already generated live. Duplicate reset IDs
and older reset timestamps are ignored.
