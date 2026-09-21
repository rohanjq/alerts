# Portable alert definitions

Alert definitions are backed up as versioned JSON manifests rather than SQL
dumps. The format contains only authored configuration. IDs, hashes, versions,
and timestamps are deliberately omitted and are regenerated deterministically
by the destination Alerts service.

The checked-in [`definitions/seed.json`](../definitions/seed.json) contains the
five active definitions recovered from the local `alertsd` PostgreSQL database.
It contains no credentials or runtime evaluation state.

## Load the seed

Start `alertsd`, set its control-plane token, and run:

```bash
ALERTS_API_URL=http://127.0.0.1:8100 \
ALERTS_API_TOKEN="$ALERTS_API_TOKEN" \
go run ./cmd/alertdefs import -file definitions/seed.json
```

Import is idempotent because the service derives identity from a definition's
series, rule, evaluation mode, trigger mode, and cooldown. It also applies the
manifest's `enabled` state. If a run is interrupted, run the same command again.

For an insecure local process, omit `ALERTS_API_TOKEN`. When using the existing
local container bound to port 18100, set `ALERTS_API_URL=http://127.0.0.1:18100`.

## Export the current definitions

Write a stable, reviewable manifest atomically:

```bash
ALERTS_API_URL=http://127.0.0.1:8100 \
ALERTS_API_TOKEN="$ALERTS_API_TOKEN" \
go run ./cmd/alertdefs export -out definitions/seed.json
```

Use `-out -` (the default) to print JSON to standard output. Import similarly
accepts standard input when `-file -` is used. Prefer the environment variable
for the token so it does not appear in shell history or the process list.

## Format

```json
{
  "schema": "alerts.definitions.v1",
  "definitions": [
    {
      "name": "Three red 15m candles",
      "enabled": true,
      "series": {
        "dataset": "live",
        "symbol": "BTCUSDT",
        "timeframe": "15m"
      },
      "rule": {
        "type": "candle_streak",
        "direction": "red",
        "count": 3
      },
      "evaluation_mode": "confirmed_close",
      "trigger_mode": "on_occurrence",
      "cooldown_seconds": 900
    }
  ]
}
```

`enabled` defaults to `true` when omitted. `evaluation_mode` and
`trigger_mode` use the same service defaults as the create API. Unknown fields,
unsupported schema versions, and invalid rules are rejected before import.
