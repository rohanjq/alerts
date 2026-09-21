package signals

import (
	"encoding/json"
	"testing"
)

func TestObservationConvertsCompleteMarketEvent(t *testing.T) {
	var event Event
	raw := `{"id":"event-1","type":"io.ytstack.signals.analysis.updated.v1","time":"2026-09-07T10:15:00Z","correlationid":"bar-1","data":{"series":{"dataset":"spot","symbol":"BTCUSDT","timeframe":"15m"},"bar":{"open_time":"2026-09-07T10:00:00Z","revision":8,"status":"confirmed","source_id":"bar-1","open":101,"high":103,"low":98,"close":99},"analysis":{"name":"smc.market_state"},"analysis_revision":9,"outputs":[{"name":"ema_200","value":100},{"name":"zone_transitions","value":[{"label":"touched","direction":"bullish","attributes":{"zone_id":"fvg-1","kind":"fvg","transition":"touched","upper":102,"lower":100,"source_direction":"up"}}]},{"name":"pattern_occurrences","value":[{"label":"three_crows","direction":"bearish","attributes":{"source_direction":"down"}}]}]}}`
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatal(err)
	}
	o, err := Observation(event)
	if err != nil {
		t.Fatal(err)
	}
	if o.Revision != 9 || len(o.Indicators) != 1 || o.Indicators[0].Period != 200 || len(o.ZoneTransitions) != 1 || len(o.Patterns) != 1 {
		t.Fatalf("unexpected observation: %+v", o)
	}
}

func TestObservationCarriesAuthoritativeResetHistory(t *testing.T) {
	var event Event
	raw := `{"id":"reset-1","type":"io.ytstack.signals.analysis.updated.v1","time":"2026-09-07T10:15:00Z","correlationid":"bar-1","data":{"series":{"dataset":"spot","symbol":"BTCUSDT","timeframe":"15m"},"bar":{"open_time":"2026-09-07T10:00:00Z","revision":8,"status":"confirmed","source_id":"bar-1","open":100,"high":102,"low":99,"close":101},"reset":true,"history":[{"open_time":"2026-09-07T09:45:00Z","open":101,"high":102,"low":98,"close":99,"volume":12,"trades":8}],"analysis":{"name":"smc.market_state"},"analysis_revision":9,"implementation":{"engine":"ytstack-native","algorithm_version":"market-v1","config_hash":"sha256:x"},"outputs":[]}}`
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatal(err)
	}
	observation, err := Observation(event)
	if err != nil {
		t.Fatal(err)
	}
	if !observation.Reset || len(observation.History) != 1 || observation.History[0].Close != 99 {
		t.Fatalf("unexpected reset observation: %+v", observation)
	}
}
