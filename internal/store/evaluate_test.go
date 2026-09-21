package store

import (
	"testing"
	"time"

	"github.com/rohanjq/alerts/internal/model"
)

func TestCandleFlipOncePerBar(t *testing.T) {
	start := time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC)
	definition := model.AlertDefinition{ID: "definition", Version: 1, Name: "flip", TriggerMode: "once_per_bar",
		Rule: model.Rule{Type: "candle_color_flip", Direction: "either", MinElapsedPercent: 50}}
	observation := func(id string, revision uint64, at time.Duration, close float64) model.Observation {
		return model.Observation{Schema: "alerts.observation.v1", SourceEventID: id, Status: "provisional", Revision: revision,
			OccurredAt: start.Add(at), Series: model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"},
			Candle: model.Candle{OpenTime: start, Open: 100, High: max(100, close), Low: min(100, close), Close: close}}
	}

	state, alert, accepted := apply(definition, observation("green", 1, 10*time.Second, 101), model.EvaluationState{})
	if !accepted || alert != nil {
		t.Fatalf("initial state accepted=%v alert=%v", accepted, alert)
	}
	state, alert, accepted = apply(definition, observation("red", 2, 31*time.Second, 99), state)
	if !accepted || alert == nil {
		t.Fatalf("post-midpoint flip accepted=%v alert=%v", accepted, alert)
	}
	_, second, accepted := apply(definition, observation("green-again", 3, 40*time.Second, 101), state)
	if !accepted || second != nil {
		t.Fatalf("once_per_bar emitted a second alert: accepted=%v alert=%v", accepted, second)
	}
}
