package rules

import (
	"testing"
	"time"

	"github.com/rohanjq/alerts/internal/model"
)

func observation(open, close float64) model.Observation {
	return model.Observation{Schema: "alerts.observation.v1", SourceEventID: "event", OccurredAt: time.Now(), Series: model.Series{Dataset: "binance-spot", Symbol: "BTCUSDT", Timeframe: "15m"}, Candle: model.Candle{OpenTime: time.Now(), Open: open, High: max(open, close), Low: min(open, close), Close: close}}
}

func TestCrossEMA(t *testing.T) {
	previousClose := 99.0
	state := model.FeatureState{LastClose: &previousClose, Indicators: map[string]float64{"ema:200": 100}}
	o := observation(99, 102)
	o.Indicators = []model.Indicator{{Name: "ema", Period: 200, Value: 101, Ready: true}}
	matched, _ := Evaluate(model.Rule{Type: "price_crosses_indicator", Name: "ema", Period: 200, Direction: "above"}, o, state)
	if !matched {
		t.Fatal("expected upward EMA cross")
	}
}

func TestCombinedZoneAndPattern(t *testing.T) {
	o := observation(100, 101)
	o.ZoneTransitions = []model.ZoneTransition{{ID: "z1", Kind: "fvg", Side: "up", Transition: "touched", Upper: 102, Lower: 100}}
	o.Patterns = []model.Pattern{{Kind: "three_soldiers", Side: "up"}}
	rule := model.Rule{Type: "all", Rules: []model.Rule{{Type: "price_hits_zone", Kind: "fvg", Transition: "touched"}, {Type: "pattern", Kind: "three_soldiers"}}}
	matched, reasons := Evaluate(rule, o, model.FeatureState{})
	if !matched || len(reasons) != 2 {
		t.Fatalf("matched=%v reasons=%v", matched, reasons)
	}
}

func TestCandleStreakEdges(t *testing.T) {
	rule := model.Rule{Type: "candle_streak", Direction: "red", Count: 3}
	state := model.FeatureState{CandleColors: []string{"red", "red"}}
	matched, _ := Evaluate(rule, observation(102, 100), state)
	if !matched {
		t.Fatal("expected third red candle to match")
	}
}

func TestCandleColor(t *testing.T) {
	red, reasons := Evaluate(model.Rule{Type: "candle_color", Direction: "red"}, observation(102, 100), model.FeatureState{})
	if !red || len(reasons) != 1 {
		t.Fatalf("red=%v reasons=%v", red, reasons)
	}
	green, _ := Evaluate(model.Rule{Type: "candle_color", Direction: "green"}, observation(100, 102), model.FeatureState{})
	if !green {
		t.Fatal("expected green candle to match")
	}
	doji, _ := Evaluate(model.Rule{Type: "candle_color", Direction: "green"}, observation(100, 100), model.FeatureState{})
	if doji {
		t.Fatal("doji must not match green candle")
	}
}

func TestFormingUpdatesDoNotAdvanceCandleStreak(t *testing.T) {
	state := model.FeatureState{CandleColors: []string{"red", "green"}}
	o := observation(102, 100)
	o.Status = "provisional"
	state = Advance(state, o)
	state = Advance(state, o)
	if len(state.CandleColors) != 2 {
		t.Fatalf("forming revisions changed candle history: %v", state.CandleColors)
	}
	o.Status = "confirmed"
	state = Advance(state, o)
	if len(state.CandleColors) != 3 || state.CandleColors[2] != "red" {
		t.Fatalf("confirmed candle was not appended once: %v", state.CandleColors)
	}
}

func TestCandleColorFlipAfterElapsedThreshold(t *testing.T) {
	start := time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC)
	rule := model.Rule{Type: "candle_color_flip", Direction: "either", MinElapsedPercent: 50}
	state := model.FeatureState{}

	green := observation(100, 101)
	green.Status, green.Series.Timeframe, green.Candle.OpenTime = "provisional", "1m", start
	green.OccurredAt = start.Add(5 * time.Second)
	state = Advance(state, green)

	redTooEarly := observation(100, 99)
	redTooEarly.Status, redTooEarly.Series.Timeframe, redTooEarly.Candle.OpenTime = "provisional", "1m", start
	redTooEarly.OccurredAt = start.Add(29 * time.Second)
	if matched, _ := Evaluate(rule, redTooEarly, state); matched {
		t.Fatal("flip before 50% must not match")
	}
	state = Advance(state, redTooEarly)

	greenAfterThreshold := green
	greenAfterThreshold.OccurredAt = start.Add(31 * time.Second)
	matched, reasons := Evaluate(rule, greenAfterThreshold, state)
	if !matched || len(reasons) != 1 {
		t.Fatalf("matched=%v reasons=%v", matched, reasons)
	}
}

func TestCandleColorFlipRequiresSameFormingBar(t *testing.T) {
	start := time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC)
	rule := model.Rule{Type: "candle_color_flip", Direction: "green_to_red", MinElapsedPercent: 50}
	o := observation(100, 99)
	o.Status, o.Series.Timeframe, o.Candle.OpenTime = "provisional", "1m", start
	o.OccurredAt = start.Add(30 * time.Second)
	state := model.FeatureState{FormingBar: start.Add(-time.Minute), FormingColor: "green"}
	if matched, _ := Evaluate(rule, o, state); matched {
		t.Fatal("colour from a previous candle must not count as a flip")
	}
}

func TestNearZoneUsesBasisPoints(t *testing.T) {
	o := observation(100, 100)
	o.ActiveZones = []model.Zone{{ID: "z1", Kind: "fvg", State: "active", Lower: 100.05, Upper: 101}}
	matched, _ := Evaluate(model.Rule{Type: "price_near_zone", Kind: "fvg", DistanceBps: 6}, o, model.FeatureState{})
	if !matched {
		t.Fatal("expected price within 6 bps of FVG")
	}
}
