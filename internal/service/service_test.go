package service

import (
	"context"
	"testing"
	"time"

	"github.com/rohanjq/alerts/internal/model"
	"github.com/rohanjq/alerts/internal/store"
)

func TestIngestIsIdempotentAndTriggersOnRisingEdge(t *testing.T) {
	s := New(store.NewMemory(), nil, nil)
	start := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return start.Add(-time.Minute) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.RunPublisher(ctx)
	_, _, err := s.Create(context.Background(), "Three red 15m candles", model.Series{Symbol: "BTCUSDT", Timeframe: "15m"}, model.Rule{Type: "candle_streak", Direction: "red", Count: 3}, "confirmed_close", "on_occurrence", 0)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		o := model.Observation{Schema: "alerts.observation.v1", SourceEventID: string(rune('a' + index)), CorrelationID: "bar-" + string(rune('a'+index)), Status: "confirmed", Revision: 1, OccurredAt: start.Add(time.Duration(index) * 15 * time.Minute), Series: model.Series{Symbol: "BTCUSDT", Timeframe: "15m"}, Candle: model.Candle{OpenTime: start.Add(time.Duration(index) * 15 * time.Minute), Open: 101, High: 102, Low: 99, Close: 100}}
		alerts, ingestErr := s.Ingest(context.Background(), o)
		if ingestErr != nil {
			t.Fatal(ingestErr)
		}
		if index < 2 && len(alerts) != 0 {
			t.Fatalf("early alerts=%v", alerts)
		}
		if index == 2 && len(alerts) != 1 {
			t.Fatalf("alerts=%v", alerts)
		}
		if index == 2 {
			duplicate, _ := s.Ingest(context.Background(), o)
			if len(duplicate) != 0 {
				t.Fatal("duplicate event triggered")
			}
		}
	}
}

func TestDistinctPatternOccurrencesBothTrigger(t *testing.T) {
	s := New(store.NewMemory(), nil, nil)
	_, _, err := s.Create(context.Background(), "Three crows", model.Series{Symbol: "BTCUSDT", Timeframe: "15m"}, model.Rule{Type: "pattern", Kind: "three_crows"}, "confirmed_close", "on_occurrence", 0)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		now := time.Now().Add(time.Duration(index) * time.Minute)
		o := model.Observation{Schema: "alerts.observation.v1", SourceEventID: string(rune('x' + index)), CorrelationID: "bar-x", Status: "confirmed", Revision: 1, OccurredAt: now, Series: model.Series{Symbol: "BTCUSDT", Timeframe: "15m"}, Candle: model.Candle{OpenTime: now}, Patterns: []model.Pattern{{Kind: "three_crows"}}}
		alerts, ingestErr := s.Ingest(context.Background(), o)
		if ingestErr != nil || len(alerts) != 1 {
			t.Fatalf("iteration=%d alerts=%v err=%v", index, alerts, ingestErr)
		}
	}
}

func TestReadyIncludesExternalDependencies(t *testing.T) {
	ready := false
	s := New(store.NewMemory(), nil, nil, func(context.Context) bool { return ready })
	if s.Ready(context.Background()) {
		t.Fatal("service reported ready while dependency was unavailable")
	}
	ready = true
	if !s.Ready(context.Background()) {
		t.Fatal("service did not become ready when repository and dependency were available")
	}
}

func TestCandleFlipDefinitionRequiresIntrabarMode(t *testing.T) {
	s := New(store.NewMemory(), nil, nil)
	rule := model.Rule{Type: "candle_color_flip", Direction: "either", MinElapsedPercent: 50}
	_, _, err := s.Create(context.Background(), "flip", model.Series{Symbol: "BTCUSDT", Timeframe: "1m"}, rule, "confirmed_close", "once_per_bar", 0)
	if err == nil {
		t.Fatal("expected confirmed-only flip definition to be rejected")
	}
	if _, _, err = s.Create(context.Background(), "flip", model.Series{Symbol: "BTCUSDT", Timeframe: "1m"}, rule, "intrabar", "once_per_bar", 0); err != nil {
		t.Fatalf("intrabar flip definition rejected: %v", err)
	}
}

func TestDefinitionEnableAndDisableUseSharedEventTimeBoundary(t *testing.T) {
	repository := store.NewMemory()
	service := NewWithOptions(repository, nil, nil, Options{ActivationDelay: 2 * time.Second})
	start := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return start }
	series := model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}
	definition, _, err := service.Create(context.Background(), "red", series, model.Rule{Type: "candle_color", Direction: "red"}, "confirmed_close", "every_match", 0)
	if err != nil {
		t.Fatal(err)
	}
	before := model.Observation{Schema: "alerts.observation.v1", SourceEventID: "before", CorrelationID: "before", Status: "confirmed", Revision: 1, OccurredAt: start.Add(time.Second), Series: series, Candle: model.Candle{OpenTime: start, Open: 101, High: 102, Low: 99, Close: 100}}
	if alerts, err := service.Ingest(context.Background(), before); err != nil || len(alerts) != 0 {
		t.Fatalf("before activation alerts=%v err=%v", alerts, err)
	}
	after := before
	after.SourceEventID, after.CorrelationID = "after", "after"
	after.Candle.OpenTime = start.Add(time.Minute)
	after.OccurredAt = start.Add(2 * time.Second)
	if alerts, err := service.Ingest(context.Background(), after); err != nil || len(alerts) != 1 {
		t.Fatalf("at activation alerts=%v err=%v", alerts, err)
	}
	service.now = func() time.Time { return start.Add(3 * time.Second) }
	if _, err = service.SetEnabled(context.Background(), definition.ID, false); err != nil {
		t.Fatal(err)
	}
	pendingDisable := before
	pendingDisable.SourceEventID, pendingDisable.CorrelationID = "pending-disable", "pending-disable"
	pendingDisable.Candle.OpenTime = start.Add(2 * time.Minute)
	pendingDisable.OccurredAt = start.Add(4 * time.Second)
	if alerts, err := service.Ingest(context.Background(), pendingDisable); err != nil || len(alerts) != 1 {
		t.Fatalf("pending disable alerts=%v err=%v", alerts, err)
	}
	disabled := before
	disabled.SourceEventID, disabled.CorrelationID = "disabled", "disabled"
	disabled.Candle.OpenTime = start.Add(3 * time.Minute)
	disabled.OccurredAt = start.Add(5 * time.Second)
	if alerts, err := service.Ingest(context.Background(), disabled); err != nil || len(alerts) != 0 {
		t.Fatalf("disabled alerts=%v err=%v", alerts, err)
	}
}
