package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rohanjq/alerts/internal/model"
	"github.com/rohanjq/alerts/internal/store"
)

func TestSharedSeriesHistoryServesMultipleCompiledDefinitions(t *testing.T) {
	repository := store.NewMemory()
	start := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	series := model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}
	for _, definition := range []model.AlertDefinition{
		{ID: "red-two", DefinitionHash: "red-two", Version: 1, Name: "two red", Enabled: true, Series: series, Rule: model.Rule{Type: "candle_streak", Direction: "red", Count: 2}, EvaluationMode: "confirmed_close", TriggerMode: "on_occurrence", CreatedAt: start.Add(-time.Hour), EffectiveAt: start.Add(-time.Hour)},
		{ID: "red-three", DefinitionHash: "red-three", Version: 1, Name: "three red", Enabled: true, Series: series, Rule: model.Rule{Type: "candle_streak", Direction: "red", Count: 3}, EvaluationMode: "confirmed_close", TriggerMode: "on_occurrence", CreatedAt: start.Add(-time.Hour), EffectiveAt: start.Add(-time.Hour)},
	} {
		if _, _, err := repository.CreateDefinition(context.Background(), definition); err != nil {
			t.Fatal(err)
		}
	}
	engine := New(repository, DefaultPartitions)
	if err := engine.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	observations := []model.Observation{
		confirmed("one", series, start, 101, 99),
		confirmed("two", series, start.Add(time.Minute), 101, 99),
		confirmed("three", series, start.Add(2*time.Minute), 101, 99),
	}
	alerts, err := engine.EvaluateBatch(context.Background(), engine.Partition(series), observations)
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 2 || alerts[0].DefinitionID != "red-two" || alerts[1].DefinitionID != "red-three" {
		t.Fatalf("unexpected alerts: %+v", alerts)
	}
	snapshot, err := repository.LoadRuntimeStates(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Series) != 1 || len(snapshot.Definitions) != 2 {
		t.Fatalf("series=%d definitions=%d", len(snapshot.Series), len(snapshot.Definitions))
	}
	if got := len(snapshot.Series[series.Key()].Confirmed.CandleColors); got != 3 {
		t.Fatalf("shared candle history length=%d", got)
	}
}

func TestFailedCheckpointDoesNotAdvanceMemory(t *testing.T) {
	memory := store.NewMemory()
	repository := &failingRepository{Repository: memory, fail: true}
	start := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	series := model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}
	definition := model.AlertDefinition{ID: "two", DefinitionHash: "two", Version: 1, Name: "two red", Enabled: true, Series: series, Rule: model.Rule{Type: "candle_streak", Direction: "red", Count: 2}, EvaluationMode: "confirmed_close", TriggerMode: "on_occurrence", CreatedAt: start.Add(-time.Hour), EffectiveAt: start.Add(-time.Hour)}
	if _, _, err := memory.CreateDefinition(context.Background(), definition); err != nil {
		t.Fatal(err)
	}
	engine := New(repository, DefaultPartitions)
	if err := engine.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := confirmed("one", series, start, 101, 99)
	if _, err := engine.EvaluateBatch(context.Background(), engine.Partition(series), []model.Observation{first}); err == nil {
		t.Fatal("expected checkpoint failure")
	}
	repository.fail = false
	if alerts, err := engine.EvaluateBatch(context.Background(), engine.Partition(series), []model.Observation{first}); err != nil || len(alerts) != 0 {
		t.Fatalf("retry first alerts=%v err=%v", alerts, err)
	}
	second := confirmed("two", series, start.Add(time.Minute), 101, 99)
	if alerts, err := engine.EvaluateBatch(context.Background(), engine.Partition(series), []model.Observation{second}); err != nil || len(alerts) != 1 {
		t.Fatalf("second alerts=%v err=%v", alerts, err)
	}
}

func TestProvisionalUsesCachedConfirmedZones(t *testing.T) {
	repository := store.NewMemory()
	start := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	series := model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}
	definition := model.AlertDefinition{ID: "near", DefinitionHash: "near", Version: 1, Name: "near FVG", Enabled: true, Series: series, Rule: model.Rule{Type: "price_near_zone", Kind: "fvg", DistanceBps: 20}, EvaluationMode: "intrabar", TriggerMode: "once_per_bar", CreatedAt: start.Add(-time.Hour), EffectiveAt: start.Add(-time.Hour)}
	if _, _, err := repository.CreateDefinition(context.Background(), definition); err != nil {
		t.Fatal(err)
	}
	engine := New(repository, DefaultPartitions)
	if err := engine.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	confirmedObservation := confirmed("confirmed", series, start, 100, 100)
	confirmedObservation.ActiveZones = []model.Zone{{ID: "fvg-1", Kind: "fvg", Side: "up", State: "active", Lower: 100.10, Upper: 101}}
	forming := model.Observation{Schema: "alerts.observation.v1", SourceEventID: "forming", CorrelationID: "forming", OccurredAt: start.Add(time.Minute + 10*time.Second), Status: "provisional", Revision: 2, Series: series, Candle: model.Candle{OpenTime: start.Add(time.Minute), Open: 100, High: 100.05, Low: 99.9, Close: 100}}
	alerts, err := engine.EvaluateBatch(context.Background(), engine.Partition(series), []model.Observation{confirmedObservation, forming})
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 1 || alerts[0].DefinitionID != "near" || len(alerts[0].FactsUsed) != 1 || alerts[0].FactsUsed[0].ID != "fvg-1" {
		t.Fatalf("unexpected alerts: %+v", alerts)
	}
}

func TestAuthoritativeResetRebuildsSharedHistoryWithoutEmitting(t *testing.T) {
	repository := store.NewMemory()
	start := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	series := model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}
	definition := model.AlertDefinition{ID: "two", DefinitionHash: "two", Version: 1, Name: "two red", Enabled: true, Series: series, Rule: model.Rule{Type: "candle_streak", Direction: "red", Count: 2}, EvaluationMode: "confirmed_close", TriggerMode: "on_occurrence", CreatedAt: start.Add(-time.Hour), EffectiveAt: start.Add(-time.Hour)}
	if _, _, err := repository.CreateDefinition(context.Background(), definition); err != nil {
		t.Fatal(err)
	}
	engine := New(repository, DefaultPartitions)
	if err := engine.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := confirmed("first", series, start, 101, 99)
	second := confirmed("second", series, start.Add(time.Minute), 101, 99)
	if alerts, err := engine.EvaluateBatch(context.Background(), engine.Partition(series), []model.Observation{first, second}); err != nil || len(alerts) != 1 {
		t.Fatalf("initial alerts=%v err=%v", alerts, err)
	}
	reset := confirmed("reset", series, start.Add(time.Minute), 100, 101)
	reset.Reset = true
	reset.History = []model.Candle{{OpenTime: start, Open: 100, High: 101, Low: 99, Close: 101}}
	if alerts, err := engine.EvaluateBatch(context.Background(), engine.Partition(series), []model.Observation{reset}); err != nil || len(alerts) != 0 {
		t.Fatalf("reset alerts=%v err=%v", alerts, err)
	}
	third := confirmed("third", series, start.Add(2*time.Minute), 101, 99)
	if alerts, err := engine.EvaluateBatch(context.Background(), engine.Partition(series), []model.Observation{third}); err != nil || len(alerts) != 0 {
		t.Fatalf("first red after reset alerts=%v err=%v", alerts, err)
	}
	fourth := confirmed("fourth", series, start.Add(3*time.Minute), 101, 99)
	if alerts, err := engine.EvaluateBatch(context.Background(), engine.Partition(series), []model.Observation{fourth}); err != nil || len(alerts) != 1 {
		t.Fatalf("second red after reset alerts=%v err=%v", alerts, err)
	}
}

func TestFactsUsedIdentifiesTheZoneThatActuallyMatched(t *testing.T) {
	repository := store.NewMemory()
	start := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	series := model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}
	definition := model.AlertDefinition{ID: "near", DefinitionHash: "near-exact", Version: 1, Name: "near FVG", Enabled: true, Series: series, Rule: model.Rule{Type: "price_near_zone", Kind: "fvg", DistanceBps: 20}, EvaluationMode: "confirmed_close", TriggerMode: "every_match", CreatedAt: start.Add(-time.Hour), EffectiveAt: start.Add(-time.Hour)}
	if _, _, err := repository.CreateDefinition(context.Background(), definition); err != nil {
		t.Fatal(err)
	}
	engine := New(repository, DefaultPartitions)
	if err := engine.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	observation := confirmed("near-event", series, start, 100, 100)
	observation.ActiveZones = []model.Zone{
		{ID: "far", Kind: "fvg", State: "active", Lower: 110, Upper: 111},
		{ID: "matched", Kind: "fvg", State: "active", Lower: 100.10, Upper: 101},
	}
	alerts, err := engine.EvaluateBatch(context.Background(), engine.Partition(series), []model.Observation{observation})
	if err != nil || len(alerts) != 1 || len(alerts[0].FactsUsed) != 1 || alerts[0].FactsUsed[0].ID != "matched" {
		t.Fatalf("alerts=%+v err=%v", alerts, err)
	}
}

func TestWorkerRejectsPartitionItDoesNotOwn(t *testing.T) {
	repository := store.NewMemory()
	series := model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}
	partition := New(repository, DefaultPartitions).Partition(series)
	workerIndex := (partition + 1) % 2
	engine := NewWithOwnership(repository, DefaultPartitions, workerIndex, 2)
	observation := confirmed("wrong-owner", series, time.Now().UTC().Truncate(time.Minute), 100, 101)
	if _, err := engine.EvaluateBatch(context.Background(), partition, []model.Observation{observation}); err == nil {
		t.Fatal("expected non-owner evaluation to be rejected")
	}
}

func TestPartitionHashMatchesSignalsProtocol(t *testing.T) {
	series := model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}
	if got := New(store.NewMemory(), DefaultPartitions).Partition(series); got != 142 {
		t.Fatalf("partition=%d want=142; Signals and Alerts routing protocol drifted", got)
	}
}

type failingRepository struct {
	store.Repository
	fail bool
}

func (r *failingRepository) CommitRuntimeBatch(ctx context.Context, batch store.RuntimeBatch) ([]model.Alert, error) {
	if r.fail {
		return nil, errors.New("injected checkpoint failure")
	}
	return r.Repository.CommitRuntimeBatch(ctx, batch)
}

func confirmed(id string, series model.Series, openTime time.Time, open, close float64) model.Observation {
	return model.Observation{Schema: "alerts.observation.v1", SourceEventID: id, CorrelationID: id, OccurredAt: openTime.Add(time.Minute), Status: "confirmed", Revision: 1, Series: series, Candle: model.Candle{OpenTime: openTime, Open: open, High: max(open, close), Low: min(open, close), Close: close}}
}
