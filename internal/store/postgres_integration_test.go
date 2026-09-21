package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/rohanjq/alerts/internal/model"
)

func TestPostgresRuntimeTransactionAndPartitionLeases(t *testing.T) {
	databaseURL := os.Getenv("ALERTS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("ALERTS_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	repository, err := OpenPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()

	owned := []int{142}
	if acquired, err := repository.AcquirePartitions(ctx, "integration-owner", owned, 15*time.Second); err != nil || !acquired {
		t.Fatalf("acquire=%v err=%v", acquired, err)
	}
	if acquired, err := repository.AcquirePartitions(ctx, "other-owner", owned, 15*time.Second); err != nil || acquired {
		t.Fatalf("conflicting acquire=%v err=%v", acquired, err)
	}
	if renewed, err := repository.RenewPartitions(ctx, "integration-owner", owned, 15*time.Second); err != nil || !renewed {
		t.Fatalf("renew=%v err=%v", renewed, err)
	}
	defer repository.ReleasePartitions(context.Background(), "integration-owner")

	start := time.Date(2026, 9, 10, 7, 0, 0, 0, time.UTC)
	series := model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}
	definition := model.AlertDefinition{
		ID: "11111111-1111-4111-8111-111111111111", Version: 1, DefinitionHash: "sha256:integration",
		Name: "integration red", Enabled: true, Series: series, Rule: model.Rule{Type: "candle_color", Direction: "red"},
		EvaluationMode: "confirmed_close", TriggerMode: "every_match", CreatedAt: start.Add(-time.Hour), UpdatedAt: start.Add(-time.Hour), EffectiveAt: start.Add(-time.Hour),
	}
	if _, _, err = repository.CreateDefinition(ctx, definition); err != nil {
		t.Fatal(err)
	}
	observation := model.Observation{
		Schema: "alerts.observation.v1", SourceEventID: "integration-source-1", CorrelationID: "integration-candle-1",
		OccurredAt: start.Add(time.Minute), Status: "confirmed", Revision: 2, Series: series,
		Candle:           model.Candle{OpenTime: start, Open: 101, High: 102, Low: 98, Close: 99},
		FeatureAlgorithm: model.AlgorithmRef{Name: "smc.market_state", Version: "v1", ConfigHash: "sha256:feature"},
	}
	payload, _ := json.Marshal(observation)
	alert := model.Alert{
		Schema: "io.ytstack.alert.triggered.v1", ID: "22222222-2222-4222-8222-222222222222",
		DefinitionID: definition.ID, DefinitionVersion: 1, DefinitionHash: definition.DefinitionHash, DefinitionName: definition.Name,
		SourceEventID: observation.SourceEventID, SourceCandleID: observation.CorrelationID, Series: series,
		TriggeredAt: observation.OccurredAt, EvaluatedAt: observation.OccurredAt.Add(time.Millisecond), Reasons: []string{"red candle closed"},
		FeatureAlgorithm: observation.FeatureAlgorithm, SourceRevision: observation.Revision, SourceStatus: observation.Status,
		Observation: payload, PublishStatus: "pending",
	}
	closeValue := observation.Candle.Close
	created, err := repository.CommitRuntimeBatch(ctx, RuntimeBatch{
		PartitionID: 142, Observations: []model.Observation{observation},
		SeriesStates:     map[string]model.SeriesEvaluationState{series.Key(): {LastOpenTime: start, LastRevision: 2, LastStatus: "confirmed", Confirmed: model.FeatureState{LastClose: &closeValue}}},
		DefinitionStates: map[string]model.DefinitionEvaluationState{DefinitionStateKey(definition.ID, series.Key()): {LastTruthConfirmed: true}},
		Alerts:           []model.Alert{alert},
	})
	if err != nil || len(created) != 1 || created[0].Cursor == 0 {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	snapshot, err := repository.LoadRuntimeStates(ctx, owned)
	if err != nil || snapshot.Series[series.Key()].LastRevision != 2 || !snapshot.Definitions[DefinitionStateKey(definition.ID, series.Key())].LastTruthConfirmed {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	pending, err := repository.ClaimPending(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].DefinitionHash != definition.DefinitionHash || pending[0].SourceCandleID != observation.CorrelationID {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
}
