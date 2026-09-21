package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rohanjq/alerts/internal/model"
	"github.com/rohanjq/alerts/internal/store"
)

func BenchmarkCompiledEvaluatorBatch(b *testing.B) {
	for _, definitionCount := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("definitions_%d", definitionCount), func(b *testing.B) {
			repository := store.NewMemory()
			series := model.Series{Dataset: "load", Symbol: "BTCUSDT", Timeframe: "1m"}
			start := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
			for index := 0; index < definitionCount; index++ {
				definition := model.AlertDefinition{
					ID: fmt.Sprintf("definition-%06d", index), DefinitionHash: fmt.Sprintf("hash-%06d", index), Version: 1,
					Name: "red", Enabled: true, Series: series, Rule: model.Rule{Type: "candle_color", Direction: "red"},
					EvaluationMode: "confirmed_close", TriggerMode: "every_match", CreatedAt: start.Add(-time.Hour), EffectiveAt: start.Add(-time.Hour),
				}
				if _, _, err := repository.CreateDefinition(context.Background(), definition); err != nil {
					b.Fatal(err)
				}
			}
			engine := New(repository, DefaultPartitions)
			if err := engine.Initialize(context.Background()); err != nil {
				b.Fatal(err)
			}
			partition := engine.Partition(series)
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				openTime := start.Add(time.Duration(index) * time.Minute)
				observation := model.Observation{
					Schema: "alerts.observation.v1", SourceEventID: fmt.Sprintf("event-%d", index), CorrelationID: fmt.Sprintf("bar-%d", index),
					OccurredAt: openTime.Add(time.Minute), Status: "confirmed", Revision: 1, Series: series,
					Candle: model.Candle{OpenTime: openTime, Open: 100, High: 102, Low: 99, Close: 101},
				}
				if _, err := engine.EvaluateBatch(context.Background(), partition, []model.Observation{observation}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
