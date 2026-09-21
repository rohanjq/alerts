package signals

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rohanjq/alerts/internal/model"
)

const AnalysisEventType = "io.ytstack.signals.analysis.updated.v1"

type Event struct {
	ID            string    `json:"id"`
	Type          string    `json:"type"`
	Time          time.Time `json:"time"`
	CorrelationID string    `json:"correlationid"`
	Data          struct {
		Series model.Series `json:"series"`
		Bar    struct {
			OpenTime time.Time `json:"open_time"`
			Revision uint64    `json:"revision"`
			Status   string    `json:"status"`
			SourceID string    `json:"source_id"`
			Open     float64   `json:"open"`
			High     float64   `json:"high"`
			Low      float64   `json:"low"`
			Close    float64   `json:"close"`
			Volume   float64   `json:"volume"`
			Trades   uint64    `json:"trades"`
		} `json:"bar"`
		Analysis struct {
			Name string `json:"name"`
		} `json:"analysis"`
		AnalysisRevision uint64 `json:"analysis_revision"`
		Reset            bool   `json:"reset,omitempty"`
		History          []struct {
			OpenTime time.Time `json:"open_time"`
			Open     float64   `json:"open"`
			High     float64   `json:"high"`
			Low      float64   `json:"low"`
			Close    float64   `json:"close"`
			Volume   float64   `json:"volume"`
			Trades   uint64    `json:"trades"`
		} `json:"history,omitempty"`
		Implementation struct {
			Engine           string `json:"engine"`
			EngineVersion    string `json:"engine_version"`
			AlgorithmVersion string `json:"algorithm_version"`
			ConfigHash       string `json:"config_hash"`
		} `json:"implementation"`
		Outputs []struct {
			Name  string          `json:"name"`
			Value json.RawMessage `json:"value"`
		} `json:"outputs"`
	} `json:"data"`
}

type zone struct {
	ID         string         `json:"id"`
	Upper      float64        `json:"upper"`
	Lower      float64        `json:"lower"`
	Label      string         `json:"label"`
	Direction  string         `json:"direction"`
	State      string         `json:"state"`
	Attributes map[string]any `json:"attributes"`
}

type point struct {
	Label      string         `json:"label"`
	Direction  string         `json:"direction"`
	Attributes map[string]any `json:"attributes"`
}

// Observation converts the complete smc.market_state event emitted by signald.
// Indicator-only events are deliberately rejected so a candle is advanced once.
func Observation(event Event) (model.Observation, error) {
	if event.Type != AnalysisEventType || event.ID == "" || event.CorrelationID == "" {
		return model.Observation{}, fmt.Errorf("invalid Signals event envelope")
	}
	if event.Data.Analysis.Name != "smc.market_state" {
		return model.Observation{}, fmt.Errorf("only complete smc.market_state events are accepted")
	}
	if event.Data.Bar.SourceID == "" || event.Data.Bar.SourceID != event.CorrelationID {
		return model.Observation{}, fmt.Errorf("Signals candle correlation does not match")
	}
	o := model.Observation{
		Schema: "alerts.observation.v1", SourceEventID: event.ID, CorrelationID: event.CorrelationID,
		OccurredAt: event.Time.UTC(), Status: event.Data.Bar.Status, Revision: event.Data.AnalysisRevision,
		Reset:  event.Data.Reset,
		Series: event.Data.Series, Candle: model.Candle{OpenTime: event.Data.Bar.OpenTime.UTC(), Open: event.Data.Bar.Open, High: event.Data.Bar.High, Low: event.Data.Bar.Low, Close: event.Data.Bar.Close, Volume: event.Data.Bar.Volume, Trades: event.Data.Bar.Trades},
		FeatureAlgorithm: model.AlgorithmRef{Name: event.Data.Analysis.Name, Version: event.Data.Implementation.AlgorithmVersion, ConfigHash: event.Data.Implementation.ConfigHash},
	}
	for _, bar := range event.Data.History {
		o.History = append(o.History, model.Candle{OpenTime: bar.OpenTime.UTC(), Open: bar.Open, High: bar.High, Low: bar.Low, Close: bar.Close, Volume: bar.Volume, Trades: bar.Trades})
	}
	for _, output := range event.Data.Outputs {
		switch {
		case strings.HasPrefix(output.Name, "ema_"):
			period, err := strconv.Atoi(strings.TrimPrefix(output.Name, "ema_"))
			var value float64
			if err != nil || json.Unmarshal(output.Value, &value) != nil {
				return model.Observation{}, fmt.Errorf("invalid %s output", output.Name)
			}
			o.Indicators = append(o.Indicators, model.Indicator{Name: "ema", Period: period, Value: value, Ready: true})
		case output.Name == "fvgs" || output.Name == "order_blocks":
			var values []zone
			if err := json.Unmarshal(output.Value, &values); err != nil {
				return model.Observation{}, fmt.Errorf("invalid %s output: %w", output.Name, err)
			}
			for _, value := range values {
				o.ActiveZones = append(o.ActiveZones, model.Zone{ID: value.ID, Kind: value.Label, Side: sourceSide(value.Direction, value.Attributes), State: value.State, Upper: value.Upper, Lower: value.Lower})
			}
		case output.Name == "zone_transitions":
			var values []point
			if err := json.Unmarshal(output.Value, &values); err != nil {
				return model.Observation{}, fmt.Errorf("invalid zone_transitions output: %w", err)
			}
			for _, value := range values {
				o.ZoneTransitions = append(o.ZoneTransitions, model.ZoneTransition{ID: text(value.Attributes["zone_id"]), Kind: text(value.Attributes["kind"]), Side: sourceSide(value.Direction, value.Attributes), Transition: text(value.Attributes["transition"]), Upper: number(value.Attributes["upper"]), Lower: number(value.Attributes["lower"])})
			}
		case output.Name == "pattern_occurrences":
			var values []point
			if err := json.Unmarshal(output.Value, &values); err != nil {
				return model.Observation{}, fmt.Errorf("invalid pattern_occurrences output: %w", err)
			}
			for _, value := range values {
				o.Patterns = append(o.Patterns, model.Pattern{Kind: value.Label, Side: sourceSide(value.Direction, value.Attributes)})
			}
		}
	}
	return o, nil
}

func sourceSide(direction string, attributes map[string]any) string {
	if side := text(attributes["source_direction"]); side != "" {
		return side
	}
	if direction == "bullish" {
		return "up"
	}
	if direction == "bearish" {
		return "down"
	}
	return ""
}

func text(value any) string {
	result, _ := value.(string)
	return result
}

func number(value any) float64 {
	result, _ := value.(float64)
	return result
}
