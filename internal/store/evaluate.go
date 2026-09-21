package store

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/rohanjq/alerts/internal/model"
	"github.com/rohanjq/alerts/internal/rules"
)

func apply(definition model.AlertDefinition, observation model.Observation, state model.EvaluationState) (model.EvaluationState, *model.Alert, bool) {
	now := observation.OccurredAt.UTC()
	if stale(observation, state) {
		return state, nil, false
	}
	feature := model.FeatureState{LastClose: state.LastClose, Indicators: state.Indicators, CandleColors: state.CandleColors, FormingBar: state.FormingBar, FormingColor: state.FormingColor}
	compiled, compileErr := rules.Compile(definition.Rule)
	if compileErr != nil {
		return state, nil, false
	}
	truth, reasons, facts := compiled.EvaluateDetailed(observation, feature)
	trigger := shouldTrigger(definition.TriggerMode, truth, rules.Pulse(definition.Rule), observation, state)
	if trigger && definition.CooldownSeconds > 0 && !state.LastTriggerAt.IsZero() && now.Sub(state.LastTriggerAt) < time.Duration(definition.CooldownSeconds)*time.Second {
		trigger = false
	}
	feature = rules.Advance(feature, observation)
	state.LastClose, state.Indicators, state.CandleColors = feature.LastClose, feature.Indicators, feature.CandleColors
	state.FormingBar, state.FormingColor = feature.FormingBar, feature.FormingColor
	state.LastOpenTime, state.LastRevision, state.LastStatus = observation.Candle.OpenTime.UTC(), observation.Revision, observation.Status
	if !trigger {
		return state, nil, true
	}
	state.LastTriggerAt, state.LastAlertBar = now.UTC(), observation.Candle.OpenTime.UTC()
	alert := &model.Alert{
		Schema: "io.ytstack.alert.triggered.v1", ID: deterministicID(definition.ID, observation.SourceEventID),
		DefinitionID: definition.ID, DefinitionVersion: definition.Version, DefinitionHash: definition.DefinitionHash, DefinitionName: definition.Name,
		SourceEventID: observation.SourceEventID, SourceCandleID: observation.CorrelationID, Series: observation.Series, TriggeredAt: now.UTC(),
		EvaluatedAt: time.Now().UTC(), Reasons: reasons, FactsUsed: facts,
		FeatureAlgorithm: observation.FeatureAlgorithm, SourceRevision: observation.Revision, SourceStatus: observation.Status,
		PublishStatus: "pending",
	}
	return state, alert, true
}

func stale(o model.Observation, state model.EvaluationState) bool {
	if state.LastOpenTime.IsZero() {
		return false
	}
	if o.Candle.OpenTime.Before(state.LastOpenTime) {
		return true
	}
	if o.Candle.OpenTime.After(state.LastOpenTime) {
		return false
	}
	if o.Revision < state.LastRevision {
		return true
	}
	if o.Revision > state.LastRevision {
		return false
	}
	return statusRank(o.Status) <= statusRank(state.LastStatus)
}

func statusRank(status string) int {
	if status == "confirmed" {
		return 2
	}
	return 1
}

func shouldTrigger(mode string, truth, pulse bool, o model.Observation, state model.EvaluationState) bool {
	if !truth {
		return false
	}
	switch mode {
	case "every_match":
		return true
	case "once_per_bar":
		return state.LastAlertBar.IsZero() || !state.LastAlertBar.Equal(o.Candle.OpenTime)
	default:
		return pulse || !state.LastTruth
	}
}

func deterministicID(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	raw := hex.EncodeToString(h.Sum(nil)[:16])
	return raw[:8] + "-" + raw[8:12] + "-" + raw[12:16] + "-" + raw[16:20] + "-" + raw[20:32]
}
