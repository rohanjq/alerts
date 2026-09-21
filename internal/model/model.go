package model

import (
	"encoding/json"
	"time"
)

type Series struct {
	Dataset   string `json:"dataset"`
	Symbol    string `json:"symbol"`
	Timeframe string `json:"timeframe"`
}

func (s Series) Key() string { return s.Dataset + "/" + s.Symbol + "/" + s.Timeframe }

type Candle struct {
	OpenTime time.Time `json:"open_time"`
	Open     float64   `json:"open"`
	High     float64   `json:"high"`
	Low      float64   `json:"low"`
	Close    float64   `json:"close"`
	Volume   float64   `json:"volume,omitempty"`
	Trades   uint64    `json:"trades,omitempty"`
}

type AlgorithmRef struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	ConfigHash string `json:"config_hash"`
}

type FactReference struct {
	Kind  string  `json:"kind"`
	ID    string  `json:"id,omitempty"`
	Side  string  `json:"side,omitempty"`
	Name  string  `json:"name,omitempty"`
	Value float64 `json:"value,omitempty"`
}

type Indicator struct {
	Name   string  `json:"name"`
	Period int     `json:"period,omitempty"`
	Value  float64 `json:"value"`
	Ready  bool    `json:"ready"`
}

type ZoneTransition struct {
	ID         string  `json:"id"`
	Kind       string  `json:"kind"`
	Side       string  `json:"side,omitempty"`
	Transition string  `json:"transition"`
	Upper      float64 `json:"upper"`
	Lower      float64 `json:"lower"`
}

type Zone struct {
	ID    string  `json:"id"`
	Kind  string  `json:"kind"`
	Side  string  `json:"side,omitempty"`
	State string  `json:"state"`
	Upper float64 `json:"upper"`
	Lower float64 `json:"lower"`
}

type Pattern struct {
	Kind string `json:"kind"`
	Side string `json:"side,omitempty"`
}

// Observation is the stable boundary between fact producers and alert policy.
// It is complete for a confirmed bar and has a deterministic SourceEventID.
type Observation struct {
	Schema           string           `json:"schema"`
	SourceEventID    string           `json:"source_event_id"`
	CorrelationID    string           `json:"correlation_id"`
	OccurredAt       time.Time        `json:"occurred_at"`
	Status           string           `json:"status"`
	Revision         uint64           `json:"revision"`
	Reset            bool             `json:"reset,omitempty"`
	History          []Candle         `json:"history,omitempty"`
	Series           Series           `json:"series"`
	Candle           Candle           `json:"candle"`
	Indicators       []Indicator      `json:"indicators,omitempty"`
	ActiveZones      []Zone           `json:"active_zones,omitempty"`
	ZoneTransitions  []ZoneTransition `json:"zone_transitions,omitempty"`
	Patterns         []Pattern        `json:"patterns,omitempty"`
	FeatureAlgorithm AlgorithmRef     `json:"feature_algorithm"`
}

type Rule struct {
	Type              string  `json:"type"`
	Rules             []Rule  `json:"rules,omitempty"`
	Rule              *Rule   `json:"rule,omitempty"`
	Kind              string  `json:"kind,omitempty"`
	Side              string  `json:"side,omitempty"`
	Transition        string  `json:"transition,omitempty"`
	Name              string  `json:"name,omitempty"`
	Period            int     `json:"period,omitempty"`
	Direction         string  `json:"direction,omitempty"`
	Count             int     `json:"count,omitempty"`
	DistanceBps       float64 `json:"distance_bps,omitempty"`
	MinElapsedPercent float64 `json:"min_elapsed_percent,omitempty"`
}

type AlertDefinition struct {
	ID              string    `json:"id"`
	Version         int       `json:"version"`
	DefinitionHash  string    `json:"definition_hash"`
	Name            string    `json:"name"`
	Enabled         bool      `json:"enabled"`
	Series          Series    `json:"series"`
	Rule            Rule      `json:"rule"`
	EvaluationMode  string    `json:"evaluation_mode"`
	TriggerMode     string    `json:"trigger_mode"`
	CooldownSeconds int       `json:"cooldown_seconds"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	EffectiveAt     time.Time `json:"effective_at"`
}

type Alert struct {
	Schema            string          `json:"schema"`
	ID                string          `json:"id"`
	DefinitionID      string          `json:"definition_id"`
	DefinitionVersion int             `json:"definition_version"`
	DefinitionHash    string          `json:"definition_hash"`
	DefinitionName    string          `json:"definition_name"`
	SourceEventID     string          `json:"source_event_id"`
	SourceCandleID    string          `json:"source_candle_id"`
	Series            Series          `json:"series"`
	TriggeredAt       time.Time       `json:"triggered_at"`
	EvaluatedAt       time.Time       `json:"evaluated_at"`
	Reasons           []string        `json:"reasons"`
	FactsUsed         []FactReference `json:"facts_used,omitempty"`
	FeatureAlgorithm  AlgorithmRef    `json:"feature_algorithm"`
	SourceRevision    uint64          `json:"source_revision"`
	SourceStatus      string          `json:"source_status"`
	Observation       json.RawMessage `json:"observation"`
	PublishStatus     string          `json:"publish_status,omitempty"`
	Cursor            int64           `json:"cursor,omitempty"`
}

// FeatureState is shared once per series/cadence. Rolling market history must
// not be copied into every alert definition.
type FeatureState struct {
	LastClose    *float64           `json:"last_close,omitempty"`
	Indicators   map[string]float64 `json:"indicators,omitempty"`
	Candles      []Candle           `json:"candles,omitempty"`
	CandleColors []string           `json:"candle_colors,omitempty"`
	FormingBar   time.Time          `json:"forming_bar,omitempty"`
	FormingColor string             `json:"forming_color,omitempty"`
}

type SeriesEvaluationState struct {
	LastOpenTime time.Time    `json:"last_open_time,omitempty"`
	LastRevision uint64       `json:"last_revision,omitempty"`
	LastStatus   string       `json:"last_status,omitempty"`
	LastResetID  string       `json:"last_reset_id,omitempty"`
	LastResetAt  time.Time    `json:"last_reset_at,omitempty"`
	Confirmed    FeatureState `json:"confirmed"`
	Intrabar     FeatureState `json:"intrabar"`
	ActiveZones  []Zone       `json:"active_zones,omitempty"`
}

// DefinitionEvaluationState contains only temporal trigger policy. Market
// features and candle history live in SeriesEvaluationState.
type DefinitionEvaluationState struct {
	LastTruthConfirmed bool      `json:"last_truth_confirmed"`
	LastTruthIntrabar  bool      `json:"last_truth_intrabar"`
	LastTriggerAt      time.Time `json:"last_trigger_at,omitempty"`
	LastAlertBar       time.Time `json:"last_alert_bar,omitempty"`
}

type EvaluationState struct {
	LastTruth     bool               `json:"last_truth"`
	LastTriggerAt time.Time          `json:"last_trigger_at,omitempty"`
	LastClose     *float64           `json:"last_close,omitempty"`
	Indicators    map[string]float64 `json:"indicators,omitempty"`
	CandleColors  []string           `json:"candle_colors,omitempty"`
	LastOpenTime  time.Time          `json:"last_open_time,omitempty"`
	LastRevision  uint64             `json:"last_revision,omitempty"`
	LastStatus    string             `json:"last_status,omitempty"`
	LastAlertBar  time.Time          `json:"last_alert_bar,omitempty"`
	FormingBar    time.Time          `json:"forming_bar,omitempty"`
	FormingColor  string             `json:"forming_color,omitempty"`
}
