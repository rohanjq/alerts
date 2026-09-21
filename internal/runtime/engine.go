package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rohanjq/alerts/internal/model"
	"github.com/rohanjq/alerts/internal/rules"
	"github.com/rohanjq/alerts/internal/store"
)

type compiledDefinition struct {
	definition model.AlertDefinition
	rule       rules.Compiled
}

type partitionRuntime struct {
	mu          sync.Mutex
	series      map[string]model.SeriesEvaluationState
	definitions map[string]model.DefinitionEvaluationState
}

// Engine owns the real-time state used by alert evaluation. PostgreSQL remains
// authoritative through CommitRuntimeBatch, but definition lookup, JSON rule
// parsing, feature history, and predicate execution happen in memory.
type Engine struct {
	repository store.Repository
	partitions []partitionRuntime

	definitionsMu sync.RWMutex
	byRoute       map[string][]compiledDefinition
	refreshedAt   atomic.Int64
	workerIndex   int
	workerCount   int
	staleEvents   atomic.Uint64
	resetEvents   atomic.Uint64
	ruleEvals     atomic.Uint64
}

const DefaultPartitions = 256

func New(repository store.Repository, partitionCount int) *Engine {
	return NewWithOwnership(repository, partitionCount, 0, 1)
}

func NewWithOwnership(repository store.Repository, partitionCount, workerIndex, workerCount int) *Engine {
	if partitionCount < 1 {
		partitionCount = 1
	}
	if workerCount < 1 {
		workerCount = 1
	}
	engine := &Engine{repository: repository, partitions: make([]partitionRuntime, partitionCount), byRoute: make(map[string][]compiledDefinition), workerIndex: workerIndex, workerCount: workerCount}
	for index := range engine.partitions {
		engine.partitions[index].series = make(map[string]model.SeriesEvaluationState)
		engine.partitions[index].definitions = make(map[string]model.DefinitionEvaluationState)
	}
	return engine
}

func (e *Engine) Initialize(ctx context.Context) error {
	definitions, err := e.repository.ListDefinitions(ctx)
	if err != nil {
		return fmt.Errorf("load alert definitions: %w", err)
	}
	if err = e.ReplaceDefinitions(definitions); err != nil {
		return err
	}
	snapshot, err := e.repository.LoadRuntimeStates(ctx, e.ownedPartitions())
	if err != nil {
		return fmt.Errorf("load evaluator runtime: %w", err)
	}
	for key, state := range snapshot.Series {
		partitionID := e.PartitionForKey(key)
		if partitionID%e.workerCount != e.workerIndex {
			continue
		}
		partition := &e.partitions[partitionID]
		partition.series[key] = cloneSeriesState(state)
	}
	for key, state := range snapshot.Definitions {
		seriesKey := definitionSeriesKey(key)
		partitionID := e.PartitionForKey(seriesKey)
		if partitionID%e.workerCount != e.workerIndex {
			continue
		}
		partition := &e.partitions[partitionID]
		partition.definitions[key] = state
	}
	return nil
}

func (e *Engine) ownedPartitions() []int {
	owned := make([]int, 0, len(e.partitions)/e.workerCount+1)
	for partition := range e.partitions {
		if partition%e.workerCount == e.workerIndex {
			owned = append(owned, partition)
		}
	}
	return owned
}

func (e *Engine) ReplaceDefinitions(definitions []model.AlertDefinition) error {
	routes := make(map[string][]compiledDefinition)
	for _, definition := range definitions {
		compiled, err := rules.Compile(definition.Rule)
		if err != nil {
			return fmt.Errorf("compile definition %s: %w", definition.ID, err)
		}
		key := routeKey(definition.Series)
		routes[key] = append(routes[key], compiledDefinition{definition: definition, rule: compiled})
	}
	for key := range routes {
		sort.Slice(routes[key], func(i, j int) bool { return routes[key][i].definition.ID < routes[key][j].definition.ID })
	}
	e.definitionsMu.Lock()
	e.byRoute = routes
	e.definitionsMu.Unlock()
	e.refreshedAt.Store(time.Now().UTC().UnixNano())
	return nil
}

// DefinitionsFresh lets ingestion fail closed while the control-plane cache
// cannot be refreshed. Broker redelivery is safer than silently missing a rule
// that became effective on another replica.
func (e *Engine) DefinitionsFresh(maxAge time.Duration) bool {
	if maxAge <= 0 {
		return true
	}
	refreshedAt := e.refreshedAt.Load()
	return refreshedAt > 0 && time.Since(time.Unix(0, refreshedAt)) <= maxAge
}

func (e *Engine) DefinitionCacheAge() time.Duration {
	refreshedAt := e.refreshedAt.Load()
	if refreshedAt == 0 {
		return 0
	}
	return time.Since(time.Unix(0, refreshedAt))
}

type Metrics struct {
	StaleEvents uint64
	ResetEvents uint64
	RuleEvals   uint64
}

func (e *Engine) Metrics() Metrics {
	return Metrics{StaleEvents: e.staleEvents.Load(), ResetEvents: e.resetEvents.Load(), RuleEvals: e.ruleEvals.Load()}
}

func (e *Engine) UpsertDefinition(definition model.AlertDefinition) error {
	compiled, err := rules.Compile(definition.Rule)
	if err != nil {
		return err
	}
	e.definitionsMu.Lock()
	defer e.definitionsMu.Unlock()
	key := routeKey(definition.Series)
	definitions := e.byRoute[key]
	for index := range definitions {
		if definitions[index].definition.ID == definition.ID {
			definitions[index] = compiledDefinition{definition: definition, rule: compiled}
			e.byRoute[key] = definitions
			return nil
		}
	}
	e.byRoute[key] = append(definitions, compiledDefinition{definition: definition, rule: compiled})
	sort.Slice(e.byRoute[key], func(i, j int) bool { return e.byRoute[key][i].definition.ID < e.byRoute[key][j].definition.ID })
	return nil
}

func (e *Engine) RemoveDefinition(id string) {
	e.definitionsMu.Lock()
	defer e.definitionsMu.Unlock()
	for key, definitions := range e.byRoute {
		filtered := definitions[:0]
		for _, definition := range definitions {
			if definition.definition.ID != id {
				filtered = append(filtered, definition)
			}
		}
		if len(filtered) == 0 {
			delete(e.byRoute, key)
		} else {
			e.byRoute[key] = filtered
		}
	}
}

func (e *Engine) Partition(series model.Series) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(series.Dataset + "\x00" + series.Symbol + "\x00" + series.Timeframe))
	return int(hash.Sum32() % uint32(len(e.partitions)))
}

func (e *Engine) PartitionForKey(key string) int {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) == 3 {
		return e.Partition(model.Series{Dataset: parts[0], Symbol: parts[1], Timeframe: parts[2]})
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(len(e.partitions)))
}

func (e *Engine) EvaluateBatch(ctx context.Context, partitionID int, observations []model.Observation) ([]model.Alert, error) {
	if partitionID < 0 || partitionID >= len(e.partitions) {
		return nil, fmt.Errorf("partition %d is out of range", partitionID)
	}
	if partitionID%e.workerCount != e.workerIndex {
		return nil, fmt.Errorf("partition %d is not owned by worker %d/%d", partitionID, e.workerIndex, e.workerCount)
	}
	partition := &e.partitions[partitionID]
	partition.mu.Lock()
	defer partition.mu.Unlock()

	seriesStates := make(map[string]model.SeriesEvaluationState)
	definitionStates := make(map[string]model.DefinitionEvaluationState)
	resetSeries := make(map[string]struct{})
	accepted := make([]model.Observation, 0, len(observations))
	alerts := make([]model.Alert, 0)
	now := time.Now().UTC()

	for _, input := range observations {
		if e.Partition(input.Series) != partitionID {
			return nil, fmt.Errorf("series %s was routed to the wrong partition", input.Series.Key())
		}
		seriesKey := input.Series.Key()
		seriesState, exists := seriesStates[seriesKey]
		if !exists {
			seriesState = cloneSeriesState(partition.series[seriesKey])
		}
		if input.Reset && (input.SourceEventID == seriesState.LastResetID || (!seriesState.LastResetAt.IsZero() && !input.OccurredAt.After(seriesState.LastResetAt))) {
			e.staleEvents.Add(1)
			continue
		}
		if !input.Reset && stale(input, seriesState) {
			e.staleEvents.Add(1)
			continue
		}
		if input.Reset {
			e.resetEvents.Add(1)
			// A reset is an authoritative corrected snapshot, not an alertable
			// historical occurrence. It replaces future state while immutable
			// alerts already emitted under the live-as-known policy remain intact.
			seriesState = model.SeriesEvaluationState{}
			resetSeries[seriesKey] = struct{}{}
			for key := range definitionStates {
				if definitionSeriesKey(key) == seriesKey {
					delete(definitionStates, key)
				}
			}
			feature := model.FeatureState{}
			for _, bar := range input.History {
				feature = rules.Advance(feature, model.Observation{Status: "confirmed", Candle: bar})
			}
			for _, compiled := range e.definitionsFor(input.Series) {
				definition := compiled.definition
				if !acceptsMode(definition.EvaluationMode, "confirmed") {
					continue
				}
				stateKey := store.DefinitionStateKey(definition.ID, seriesKey)
				definitionState := partition.definitions[stateKey]
				truth, _, _ := compiled.rule.EvaluateDetailed(input, feature)
				e.ruleEvals.Add(1)
				definitionState.LastTruthConfirmed = truth
				definitionState.LastTruthIntrabar = false
				definitionStates[stateKey] = definitionState
			}
			seriesState.Confirmed = rules.Advance(feature, input)
			seriesState.Intrabar = cloneFeatureState(seriesState.Confirmed)
			seriesState.ActiveZones = append([]model.Zone(nil), input.ActiveZones...)
			seriesState.LastOpenTime = input.Candle.OpenTime.UTC()
			seriesState.LastRevision = input.Revision
			seriesState.LastStatus = input.Status
			seriesState.LastResetID = input.SourceEventID
			seriesState.LastResetAt = input.OccurredAt.UTC()
			seriesStates[seriesKey] = seriesState
			accepted = append(accepted, input)
			continue
		}

		observation := input
		if observation.Status == "provisional" && len(observation.ActiveZones) == 0 {
			observation.ActiveZones = append([]model.Zone(nil), seriesState.ActiveZones...)
		}
		feature := seriesState.Confirmed
		if observation.Status == "provisional" {
			feature = seriesState.Intrabar
			if feature.LastClose == nil && seriesState.Confirmed.LastClose != nil {
				feature = cloneFeatureState(seriesState.Confirmed)
			}
		}

		for _, compiled := range e.definitionsFor(observation.Series) {
			definition := compiled.definition
			if observation.Status == "provisional" && compiled.rule.Dependencies&(rules.DependencyCandle|rules.DependencyIndicator) == 0 {
				continue
			}
			if !definitionEnabledAt(definition, observation.OccurredAt) || !acceptsMode(definition.EvaluationMode, observation.Status) {
				continue
			}
			stateKey := store.DefinitionStateKey(definition.ID, seriesKey)
			definitionState, stateExists := definitionStates[stateKey]
			if !stateExists {
				if _, wasReset := resetSeries[seriesKey]; !wasReset {
					definitionState = partition.definitions[stateKey]
				}
			}
			truth, reasons, facts := compiled.rule.EvaluateDetailed(observation, feature)
			e.ruleEvals.Add(1)
			lastTruth := definitionState.LastTruthConfirmed
			if observation.Status == "provisional" {
				lastTruth = definitionState.LastTruthIntrabar
			}
			trigger := shouldTrigger(definition.TriggerMode, truth, rules.Pulse(definition.Rule), observation, definitionState, lastTruth)
			if trigger && definition.CooldownSeconds > 0 && !definitionState.LastTriggerAt.IsZero() && observation.OccurredAt.Sub(definitionState.LastTriggerAt) < time.Duration(definition.CooldownSeconds)*time.Second {
				trigger = false
			}
			if observation.Status == "provisional" {
				definitionState.LastTruthIntrabar = truth
			} else {
				definitionState.LastTruthConfirmed = truth
			}
			if trigger {
				definitionState.LastTriggerAt = observation.OccurredAt.UTC()
				definitionState.LastAlertBar = observation.Candle.OpenTime.UTC()
				payload, _ := json.Marshal(observation)
				alerts = append(alerts, model.Alert{
					Schema: "io.ytstack.alert.triggered.v1", ID: deterministicID(definition.ID, observation.SourceEventID),
					DefinitionID: definition.ID, DefinitionVersion: definition.Version, DefinitionHash: definition.DefinitionHash, DefinitionName: definition.Name,
					SourceEventID: observation.SourceEventID, SourceCandleID: observation.CorrelationID, Series: observation.Series,
					TriggeredAt: observation.OccurredAt.UTC(), EvaluatedAt: now, Reasons: reasons,
					FactsUsed: facts, FeatureAlgorithm: observation.FeatureAlgorithm,
					SourceRevision: observation.Revision, SourceStatus: observation.Status,
					Observation: payload, PublishStatus: "pending",
				})
			}
			definitionStates[stateKey] = definitionState
		}

		if observation.Status == "confirmed" {
			seriesState.Confirmed = rules.Advance(seriesState.Confirmed, observation)
			seriesState.Intrabar = cloneFeatureState(seriesState.Confirmed)
		} else {
			seriesState.Intrabar = rules.Advance(feature, observation)
		}
		if len(observation.ActiveZones) > 0 {
			seriesState.ActiveZones = append([]model.Zone(nil), observation.ActiveZones...)
		}
		seriesState.LastOpenTime = observation.Candle.OpenTime.UTC()
		seriesState.LastRevision = observation.Revision
		seriesState.LastStatus = observation.Status
		seriesStates[seriesKey] = seriesState
		accepted = append(accepted, observation)
	}

	if len(accepted) == 0 {
		return nil, nil
	}
	resetKeys := make([]string, 0, len(resetSeries))
	for key := range resetSeries {
		resetKeys = append(resetKeys, key)
	}
	sort.Strings(resetKeys)
	committed, err := e.repository.CommitRuntimeBatch(ctx, store.RuntimeBatch{
		PartitionID: partitionID, Observations: accepted, SeriesStates: seriesStates, DefinitionStates: definitionStates, ResetSeriesKeys: resetKeys, Alerts: alerts,
	})
	if err != nil {
		return nil, err
	}
	for key, state := range seriesStates {
		partition.series[key] = cloneSeriesState(state)
	}
	for _, seriesKey := range resetKeys {
		for key := range partition.definitions {
			if definitionSeriesKey(key) == seriesKey {
				delete(partition.definitions, key)
			}
		}
	}
	for key, state := range definitionStates {
		partition.definitions[key] = state
	}
	return committed, nil
}

func (e *Engine) definitionsFor(series model.Series) []compiledDefinition {
	e.definitionsMu.RLock()
	defer e.definitionsMu.RUnlock()
	exact := e.byRoute[routeKey(series)]
	wildcard := e.byRoute[routeKey(model.Series{Symbol: series.Symbol, Timeframe: series.Timeframe})]
	out := make([]compiledDefinition, 0, len(exact)+len(wildcard))
	out = append(out, exact...)
	if series.Dataset != "" {
		out = append(out, wildcard...)
	}
	return out
}

func routeKey(series model.Series) string {
	return series.Dataset + "\x00" + series.Symbol + "\x00" + series.Timeframe
}

func effectiveAt(definition model.AlertDefinition) time.Time {
	if !definition.EffectiveAt.IsZero() {
		return definition.EffectiveAt
	}
	return definition.CreatedAt
}

func definitionEnabledAt(definition model.AlertDefinition, eventTime time.Time) bool {
	effective := effectiveAt(definition)
	if definition.Enabled {
		return !eventTime.Before(effective)
	}
	// A disable is scheduled so all evaluator replicas switch at the same
	// event-time boundary even if their caches refresh at slightly different
	// wall-clock instants.
	return eventTime.Before(effective)
}

func acceptsMode(mode, status string) bool {
	switch mode {
	case "confirmed_close":
		return status == "confirmed"
	case "intrabar":
		return status == "provisional"
	default:
		return status == "confirmed" || status == "provisional"
	}
}

func stale(observation model.Observation, state model.SeriesEvaluationState) bool {
	if state.LastOpenTime.IsZero() {
		return false
	}
	if observation.Candle.OpenTime.Before(state.LastOpenTime) {
		return true
	}
	if observation.Candle.OpenTime.After(state.LastOpenTime) {
		return false
	}
	if observation.Revision < state.LastRevision {
		return true
	}
	if observation.Revision > state.LastRevision {
		return false
	}
	return statusRank(observation.Status) <= statusRank(state.LastStatus)
}

func statusRank(status string) int {
	if status == "confirmed" {
		return 2
	}
	return 1
}

func shouldTrigger(mode string, truth, pulse bool, observation model.Observation, state model.DefinitionEvaluationState, lastTruth bool) bool {
	if !truth {
		return false
	}
	switch mode {
	case "every_match":
		return true
	case "once_per_bar":
		return state.LastAlertBar.IsZero() || !state.LastAlertBar.Equal(observation.Candle.OpenTime)
	default:
		return pulse || !lastTruth
	}
}

func cloneFeatureState(state model.FeatureState) model.FeatureState {
	if state.LastClose != nil {
		value := *state.LastClose
		state.LastClose = &value
	}
	if state.Indicators != nil {
		state.Indicators = make(map[string]float64, len(state.Indicators))
		for key, value := range state.Indicators {
			state.Indicators[key] = value
		}
	}
	state.CandleColors = append([]string(nil), state.CandleColors...)
	state.Candles = append([]model.Candle(nil), state.Candles...)
	return state
}

func cloneSeriesState(state model.SeriesEvaluationState) model.SeriesEvaluationState {
	state.Confirmed = cloneFeatureState(state.Confirmed)
	state.Intrabar = cloneFeatureState(state.Intrabar)
	state.ActiveZones = append([]model.Zone(nil), state.ActiveZones...)
	return state
}

func deterministicID(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	raw := hex.EncodeToString(hash.Sum(nil)[:16])
	return raw[:8] + "-" + raw[8:12] + "-" + raw[12:16] + "-" + raw[16:20] + "-" + raw[20:32]
}

func definitionSeriesKey(key string) string {
	for index := 0; index < len(key); index++ {
		if key[index] == '|' {
			return key[index+1:]
		}
	}
	return key
}
