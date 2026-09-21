package store

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rohanjq/alerts/internal/model"
)

type Memory struct {
	mu                sync.Mutex
	definitions       map[string]model.AlertDefinition
	byHash            map[string]string
	archived          map[string]bool
	states            map[string]model.EvaluationState
	seriesRuntime     map[string]model.SeriesEvaluationState
	definitionRuntime map[string]model.DefinitionEvaluationState
	inbox             map[string]struct{}
	alerts            []model.Alert
	leases            map[int]string
}

func NewMemory() *Memory {
	return &Memory{definitions: map[string]model.AlertDefinition{}, byHash: map[string]string{}, archived: map[string]bool{}, states: map[string]model.EvaluationState{}, seriesRuntime: map[string]model.SeriesEvaluationState{}, definitionRuntime: map[string]model.DefinitionEvaluationState{}, inbox: map[string]struct{}{}, leases: map[int]string{}}
}
func (m *Memory) Close()                     {}
func (m *Memory) Ping(context.Context) error { return nil }

func (m *Memory) CreateDefinition(_ context.Context, d model.AlertDefinition) (model.AlertDefinition, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.byHash[d.DefinitionHash]; ok {
		if m.archived[id] {
			m.definitions[id] = d
			delete(m.archived, id)
			return d, false, nil
		}
		return m.definitions[id], false, nil
	}
	m.definitions[d.ID] = d
	m.byHash[d.DefinitionHash] = d.ID
	return d, true, nil
}
func (m *Memory) ListDefinitions(context.Context) ([]model.AlertDefinition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]model.AlertDefinition, 0, len(m.definitions))
	for _, d := range m.definitions {
		if m.archived[d.ID] && !time.Now().UTC().Before(d.EffectiveAt) {
			continue
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}
func (m *Memory) SetEnabled(_ context.Context, id string, enabled bool, effectiveAt time.Time) (model.AlertDefinition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.definitions[id]
	if !ok || m.archived[id] {
		return model.AlertDefinition{}, ErrNotFound
	}
	d.Enabled = enabled
	d.UpdatedAt = time.Now().UTC()
	d.EffectiveAt = effectiveAt.UTC()
	m.definitions[id] = d
	return d, nil
}
func (m *Memory) ArchiveDefinition(_ context.Context, id string, effectiveAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.definitions[id]
	if !ok {
		return ErrNotFound
	}
	d.Enabled = false
	d.EffectiveAt = effectiveAt.UTC()
	m.definitions[id] = d
	m.archived[id] = true
	return nil
}
func (m *Memory) ApplyObservation(_ context.Context, o model.Observation) ([]model.Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.inbox[o.SourceEventID]; ok {
		return []model.Alert{}, nil
	}
	m.inbox[o.SourceEventID] = struct{}{}
	payload, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	created := []model.Alert{}
	for _, d := range m.definitions {
		if !d.Enabled || o.OccurredAt.Before(d.CreatedAt) || !matchSeries(d.Series, o.Series) || !acceptsMode(d.EvaluationMode, o.Status) {
			continue
		}
		key := d.ID + "|" + o.Series.Key()
		state, alert, accepted := apply(d, o, m.states[key])
		if !accepted {
			continue
		}
		m.states[key] = state
		if alert != nil {
			alert.Observation = payload
			alert.Cursor = int64(len(m.alerts) + 1)
			m.alerts = append(m.alerts, *alert)
			created = append(created, *alert)
		}
	}
	return created, nil
}

func (m *Memory) LoadRuntimeStates(context.Context, []int) (RuntimeSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshot := RuntimeSnapshot{Series: make(map[string]model.SeriesEvaluationState, len(m.seriesRuntime)), Definitions: make(map[string]model.DefinitionEvaluationState, len(m.definitionRuntime))}
	for key, state := range m.seriesRuntime {
		snapshot.Series[key] = state
	}
	for key, state := range m.definitionRuntime {
		snapshot.Definitions[key] = state
	}
	return snapshot, nil
}

func (m *Memory) CommitRuntimeBatch(_ context.Context, batch RuntimeBatch) ([]model.Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, observation := range batch.Observations {
		m.inbox[observation.SourceEventID] = struct{}{}
	}
	for key, state := range batch.SeriesStates {
		m.seriesRuntime[key] = state
	}
	for _, seriesKey := range batch.ResetSeriesKeys {
		for key := range m.definitionRuntime {
			if definitionSeriesKey(key) == seriesKey {
				delete(m.definitionRuntime, key)
			}
		}
	}
	for key, state := range batch.DefinitionStates {
		m.definitionRuntime[key] = state
	}
	created := make([]model.Alert, 0, len(batch.Alerts))
	for _, alert := range batch.Alerts {
		duplicate := false
		for _, existing := range m.alerts {
			if existing.ID == alert.ID {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		alert.Cursor = int64(len(m.alerts) + 1)
		m.alerts = append(m.alerts, alert)
		created = append(created, alert)
	}
	return created, nil
}

func definitionSeriesKey(key string) string {
	_, seriesKey, found := strings.Cut(key, "|")
	if !found {
		return key
	}
	return seriesKey
}
func (m *Memory) ListAlerts(_ context.Context, after int64, limit int) ([]model.Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []model.Alert{}
	for _, a := range m.alerts {
		if a.Cursor > after {
			out = append(out, a)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}
func (m *Memory) ClaimPending(_ context.Context, limit int) ([]model.Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []model.Alert{}
	for _, a := range m.alerts {
		if a.PublishStatus == "pending" {
			out = append(out, a)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}
func (m *Memory) MarkPublished(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.alerts {
		if m.alerts[i].ID == id {
			m.alerts[i].PublishStatus = "published"
			return nil
		}
	}
	return ErrNotFound
}
func (m *Memory) MarkPublishFailed(context.Context, string, string) error { return nil }
func (m *Memory) Maintain(context.Context, RetentionPolicy) (MaintenanceResult, error) {
	return MaintenanceResult{}, nil
}
func (m *Memory) AcquirePartitions(_ context.Context, owner string, partitions []int, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, partition := range partitions {
		if current := m.leases[partition]; current != "" && current != owner {
			return false, nil
		}
	}
	for _, partition := range partitions {
		m.leases[partition] = owner
	}
	return true, nil
}
func (m *Memory) RenewPartitions(ctx context.Context, owner string, partitions []int, ttl time.Duration) (bool, error) {
	return m.AcquirePartitions(ctx, owner, partitions, ttl)
}
func (m *Memory) ReleasePartitions(_ context.Context, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for partition, current := range m.leases {
		if current == owner {
			delete(m.leases, partition)
		}
	}
	return nil
}
func (m *Memory) OperationalStats(context.Context) (OperationalStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := OperationalStats{}
	for _, alert := range m.alerts {
		if alert.PublishStatus == "pending" {
			stats.PendingOutbox++
		}
	}
	return stats, nil
}

func matchSeries(selector, actual model.Series) bool {
	return selector.Symbol == actual.Symbol && selector.Timeframe == actual.Timeframe && (selector.Dataset == "" || selector.Dataset == actual.Dataset)
}
