package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/rohanjq/alerts/internal/model"
	"github.com/rohanjq/alerts/internal/rules"
	evaluationruntime "github.com/rohanjq/alerts/internal/runtime"
	"github.com/rohanjq/alerts/internal/store"
)

type Publisher interface {
	Publish(context.Context, model.Alert) error
}

type Readiness func(context.Context) bool

type MultiPublisher []Publisher

func (publishers MultiPublisher) Publish(ctx context.Context, alert model.Alert) error {
	for _, publisher := range publishers {
		if err := publisher.Publish(ctx, alert); err != nil {
			return err
		}
	}
	return nil
}

type LogPublisher struct{ Logger *slog.Logger }

func (p LogPublisher) Publish(_ context.Context, a model.Alert) error {
	p.Logger.Info("alert event published", "alert_id", a.ID, "definition_id", a.DefinitionID, "definition_name", a.DefinitionName, "reasons", a.Reasons)
	return nil
}

type Service struct {
	repo                   store.Repository
	publisher              Publisher
	logger                 *slog.Logger
	wake                   chan struct{}
	now                    func() time.Time
	metrics                Metrics
	readiness              []Readiness
	runtime                *evaluationruntime.Engine
	activationDelay        time.Duration
	definitionMaxStaleness time.Duration
}

type Metrics struct {
	Observations     atomic.Uint64
	Alerts           atomic.Uint64
	Published        atomic.Uint64
	PublishFails     atomic.Uint64
	Batches          atomic.Uint64
	IngestFails      atomic.Uint64
	EvalNanos        atomic.Uint64
	RefreshFails     atomic.Uint64
	InboxDeleted     atomic.Uint64
	AlertsDeleted    atomic.Uint64
	MaintenanceFails atomic.Uint64
	EvalBuckets      [9]atomic.Uint64
}

var evaluationBucketBounds = [...]time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, time.Second}

type MetricSnapshot struct {
	Observations, Alerts, Published, PublishFailures                          uint64
	Batches, IngestFailures, EvaluationNanoseconds, DefinitionRefreshFailures uint64
	InboxDeleted, AlertsDeleted, MaintenanceFailures                          uint64
	StaleEvents, ResetEvents, RuleEvaluations                                 uint64
	PendingOutbox                                                             int64
	DefinitionCacheAgeSeconds                                                 float64
	OldestOutboxAgeSeconds                                                    float64
	EvaluationBuckets                                                         [9]uint64
}

type Options struct {
	ActivationDelay        time.Duration
	DefinitionMaxStaleness time.Duration
	WorkerIndex            int
	WorkerCount            int
}

func New(repo store.Repository, publisher Publisher, logger *slog.Logger, readiness ...Readiness) *Service {
	return NewWithOptions(repo, publisher, logger, Options{}, readiness...)
}

func NewWithOptions(repo store.Repository, publisher Publisher, logger *slog.Logger, options Options, readiness ...Readiness) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	if publisher == nil {
		publisher = LogPublisher{Logger: logger}
	}
	return &Service{repo: repo, publisher: publisher, logger: logger, wake: make(chan struct{}, 1), now: time.Now, readiness: readiness, runtime: evaluationruntime.NewWithOwnership(repo, evaluationruntime.DefaultPartitions, options.WorkerIndex, options.WorkerCount), activationDelay: options.ActivationDelay, definitionMaxStaleness: options.DefinitionMaxStaleness}
}

func (s *Service) Initialize(ctx context.Context) error { return s.runtime.Initialize(ctx) }

func (s *Service) RefreshDefinitions(ctx context.Context) error {
	definitions, err := s.repo.ListDefinitions(ctx)
	if err != nil {
		return err
	}
	return s.runtime.ReplaceDefinitions(definitions)
}

func (s *Service) RunDefinitionRefresh(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.RefreshDefinitions(ctx); err != nil {
				s.metrics.RefreshFails.Add(1)
				s.logger.Error("refresh compiled alert definitions", "err", err)
			}
		}
	}
}

func (s *Service) Create(ctx context.Context, name string, series model.Series, rule model.Rule, evaluationMode, triggerMode string, cooldown int) (model.AlertDefinition, bool, error) {
	if name == "" || series.Symbol == "" || series.Timeframe == "" {
		return model.AlertDefinition{}, false, fmt.Errorf("name, symbol, and timeframe are required")
	}
	if cooldown < 0 {
		return model.AlertDefinition{}, false, fmt.Errorf("cooldown_seconds cannot be negative")
	}
	if evaluationMode == "" {
		evaluationMode = "confirmed_close"
	}
	if evaluationMode != "confirmed_close" && evaluationMode != "intrabar" && evaluationMode != "both" {
		return model.AlertDefinition{}, false, fmt.Errorf("invalid evaluation_mode")
	}
	if triggerMode == "" {
		triggerMode = "on_occurrence"
	}
	if triggerMode != "on_occurrence" && triggerMode != "once_per_bar" && triggerMode != "every_match" {
		return model.AlertDefinition{}, false, fmt.Errorf("invalid trigger_mode")
	}
	if err := rules.Validate(rule); err != nil {
		return model.AlertDefinition{}, false, err
	}
	if rules.RequiresIntrabar(rule) && evaluationMode == "confirmed_close" {
		return model.AlertDefinition{}, false, fmt.Errorf("candle_color_flip requires intrabar or both evaluation_mode")
	}
	canonical := struct {
		Series         model.Series `json:"series"`
		Rule           model.Rule   `json:"rule"`
		EvaluationMode string       `json:"evaluation_mode"`
		TriggerMode    string       `json:"trigger_mode"`
		Cooldown       int          `json:"cooldown_seconds"`
	}{series, rule, evaluationMode, triggerMode, cooldown}
	raw, _ := json.Marshal(canonical)
	sum := sha256.Sum256(raw)
	hash := "sha256:" + hex.EncodeToString(sum[:])
	now := s.now().UTC()
	d := model.AlertDefinition{ID: uuidFrom(sum[:16]), Version: 1, DefinitionHash: hash, Name: name, Enabled: true, Series: series, Rule: rule, EvaluationMode: evaluationMode, TriggerMode: triggerMode, CooldownSeconds: cooldown, CreatedAt: now, UpdatedAt: now, EffectiveAt: now.Add(s.activationDelay)}
	createdDefinition, created, err := s.repo.CreateDefinition(ctx, d)
	if err == nil {
		err = s.runtime.UpsertDefinition(createdDefinition)
	}
	return createdDefinition, created, err
}
func (s *Service) ListDefinitions(ctx context.Context) ([]model.AlertDefinition, error) {
	return s.repo.ListDefinitions(ctx)
}
func (s *Service) SetEnabled(ctx context.Context, id string, enabled bool) (model.AlertDefinition, error) {
	definition, err := s.repo.SetEnabled(ctx, id, enabled, s.now().UTC().Add(s.activationDelay))
	if err == nil {
		err = s.runtime.UpsertDefinition(definition)
	}
	return definition, err
}
func (s *Service) Delete(ctx context.Context, id string) error {
	effectiveAt := s.now().UTC().Add(s.activationDelay)
	if err := s.repo.ArchiveDefinition(ctx, id, effectiveAt); err != nil {
		return err
	}
	// Reload the scheduled disabled row rather than removing it immediately;
	// definitionEnabledAt keeps it active until the shared event-time boundary.
	return s.RefreshDefinitions(ctx)
}
func (s *Service) ListAlerts(ctx context.Context, after int64, limit int) ([]model.Alert, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	return s.repo.ListAlerts(ctx, after, limit)
}

func (s *Service) Ingest(ctx context.Context, o model.Observation) ([]model.Alert, error) {
	if err := validateObservation(o); err != nil {
		return nil, err
	}
	return s.IngestBatch(ctx, s.runtime.Partition(o.Series), []model.Observation{o})
}

func (s *Service) IngestBatch(ctx context.Context, partition int, observations []model.Observation) ([]model.Alert, error) {
	if !s.runtime.DefinitionsFresh(s.definitionMaxStaleness) {
		return nil, fmt.Errorf("compiled definition cache is stale")
	}
	for _, observation := range observations {
		if err := validateObservation(observation); err != nil {
			return nil, err
		}
	}
	started := time.Now()
	alerts, err := s.runtime.EvaluateBatch(ctx, partition, observations)
	duration := time.Since(started)
	s.metrics.Batches.Add(1)
	s.metrics.EvalNanos.Add(uint64(duration))
	for index, bound := range evaluationBucketBounds {
		if duration <= bound {
			s.metrics.EvalBuckets[index].Add(1)
		}
	}
	if err == nil {
		s.metrics.Observations.Add(uint64(len(observations)))
		s.metrics.Alerts.Add(uint64(len(alerts)))
	}
	if err != nil {
		s.metrics.IngestFails.Add(1)
	}
	if err == nil && len(alerts) > 0 {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	return alerts, err
}

func (s *Service) RunMaintenance(ctx context.Context, interval time.Duration, policy store.RetentionPolicy) {
	if interval <= 0 {
		interval = time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, err := s.repo.Maintain(ctx, policy)
			if err != nil {
				s.metrics.MaintenanceFails.Add(1)
				s.logger.Error("run alert retention maintenance", "err", err)
				continue
			}
			s.metrics.InboxDeleted.Add(uint64(result.InboxDeleted))
			s.metrics.AlertsDeleted.Add(uint64(result.AlertsDeleted))
		}
	}
}

func validateObservation(o model.Observation) error {
	if o.Schema != "alerts.observation.v1" || o.SourceEventID == "" || o.CorrelationID == "" || o.Series.Symbol == "" || o.Series.Timeframe == "" || o.Candle.OpenTime.IsZero() || o.OccurredAt.IsZero() {
		return fmt.Errorf("invalid observation envelope")
	}
	if o.Status != "confirmed" && o.Status != "provisional" {
		return fmt.Errorf("status must be confirmed or provisional")
	}
	if o.Reset && o.Status != "confirmed" {
		return fmt.Errorf("authoritative reset must be confirmed")
	}
	if o.Candle.High < o.Candle.Low || o.Candle.High < o.Candle.Open || o.Candle.High < o.Candle.Close || o.Candle.Low > o.Candle.Open || o.Candle.Low > o.Candle.Close {
		return fmt.Errorf("invalid candle prices")
	}
	for name, value := range map[string]float64{"open": o.Candle.Open, "high": o.Candle.High, "low": o.Candle.Low, "close": o.Candle.Close} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("candle %s must be finite", name)
		}
	}
	for _, indicator := range o.Indicators {
		if indicator.Name == "" || indicator.Period < 1 || math.IsNaN(indicator.Value) || math.IsInf(indicator.Value, 0) {
			return fmt.Errorf("invalid indicator")
		}
	}
	for _, candle := range o.History {
		if candle.OpenTime.IsZero() || candle.High < candle.Low || candle.High < candle.Open || candle.High < candle.Close || candle.Low > candle.Open || candle.Low > candle.Close {
			return fmt.Errorf("invalid reset history candle")
		}
	}
	return nil
}

func (s *Service) RunPublisher(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.wake:
		}
		for {
			events, err := s.repo.ClaimPending(ctx, 100)
			if err != nil {
				s.logger.Error("claim alert outbox", "err", err)
				break
			}
			if len(events) == 0 {
				break
			}
			for _, event := range events {
				if err = s.publisher.Publish(ctx, event); err != nil {
					s.metrics.PublishFails.Add(1)
					_ = s.repo.MarkPublishFailed(ctx, event.ID, err.Error())
					continue
				}
				if err = s.repo.MarkPublished(ctx, event.ID); err != nil {
					s.logger.Error("mark alert published", "alert_id", event.ID, "err", err)
					continue
				}
				s.metrics.Published.Add(1)
			}
			if len(events) < 100 {
				break
			}
		}
	}
}

func (s *Service) Ready(ctx context.Context) bool {
	if s.repo.Ping(ctx) != nil {
		return false
	}
	if !s.runtime.DefinitionsFresh(s.definitionMaxStaleness) {
		return false
	}
	for _, ready := range s.readiness {
		if !ready(ctx) {
			return false
		}
	}
	return true
}
func (s *Service) Metrics(ctx context.Context) MetricSnapshot {
	runtimeMetrics := s.runtime.Metrics()
	storeMetrics, _ := s.repo.OperationalStats(ctx)
	snapshot := MetricSnapshot{Observations: s.metrics.Observations.Load(), Alerts: s.metrics.Alerts.Load(), Published: s.metrics.Published.Load(), PublishFailures: s.metrics.PublishFails.Load(), Batches: s.metrics.Batches.Load(), IngestFailures: s.metrics.IngestFails.Load(), EvaluationNanoseconds: s.metrics.EvalNanos.Load(), DefinitionRefreshFailures: s.metrics.RefreshFails.Load(), InboxDeleted: s.metrics.InboxDeleted.Load(), AlertsDeleted: s.metrics.AlertsDeleted.Load(), MaintenanceFailures: s.metrics.MaintenanceFails.Load(), StaleEvents: runtimeMetrics.StaleEvents, ResetEvents: runtimeMetrics.ResetEvents, RuleEvaluations: runtimeMetrics.RuleEvals, PendingOutbox: storeMetrics.PendingOutbox, DefinitionCacheAgeSeconds: s.runtime.DefinitionCacheAge().Seconds(), OldestOutboxAgeSeconds: storeMetrics.OldestOutboxAge.Seconds()}
	for index := range snapshot.EvaluationBuckets {
		snapshot.EvaluationBuckets[index] = s.metrics.EvalBuckets[index].Load()
	}
	return snapshot
}

func EvaluationBucketBounds() []time.Duration {
	return append([]time.Duration(nil), evaluationBucketBounds[:]...)
}
func (s *Service) Close()       { s.repo.Close() }
func IsNotFound(err error) bool { return errors.Is(err, store.ErrNotFound) }
func uuidFrom(raw []byte) string {
	h := hex.EncodeToString(raw)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
