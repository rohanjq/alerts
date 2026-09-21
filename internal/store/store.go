package store

import (
	"context"
	"errors"
	"time"

	"github.com/rohanjq/alerts/internal/model"
)

var ErrNotFound = errors.New("not found")

type Repository interface {
	CreateDefinition(context.Context, model.AlertDefinition) (model.AlertDefinition, bool, error)
	ListDefinitions(context.Context) ([]model.AlertDefinition, error)
	SetEnabled(context.Context, string, bool, time.Time) (model.AlertDefinition, error)
	ArchiveDefinition(context.Context, string, time.Time) error
	ApplyObservation(context.Context, model.Observation) ([]model.Alert, error)
	LoadRuntimeStates(context.Context, []int) (RuntimeSnapshot, error)
	CommitRuntimeBatch(context.Context, RuntimeBatch) ([]model.Alert, error)
	ListAlerts(context.Context, int64, int) ([]model.Alert, error)
	ClaimPending(context.Context, int) ([]model.Alert, error)
	MarkPublished(context.Context, string) error
	MarkPublishFailed(context.Context, string, string) error
	Maintain(context.Context, RetentionPolicy) (MaintenanceResult, error)
	AcquirePartitions(context.Context, string, []int, time.Duration) (bool, error)
	RenewPartitions(context.Context, string, []int, time.Duration) (bool, error)
	ReleasePartitions(context.Context, string) error
	OperationalStats(context.Context) (OperationalStats, error)
	Ping(context.Context) error
	Close()
}

type OperationalStats struct {
	PendingOutbox   int64
	OldestOutboxAge time.Duration
}

type RetentionPolicy struct {
	ProvisionalInbox time.Duration
	ConfirmedInbox   time.Duration
	AlertEvents      time.Duration
	BatchSize        int
}

type MaintenanceResult struct {
	InboxDeleted  int64
	AlertsDeleted int64
}

type RuntimeSnapshot struct {
	Series      map[string]model.SeriesEvaluationState
	Definitions map[string]model.DefinitionEvaluationState
}

type RuntimeBatch struct {
	PartitionID      int
	Observations     []model.Observation
	SeriesStates     map[string]model.SeriesEvaluationState
	DefinitionStates map[string]model.DefinitionEvaluationState
	ResetSeriesKeys  []string
	Alerts           []model.Alert
}

func DefinitionStateKey(definitionID, seriesKey string) string {
	return definitionID + "|" + seriesKey
}
