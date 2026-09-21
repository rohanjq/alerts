package store

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rohanjq/alerts/internal/model"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Postgres struct{ pool *pgxpool.Pool }

func OpenPostgres(ctx context.Context, databaseURL string) (*Postgres, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	config.MaxConns = 32
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	p := &Postgres{pool: pool}
	if err := p.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

func (p *Postgres) migrate(ctx context.Context) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext('alertsd-migrations'))`); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtext('alertsd-migrations'))`)
	if _, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS alert_schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		body, readErr := migrationFiles.ReadFile("migrations/" + entry.Name())
		if readErr != nil {
			return readErr
		}
		var applied bool
		if err = conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM alert_schema_migrations WHERE name=$1)`, entry.Name()).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		tx, beginErr := conn.Begin(ctx)
		if beginErr != nil {
			return beginErr
		}
		if _, execErr := tx.Exec(ctx, string(body)); execErr != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", entry.Name(), execErr)
		}
		if _, execErr := tx.Exec(ctx, `INSERT INTO alert_schema_migrations(name) VALUES($1)`, entry.Name()); execErr != nil {
			_ = tx.Rollback(ctx)
			return execErr
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *Postgres) Close()                         { p.pool.Close() }
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

func (p *Postgres) CreateDefinition(ctx context.Context, d model.AlertDefinition) (model.AlertDefinition, bool, error) {
	rule, err := json.Marshal(d.Rule)
	if err != nil {
		return model.AlertDefinition{}, false, err
	}
	row := p.pool.QueryRow(ctx, `INSERT INTO alert_definitions
		(id,version,definition_hash,name,enabled,dataset,symbol,timeframe,rule,evaluation_mode,trigger_mode,cooldown_seconds,created_at,updated_at,effective_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$13,$14)
		ON CONFLICT(definition_hash) DO UPDATE SET name=alert_definitions.name,enabled=true,archived_at=NULL,
		updated_at=CASE WHEN alert_definitions.enabled AND alert_definitions.archived_at IS NULL THEN alert_definitions.updated_at ELSE EXCLUDED.updated_at END,
		effective_at=CASE WHEN alert_definitions.enabled AND alert_definitions.archived_at IS NULL THEN alert_definitions.effective_at ELSE EXCLUDED.effective_at END
		RETURNING id,version,definition_hash,name,enabled,dataset,symbol,timeframe,rule,evaluation_mode,trigger_mode,cooldown_seconds,created_at,updated_at,effective_at,(xmax=0)`,
		d.ID, d.Version, d.DefinitionHash, d.Name, d.Enabled, d.Series.Dataset, d.Series.Symbol, d.Series.Timeframe, rule, d.EvaluationMode, d.TriggerMode, d.CooldownSeconds, d.CreatedAt, d.EffectiveAt)
	var got model.AlertDefinition
	var raw []byte
	var created bool
	err = row.Scan(&got.ID, &got.Version, &got.DefinitionHash, &got.Name, &got.Enabled, &got.Series.Dataset, &got.Series.Symbol, &got.Series.Timeframe, &raw, &got.EvaluationMode, &got.TriggerMode, &got.CooldownSeconds, &got.CreatedAt, &got.UpdatedAt, &got.EffectiveAt, &created)
	if err == nil {
		err = json.Unmarshal(raw, &got.Rule)
	}
	return got, created, err
}

const definitionSelect = `SELECT id,version,definition_hash,name,enabled,dataset,symbol,timeframe,rule,evaluation_mode,trigger_mode,cooldown_seconds,created_at,updated_at,effective_at FROM alert_definitions`

type rowScanner interface{ Scan(...any) error }

func scanDefinition(row rowScanner) (model.AlertDefinition, error) {
	var d model.AlertDefinition
	var raw []byte
	err := row.Scan(&d.ID, &d.Version, &d.DefinitionHash, &d.Name, &d.Enabled, &d.Series.Dataset, &d.Series.Symbol, &d.Series.Timeframe, &raw, &d.EvaluationMode, &d.TriggerMode, &d.CooldownSeconds, &d.CreatedAt, &d.UpdatedAt, &d.EffectiveAt)
	if err == nil {
		err = json.Unmarshal(raw, &d.Rule)
	}
	return d, err
}

func (p *Postgres) ListDefinitions(ctx context.Context) ([]model.AlertDefinition, error) {
	rows, err := p.pool.Query(ctx, definitionSelect+` WHERE archived_at IS NULL OR archived_at>now() ORDER BY created_at,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.AlertDefinition{}
	for rows.Next() {
		d, e := scanDefinition(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
func (p *Postgres) SetEnabled(ctx context.Context, id string, enabled bool, effectiveAt time.Time) (model.AlertDefinition, error) {
	return scanDefinition(p.pool.QueryRow(ctx, `UPDATE alert_definitions SET enabled=$2,updated_at=now(),effective_at=$3
		WHERE id=$1 AND archived_at IS NULL
		RETURNING id,version,definition_hash,name,enabled,dataset,symbol,timeframe,rule,evaluation_mode,trigger_mode,cooldown_seconds,created_at,updated_at,effective_at`, id, enabled, effectiveAt))
}

func (p *Postgres) ArchiveDefinition(ctx context.Context, id string, effectiveAt time.Time) error {
	tag, err := p.pool.Exec(ctx, `UPDATE alert_definitions SET enabled=false,archived_at=$2,effective_at=$2,updated_at=now() WHERE id=$1 AND archived_at IS NULL`, id, effectiveAt)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (p *Postgres) ApplyObservation(ctx context.Context, o model.Observation) ([]model.Alert, error) {
	payload, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO alert_inbox(source_event_id,payload) VALUES($1,$2) ON CONFLICT DO NOTHING`, o.SourceEventID, payload)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return []model.Alert{}, tx.Commit(ctx)
	}
	rows, err := tx.Query(ctx, definitionSelect+` WHERE enabled AND archived_at IS NULL AND symbol=$1 AND timeframe=$2 AND (dataset='' OR dataset=$3) AND created_at<=$4 ORDER BY id`, o.Series.Symbol, o.Series.Timeframe, o.Series.Dataset, o.OccurredAt)
	if err != nil {
		return nil, err
	}
	definitions := []model.AlertDefinition{}
	for rows.Next() {
		d, e := scanDefinition(rows)
		if e != nil {
			rows.Close()
			return nil, e
		}
		definitions = append(definitions, d)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	created := []model.Alert{}
	for _, d := range definitions {
		if !acceptsMode(d.EvaluationMode, o.Status) {
			continue
		}
		key := o.Series.Key()
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, d.ID+"|"+key); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO alert_rule_state(definition_id,series_key,state) VALUES($1,$2,'{}') ON CONFLICT DO NOTHING`, d.ID, key); err != nil {
			return nil, err
		}
		var stateRaw []byte
		if err = tx.QueryRow(ctx, `SELECT state FROM alert_rule_state WHERE definition_id=$1 AND series_key=$2 FOR UPDATE`, d.ID, key).Scan(&stateRaw); err != nil {
			return nil, err
		}
		var state model.EvaluationState
		if err = json.Unmarshal(stateRaw, &state); err != nil {
			return nil, err
		}
		state, alert, accepted := apply(d, o, state)
		if !accepted {
			continue
		}
		nextState, _ := json.Marshal(state)
		if _, err = tx.Exec(ctx, `UPDATE alert_rule_state SET state=$3,updated_at=now() WHERE definition_id=$1 AND series_key=$2`, d.ID, key, nextState); err != nil {
			return nil, err
		}
		if alert == nil {
			continue
		}
		alert.Observation = payload
		eventPayload, _ := json.Marshal(alert)
		reasons, _ := json.Marshal(alert.Reasons)
		var cursor int64
		err = tx.QueryRow(ctx, `INSERT INTO alert_events(id,definition_id,source_event_id,series_key,definition_name,reasons,observation,triggered_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(definition_id,source_event_id) DO UPDATE SET id=alert_events.id RETURNING cursor`, alert.ID, d.ID, o.SourceEventID, key, d.Name, reasons, payload, alert.TriggeredAt).Scan(&cursor)
		if err != nil {
			return nil, err
		}
		alert.Cursor = cursor
		eventPayload, _ = json.Marshal(alert)
		if _, err = tx.Exec(ctx, `INSERT INTO alert_event_outbox(alert_id,payload) VALUES($1,$2) ON CONFLICT(alert_id) DO NOTHING`, alert.ID, eventPayload); err != nil {
			return nil, err
		}
		created = append(created, *alert)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return created, nil
}

func (p *Postgres) LoadRuntimeStates(ctx context.Context, ownedPartitions []int) (RuntimeSnapshot, error) {
	snapshot := RuntimeSnapshot{Series: map[string]model.SeriesEvaluationState{}, Definitions: map[string]model.DefinitionEvaluationState{}}
	partitionIDs := smallintPartitions(ownedPartitions)
	rows, err := p.pool.Query(ctx, `SELECT series_key,state FROM alert_series_runtime_state WHERE partition_id=ANY($1) OR partition_id IS NULL`, partitionIDs)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var key string
		var raw []byte
		if err = rows.Scan(&key, &raw); err != nil {
			rows.Close()
			return snapshot, err
		}
		var state model.SeriesEvaluationState
		if err = json.Unmarshal(raw, &state); err != nil {
			rows.Close()
			return snapshot, err
		}
		snapshot.Series[key] = state
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return snapshot, err
	}
	rows.Close()

	rows, err = p.pool.Query(ctx, `SELECT definition_id::text,series_key,state FROM alert_definition_runtime_state WHERE partition_id=ANY($1) OR partition_id IS NULL`, partitionIDs)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var definitionID, seriesKey string
		var raw []byte
		if err = rows.Scan(&definitionID, &seriesKey, &raw); err != nil {
			rows.Close()
			return snapshot, err
		}
		var state model.DefinitionEvaluationState
		if err = json.Unmarshal(raw, &state); err != nil {
			rows.Close()
			return snapshot, err
		}
		snapshot.Definitions[DefinitionStateKey(definitionID, seriesKey)] = state
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return snapshot, err
	}
	rows.Close()

	// One-time compatibility bootstrap for deployments that have v1 evaluator
	// state but no v2 shared-series checkpoint yet.
	if len(snapshot.Series) == 0 {
		rows, err = p.pool.Query(ctx, `SELECT s.definition_id::text,s.series_key,s.state,d.evaluation_mode
			FROM alert_rule_state s JOIN alert_definitions d ON d.id=s.definition_id`)
		if err != nil {
			return snapshot, err
		}
		for rows.Next() {
			var definitionID, seriesKey, mode string
			var raw []byte
			if err = rows.Scan(&definitionID, &seriesKey, &raw, &mode); err != nil {
				rows.Close()
				return snapshot, err
			}
			var legacy model.EvaluationState
			if err = json.Unmarshal(raw, &legacy); err != nil {
				rows.Close()
				return snapshot, err
			}
			definitionState := model.DefinitionEvaluationState{LastTriggerAt: legacy.LastTriggerAt, LastAlertBar: legacy.LastAlertBar}
			if mode == "intrabar" {
				definitionState.LastTruthIntrabar = legacy.LastTruth
			} else {
				definitionState.LastTruthConfirmed = legacy.LastTruth
			}
			snapshot.Definitions[DefinitionStateKey(definitionID, seriesKey)] = definitionState
			current := snapshot.Series[seriesKey]
			if current.LastOpenTime.IsZero() || legacy.LastOpenTime.After(current.LastOpenTime) || (legacy.LastOpenTime.Equal(current.LastOpenTime) && legacy.LastRevision >= current.LastRevision) {
				feature := model.FeatureState{LastClose: legacy.LastClose, Indicators: legacy.Indicators, CandleColors: legacy.CandleColors, FormingBar: legacy.FormingBar, FormingColor: legacy.FormingColor}
				current = model.SeriesEvaluationState{LastOpenTime: legacy.LastOpenTime, LastRevision: legacy.LastRevision, LastStatus: legacy.LastStatus, Confirmed: feature, Intrabar: feature}
				snapshot.Series[seriesKey] = current
			}
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return snapshot, err
		}
		rows.Close()
	}
	return snapshot, nil
}

func (p *Postgres) CommitRuntimeBatch(ctx context.Context, batch RuntimeBatch) ([]model.Alert, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	inboxRows := make([]map[string]any, 0, len(batch.Observations))
	for _, observation := range batch.Observations {
		inboxRows = append(inboxRows, map[string]any{"source_event_id": observation.SourceEventID, "payload": observation})
	}
	if len(inboxRows) > 0 {
		raw, marshalErr := json.Marshal(inboxRows)
		if marshalErr != nil {
			return nil, marshalErr
		}
		if _, err = tx.Exec(ctx, `INSERT INTO alert_inbox(source_event_id,payload)
			SELECT source_event_id,payload FROM jsonb_to_recordset($1::jsonb) AS x(source_event_id text,payload jsonb)
			ON CONFLICT DO NOTHING`, raw); err != nil {
			return nil, err
		}
	}
	seriesRows := make([]map[string]any, 0, len(batch.SeriesStates))
	for seriesKey, state := range batch.SeriesStates {
		seriesRows = append(seriesRows, map[string]any{"series_key": seriesKey, "state": state})
	}
	if len(seriesRows) > 0 {
		raw, marshalErr := json.Marshal(seriesRows)
		if marshalErr != nil {
			return nil, marshalErr
		}
		if _, err = tx.Exec(ctx, `INSERT INTO alert_series_runtime_state(series_key,partition_id,state)
			SELECT series_key,$2,state FROM jsonb_to_recordset($1::jsonb) AS x(series_key text,state jsonb)
			ON CONFLICT(series_key) DO UPDATE SET partition_id=EXCLUDED.partition_id,state=EXCLUDED.state,updated_at=now()`, raw, batch.PartitionID); err != nil {
			return nil, err
		}
	}
	if len(batch.ResetSeriesKeys) > 0 {
		if _, err = tx.Exec(ctx, `DELETE FROM alert_definition_runtime_state WHERE series_key=ANY($1)`, batch.ResetSeriesKeys); err != nil {
			return nil, err
		}
	}
	definitionRows := make([]map[string]any, 0, len(batch.DefinitionStates))
	for stateKey, state := range batch.DefinitionStates {
		definitionID, seriesKey, found := strings.Cut(stateKey, "|")
		if !found {
			return nil, fmt.Errorf("invalid definition runtime key %q", stateKey)
		}
		definitionRows = append(definitionRows, map[string]any{"definition_id": definitionID, "series_key": seriesKey, "state": state})
	}
	if len(definitionRows) > 0 {
		raw, marshalErr := json.Marshal(definitionRows)
		if marshalErr != nil {
			return nil, marshalErr
		}
		if _, err = tx.Exec(ctx, `INSERT INTO alert_definition_runtime_state(definition_id,series_key,partition_id,state)
			SELECT definition_id,series_key,$2,state FROM jsonb_to_recordset($1::jsonb) AS x(definition_id uuid,series_key text,state jsonb)
			ON CONFLICT(definition_id,series_key) DO UPDATE SET partition_id=EXCLUDED.partition_id,state=EXCLUDED.state,updated_at=now()`, raw, batch.PartitionID); err != nil {
			return nil, err
		}
	}
	created := make([]model.Alert, 0, len(batch.Alerts))
	if len(batch.Alerts) > 0 {
		alertRows := make([]map[string]any, 0, len(batch.Alerts))
		for _, alert := range batch.Alerts {
			alertRows = append(alertRows, map[string]any{
				"id": alert.ID, "definition_id": alert.DefinitionID, "source_event_id": alert.SourceEventID,
				"series_key": alert.Series.Key(), "definition_name": alert.DefinitionName,
				"definition_version": alert.DefinitionVersion, "definition_hash": alert.DefinitionHash, "reasons": alert.Reasons,
				"observation": json.RawMessage(alert.Observation), "triggered_at": alert.TriggeredAt,
				"evaluated_at": alert.EvaluatedAt, "feature_algorithm": alert.FeatureAlgorithm,
				"facts_used": alert.FactsUsed, "source_revision": alert.SourceRevision, "source_candle_id": alert.SourceCandleID, "source_status": alert.SourceStatus,
			})
		}
		raw, marshalErr := json.Marshal(alertRows)
		if marshalErr != nil {
			return nil, marshalErr
		}
		rows, queryErr := tx.Query(ctx, `WITH input AS (
			SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(
				id uuid,definition_id uuid,source_event_id text,series_key text,definition_name text,
				definition_version integer,definition_hash text,reasons jsonb,observation jsonb,triggered_at timestamptz,
				evaluated_at timestamptz,feature_algorithm jsonb,facts_used jsonb,source_revision bigint,source_candle_id text,source_status text)
		), inserted AS (
			INSERT INTO alert_events(id,definition_id,source_event_id,series_key,definition_name,definition_version,definition_hash,reasons,observation,triggered_at,evaluated_at,feature_algorithm,facts_used,source_revision,source_candle_id,source_status)
			SELECT id,definition_id,source_event_id,series_key,definition_name,definition_version,definition_hash,reasons,observation,triggered_at,evaluated_at,feature_algorithm,COALESCE(facts_used,'[]'::jsonb),source_revision,source_candle_id,source_status FROM input
			ON CONFLICT(definition_id,source_event_id) DO UPDATE SET id=alert_events.id
			RETURNING id,cursor)
			SELECT id::text,cursor FROM inserted`, raw)
		if queryErr != nil {
			return nil, queryErr
		}
		cursors := make(map[string]int64, len(batch.Alerts))
		for rows.Next() {
			var id string
			var cursor int64
			if err = rows.Scan(&id, &cursor); err != nil {
				rows.Close()
				return nil, err
			}
			cursors[id] = cursor
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		outboxRows := make([]map[string]any, 0, len(batch.Alerts))
		for _, alert := range batch.Alerts {
			alert.Cursor = cursors[alert.ID]
			outboxRows = append(outboxRows, map[string]any{"alert_id": alert.ID, "payload": alert})
			created = append(created, alert)
		}
		raw, marshalErr = json.Marshal(outboxRows)
		if marshalErr != nil {
			return nil, marshalErr
		}
		if _, err = tx.Exec(ctx, `INSERT INTO alert_event_outbox(alert_id,payload)
			SELECT alert_id,payload FROM jsonb_to_recordset($1::jsonb) AS x(alert_id uuid,payload jsonb)
			ON CONFLICT(alert_id) DO NOTHING`, raw); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return created, nil
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

func (p *Postgres) ListAlerts(ctx context.Context, after int64, limit int) ([]model.Alert, error) {
	rows, err := p.pool.Query(ctx, `SELECT e.cursor,o.payload,o.published_at FROM alert_events e JOIN alert_event_outbox o ON o.alert_id=e.id WHERE e.cursor>$1 ORDER BY e.cursor LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Alert{}
	for rows.Next() {
		var a model.Alert
		var raw []byte
		var published *time.Time
		if err = rows.Scan(&a.Cursor, &raw, &published); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		if published != nil {
			a.PublishStatus = "published"
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (p *Postgres) ClaimPending(ctx context.Context, limit int) ([]model.Alert, error) {
	rows, err := p.pool.Query(ctx, `WITH claimed AS (SELECT cursor FROM alert_event_outbox WHERE published_at IS NULL AND next_attempt_at<=now() ORDER BY cursor FOR UPDATE SKIP LOCKED LIMIT $1) UPDATE alert_event_outbox o SET attempts=attempts+1,locked_at=now(),next_attempt_at=now()+interval '30 seconds' FROM claimed WHERE o.cursor=claimed.cursor RETURNING o.payload`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Alert{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		var a model.Alert
		if err = json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (p *Postgres) MarkPublished(ctx context.Context, id string) error {
	_, err := p.pool.Exec(ctx, `UPDATE alert_event_outbox SET published_at=now(),locked_at=NULL,last_error=NULL WHERE alert_id=$1`, id)
	return err
}
func (p *Postgres) MarkPublishFailed(ctx context.Context, id, message string) error {
	_, err := p.pool.Exec(ctx, `UPDATE alert_event_outbox SET locked_at=NULL,last_error=$2,next_attempt_at=now()+(least(300,power(2,least(attempts,8)))::text||' seconds')::interval WHERE alert_id=$1`, id, message)
	return err
}

func (p *Postgres) Maintain(ctx context.Context, policy RetentionPolicy) (MaintenanceResult, error) {
	if policy.BatchSize <= 0 {
		policy.BatchSize = 5000
	}
	result := MaintenanceResult{}
	// Alert events are retained for audit and replay for the configured window.
	// Unpublished events are never deleted; the outbox FK cascades only after a
	// broker-acknowledged event has exceeded retention.
	if policy.AlertEvents > 0 {
		tag, err := p.pool.Exec(ctx, `WITH doomed AS (
			SELECT e.ctid FROM alert_events e JOIN alert_event_outbox o ON o.alert_id=e.id
			WHERE e.triggered_at < now()-$1::interval AND o.published_at IS NOT NULL
			ORDER BY e.triggered_at LIMIT $2)
			DELETE FROM alert_events e USING doomed WHERE e.ctid=doomed.ctid`, policy.AlertEvents.String(), policy.BatchSize)
		if err != nil {
			return result, err
		}
		result.AlertsDeleted = tag.RowsAffected()
	}
	if policy.ProvisionalInbox > 0 && policy.ConfirmedInbox > 0 {
		tag, err := p.pool.Exec(ctx, `WITH doomed AS (
			SELECT i.ctid FROM alert_inbox i
			WHERE NOT EXISTS (SELECT 1 FROM alert_events e WHERE e.source_event_id=i.source_event_id)
			AND ((i.payload->>'status'='provisional' AND i.received_at < now()-$1::interval)
			  OR (i.payload->>'status'<>'provisional' AND i.received_at < now()-$2::interval))
			ORDER BY i.received_at LIMIT $3)
			DELETE FROM alert_inbox i USING doomed WHERE i.ctid=doomed.ctid`, policy.ProvisionalInbox.String(), policy.ConfirmedInbox.String(), policy.BatchSize)
		if err != nil {
			return result, err
		}
		result.InboxDeleted = tag.RowsAffected()
	}
	return result, nil
}

func (p *Postgres) AcquirePartitions(ctx context.Context, owner string, partitions []int, ttl time.Duration) (bool, error) {
	if len(partitions) == 0 {
		return false, fmt.Errorf("at least one partition lease is required")
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	partitionIDs := smallintPartitions(partitions)
	rows, err := tx.Query(ctx, `INSERT INTO alert_partition_leases(partition_id,owner_id,expires_at)
		SELECT unnest($1::smallint[]),$2,now()+$3::interval
		ON CONFLICT(partition_id) DO UPDATE SET owner_id=EXCLUDED.owner_id,expires_at=EXCLUDED.expires_at,updated_at=now()
		WHERE alert_partition_leases.owner_id=EXCLUDED.owner_id OR alert_partition_leases.expires_at<=now()
		RETURNING partition_id`, partitionIDs, owner, ttl.String())
	if err != nil {
		return false, err
	}
	count := 0
	for rows.Next() {
		count++
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	rows.Close()
	if count != len(partitions) {
		return false, nil
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (p *Postgres) RenewPartitions(ctx context.Context, owner string, partitions []int, ttl time.Duration) (bool, error) {
	tag, err := p.pool.Exec(ctx, `UPDATE alert_partition_leases SET expires_at=now()+$3::interval,updated_at=now()
		WHERE owner_id=$1 AND partition_id=ANY($2)`, owner, smallintPartitions(partitions), ttl.String())
	return err == nil && tag.RowsAffected() == int64(len(partitions)), err
}

func smallintPartitions(partitions []int) []int16 {
	result := make([]int16, len(partitions))
	for index, partition := range partitions {
		result[index] = int16(partition)
	}
	return result
}

func (p *Postgres) ReleasePartitions(ctx context.Context, owner string) error {
	_, err := p.pool.Exec(ctx, `DELETE FROM alert_partition_leases WHERE owner_id=$1`, owner)
	return err
}

func (p *Postgres) OperationalStats(ctx context.Context) (OperationalStats, error) {
	var stats OperationalStats
	var oldestSeconds float64
	err := p.pool.QueryRow(ctx, `SELECT count(*),COALESCE(extract(epoch FROM now()-min(created_at)),0)
		FROM alert_event_outbox WHERE published_at IS NULL`).Scan(&stats.PendingOutbox, &oldestSeconds)
	stats.OldestOutboxAge = time.Duration(oldestSeconds * float64(time.Second))
	return stats, err
}
