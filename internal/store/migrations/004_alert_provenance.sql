ALTER TABLE alert_events
  ADD COLUMN IF NOT EXISTS definition_version integer NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS evaluated_at timestamptz,
  ADD COLUMN IF NOT EXISTS feature_algorithm jsonb NOT NULL DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS facts_used jsonb NOT NULL DEFAULT '[]',
  ADD COLUMN IF NOT EXISTS source_revision bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS source_status text NOT NULL DEFAULT 'confirmed';

UPDATE alert_events SET evaluated_at=triggered_at WHERE evaluated_at IS NULL;
ALTER TABLE alert_events ALTER COLUMN evaluated_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS alert_events_series_time
  ON alert_events(series_key,triggered_at DESC);
