ALTER TABLE alert_definitions
  ADD COLUMN IF NOT EXISTS effective_at timestamptz;

UPDATE alert_definitions SET effective_at=created_at WHERE effective_at IS NULL;

ALTER TABLE alert_definitions
  ALTER COLUMN effective_at SET DEFAULT now(),
  ALTER COLUMN effective_at SET NOT NULL;

CREATE TABLE IF NOT EXISTS alert_series_runtime_state (
  series_key text PRIMARY KEY,
  state jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alert_definition_runtime_state (
  definition_id uuid NOT NULL REFERENCES alert_definitions(id) ON DELETE CASCADE,
  series_key text NOT NULL,
  state jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(definition_id,series_key)
);

-- Operational retention workers use these indexes for bounded batched deletes
-- or partition migrations without scanning the primary tables.
CREATE INDEX IF NOT EXISTS alert_inbox_status_received
  ON alert_inbox(status,received_at);
CREATE INDEX IF NOT EXISTS alert_outbox_published_time
  ON alert_event_outbox(published_at) WHERE published_at IS NOT NULL;
