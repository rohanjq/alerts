ALTER TABLE alert_event_outbox
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS alert_outbox_pending_created
  ON alert_event_outbox(created_at)
  WHERE published_at IS NULL;
