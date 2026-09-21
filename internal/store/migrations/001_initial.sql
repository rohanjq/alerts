CREATE TABLE IF NOT EXISTS alert_definitions (
  id uuid PRIMARY KEY,
  version integer NOT NULL DEFAULT 1,
  definition_hash text NOT NULL UNIQUE,
  name text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  dataset text NOT NULL DEFAULT '',
  symbol text NOT NULL,
  timeframe text NOT NULL,
  rule jsonb NOT NULL,
  evaluation_mode text NOT NULL CHECK (evaluation_mode IN ('confirmed_close','intrabar','both')),
  trigger_mode text NOT NULL CHECK (trigger_mode IN ('on_occurrence','once_per_bar','every_match')),
  cooldown_seconds integer NOT NULL DEFAULT 0 CHECK (cooldown_seconds >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  archived_at timestamptz
);
CREATE INDEX IF NOT EXISTS alert_definitions_route
  ON alert_definitions (symbol,timeframe) WHERE enabled;

-- Deduplicates replayed observations before state or alert creation.
CREATE TABLE IF NOT EXISTS alert_inbox (
  source_event_id text PRIMARY KEY,
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'accepted',
  received_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alert_rule_state (
  definition_id uuid NOT NULL REFERENCES alert_definitions(id) ON DELETE CASCADE,
  series_key text NOT NULL,
  state jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(definition_id,series_key)
);

-- Durable alert facts. Downstream systems subscribe to these; no user or
-- notification-destination data belongs here.
CREATE TABLE IF NOT EXISTS alert_events (
  id uuid PRIMARY KEY,
  cursor bigserial UNIQUE NOT NULL,
  definition_id uuid NOT NULL REFERENCES alert_definitions(id) ON DELETE CASCADE,
  source_event_id text NOT NULL REFERENCES alert_inbox(source_event_id),
  series_key text NOT NULL,
  definition_name text NOT NULL,
  reasons jsonb NOT NULL,
  observation jsonb NOT NULL,
  triggered_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(definition_id,source_event_id)
);
CREATE INDEX IF NOT EXISTS alert_events_time ON alert_events(triggered_at DESC);

-- Transactional publication outbox, not a user notification queue.
CREATE TABLE IF NOT EXISTS alert_event_outbox (
  cursor bigserial PRIMARY KEY,
  alert_id uuid NOT NULL UNIQUE REFERENCES alert_events(id) ON DELETE CASCADE,
  payload jsonb NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz,
  published_at timestamptz,
  last_error text
);
CREATE INDEX IF NOT EXISTS alert_event_outbox_work
  ON alert_event_outbox(next_attempt_at,cursor) WHERE published_at IS NULL;
