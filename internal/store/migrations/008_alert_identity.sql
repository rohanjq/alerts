ALTER TABLE alert_events
  ADD COLUMN IF NOT EXISTS definition_hash text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS source_candle_id text NOT NULL DEFAULT '';
