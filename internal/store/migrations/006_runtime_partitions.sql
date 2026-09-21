ALTER TABLE alert_series_runtime_state
  ADD COLUMN IF NOT EXISTS partition_id smallint;

ALTER TABLE alert_definition_runtime_state
  ADD COLUMN IF NOT EXISTS partition_id smallint;

CREATE INDEX IF NOT EXISTS alert_series_runtime_partition
  ON alert_series_runtime_state(partition_id,series_key);

CREATE INDEX IF NOT EXISTS alert_definition_runtime_partition
  ON alert_definition_runtime_state(partition_id,series_key);
