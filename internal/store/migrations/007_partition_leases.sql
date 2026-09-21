CREATE TABLE IF NOT EXISTS alert_partition_leases (
  partition_id smallint PRIMARY KEY CHECK (partition_id >= 0 AND partition_id < 256),
  owner_id text NOT NULL,
  expires_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alert_partition_leases_expiry
  ON alert_partition_leases(expires_at);
