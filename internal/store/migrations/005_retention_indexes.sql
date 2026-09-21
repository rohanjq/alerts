CREATE INDEX IF NOT EXISTS alert_inbox_provisional_received
  ON alert_inbox(received_at)
  WHERE payload->>'status'='provisional';

CREATE INDEX IF NOT EXISTS alert_inbox_confirmed_received
  ON alert_inbox(received_at)
  WHERE payload->>'status'<>'provisional';
