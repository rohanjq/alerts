DROP INDEX IF EXISTS alert_definitions_route;
CREATE INDEX alert_definitions_route
  ON alert_definitions (symbol,timeframe,dataset,id)
  WHERE enabled AND archived_at IS NULL;

CREATE INDEX alert_inbox_received_at ON alert_inbox(received_at);
CREATE INDEX alert_events_definition_time
  ON alert_events(definition_id,triggered_at DESC);
