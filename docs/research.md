# Design references

The architecture follows established event-processing patterns rather than a
trading-alert tutorial:

- [Prometheus alerting overview](https://prometheus.io/docs/alerting/latest/overview/)
  separates rule evaluation from downstream grouping, routing, silencing, and
  delivery. This is why `alertsd` produces alert facts and contains no users.
- [TradingView alert behavior](https://www.tradingview.com/pine-script-docs/concepts/alerts/)
  snapshots alert configuration and distinguishes intrabar frequency from bar
  close behavior. Definitions here are immutable/content-addressed and expose
  `confirmed_close`, `intrabar`, and explicit trigger modes.
- [Debezium outbox event router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
  documents atomic state-plus-event publication. Alerts commits inbox, rule
  state, alert occurrence, and outbox together.
- [Kafka delivery semantics](https://docs.confluent.io/kafka/design/delivery-semantics.html)
  explains why at-least-once delivery can repeat. Deterministic IDs and unique
  constraints make those repeats harmless here.
- [NATS JetStream model](https://github.com/nats-io/nats.docs/blob/master/using-nats/jetstream/model_deep_dive.md)
  covers the implemented explicit acknowledgement, durable pull consumer, and
  message-ID deduplication behavior.
- [Apache Flink CEP](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/)
  and the [Dataflow model paper](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/)
  motivate keyed state, event-time ordering, explicit late-event handling, and
  separating correctness from latency.

Mature open-source references reviewed were Prometheus/Alertmanager, Apache
Flink, NATS Server/JetStream, and Debezium's outbox examples. Their reusable
patterns are stronger foundations than small trading-alert repositories, which
usually combine signal computation, users, and notification side effects in a
single process.
