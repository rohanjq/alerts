package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rohanjq/alerts/internal/model"
	signalsadapter "github.com/rohanjq/alerts/internal/signals"
)

const (
	EvaluationStream     = "MARKET_EVALUATION"
	EvaluationSubjects   = "signals.evaluation.>"
	EvaluationPartitions = 256
	AlertStream          = "ALERT_EVENTS"
	AlertSubjects        = "alerts.events.>"
	DeadLetterStream     = "ALERTS_DLQ"
	DeadLetterSubject    = "alerts.dlq.signals"
)

type Sink interface {
	IngestBatch(context.Context, int, []model.Observation) ([]model.Alert, error)
}

type JetStream struct {
	connection    *nats.Conn
	context       nats.JetStreamContext
	durable       string
	ackWait       time.Duration
	maxDeliveries int
	logger        *slog.Logger
	workerIndex   int
	workerCount   int
	batchSize     int
}

type Options struct {
	URL, Token, Durable                 string
	Replicas, MaxDeliveries             int
	AckWait                             time.Duration
	WorkerIndex, WorkerCount, BatchSize int
	Logger                              *slog.Logger
}

func Open(ctx context.Context, options Options) (*JetStream, error) {
	if options.Replicas < 1 {
		options.Replicas = 1
	}
	if options.MaxDeliveries < 1 {
		options.MaxDeliveries = 20
	}
	if options.AckWait <= 0 {
		options.AckWait = 30 * time.Second
	}
	if options.Durable == "" {
		options.Durable = "ALERTS_EVALUATOR_V3"
	}
	if options.WorkerCount < 1 {
		options.WorkerCount = 1
	}
	if options.WorkerIndex < 0 || options.WorkerIndex >= options.WorkerCount {
		return nil, fmt.Errorf("worker index must be between 0 and worker count minus one")
	}
	if options.BatchSize < 1 {
		options.BatchSize = 32
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	nc, err := nats.Connect(options.URL, nats.Name("alertsd"), nats.Token(options.Token), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second), nats.Timeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(4096))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("open JetStream: %w", err)
	}
	for _, config := range []*nats.StreamConfig{
		{Name: EvaluationStream, Subjects: []string{EvaluationSubjects}, Retention: nats.LimitsPolicy, Storage: nats.FileStorage, Replicas: options.Replicas, MaxAge: 7 * 24 * time.Hour, MaxMsgSize: 2 << 20, Duplicates: 10 * time.Minute, AllowRollup: true},
		{Name: AlertStream, Subjects: []string{AlertSubjects}, Retention: nats.LimitsPolicy, Storage: nats.FileStorage, Replicas: options.Replicas, MaxAge: 30 * 24 * time.Hour, MaxMsgSize: 2 << 20, Duplicates: 10 * time.Minute},
		{Name: DeadLetterStream, Subjects: []string{DeadLetterSubject}, Retention: nats.LimitsPolicy, Storage: nats.FileStorage, Replicas: options.Replicas, MaxAge: 30 * 24 * time.Hour, MaxMsgSize: 2 << 20, Duplicates: 10 * time.Minute},
	} {
		if err = ensureStream(ctx, js, config); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return &JetStream{connection: nc, context: js, durable: options.Durable, ackWait: options.AckWait, maxDeliveries: options.MaxDeliveries, logger: options.Logger, workerIndex: options.WorkerIndex, workerCount: options.WorkerCount, batchSize: options.BatchSize}, nil
}

func ensureStream(ctx context.Context, js nats.JetStreamContext, config *nats.StreamConfig) error {
	if info, err := js.StreamInfo(config.Name, nats.Context(ctx)); err == nil {
		if sameStreamConfig(info.Config, *config) {
			return nil
		}
		if _, err = js.UpdateStream(config, nats.Context(ctx)); err != nil {
			return fmt.Errorf("reconcile JetStream %s: %w", config.Name, err)
		}
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return fmt.Errorf("inspect JetStream %s: %w", config.Name, err)
	}
	if _, err := js.AddStream(config, nats.Context(ctx)); err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("create JetStream %s: %w", config.Name, err)
	}
	return nil
}

func sameStreamConfig(got, want nats.StreamConfig) bool {
	return got.Name == want.Name && len(got.Subjects) == len(want.Subjects) && len(got.Subjects) == 1 && got.Subjects[0] == want.Subjects[0] &&
		got.Retention == want.Retention && got.Storage == want.Storage && got.Replicas == want.Replicas && got.MaxAge == want.MaxAge &&
		got.MaxMsgsPerSubject == want.MaxMsgsPerSubject && got.MaxMsgSize == want.MaxMsgSize && got.Duplicates == want.Duplicates && got.AllowRollup == want.AllowRollup
}

func (j *JetStream) Close() {
	_ = j.connection.Drain()
	j.connection.Close()
}

func (j *JetStream) Ready(_ context.Context) bool { return j.connection.IsConnected() }

func (j *JetStream) Publish(ctx context.Context, alert model.Alert) error {
	// Publication state belongs to the local outbox and cannot truthfully be
	// part of an event until after this publish has been acknowledged.
	alert.PublishStatus = ""
	payload, err := json.Marshal(alert)
	if err != nil {
		return err
	}
	message := nats.NewMsg(alertSubject(alert))
	message.Data = payload
	message.Header.Set(nats.MsgIdHdr, alert.ID)
	message.Header.Set("Content-Type", "application/json")
	if _, err = j.context.PublishMsg(message, nats.Context(ctx)); err != nil {
		return fmt.Errorf("publish alert event: %w", err)
	}
	return nil
}

func (j *JetStream) Consume(ctx context.Context, sink Sink) error {
	consumerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	owned := 0
	for shard := 0; shard < EvaluationPartitions; shard++ {
		if shard%j.workerCount == j.workerIndex {
			owned++
		}
	}
	errors := make(chan error, owned)
	for shard := 0; shard < EvaluationPartitions; shard++ {
		if shard%j.workerCount != j.workerIndex {
			continue
		}
		go func(shard int) {
			subject := fmt.Sprintf("signals.evaluation.%03d.>", shard)
			durable := fmt.Sprintf("%s_%03d", j.durable, shard)
			errors <- j.consumeShard(consumerCtx, sink, shard, EvaluationStream, subject, durable)
		}(shard)
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-errors:
		return err
	}
}

func (j *JetStream) consumeShard(ctx context.Context, sink Sink, partition int, stream, subject, durable string) error {
	subscription, err := j.shardSubscription(ctx, stream, subject, durable)
	if err != nil {
		return fmt.Errorf("create durable fact consumer %s: %w", durable, err)
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		messages, fetchErr := subscription.Fetch(j.batchSize, nats.MaxWait(time.Second))
		if fetchErr != nil && !errors.Is(fetchErr, nats.ErrTimeout) {
			return fmt.Errorf("fetch facts: %w", fetchErr)
		}
		if len(messages) == 0 {
			continue
		}
		if err = j.consumeBatch(ctx, sink, partition, messages); err != nil {
			j.logger.Error("consume market evaluation batch", "partition", partition, "err", err)
		}
	}
}

func (j *JetStream) shardSubscription(ctx context.Context, stream, subject, durable string) (*nats.Subscription, error) {
	info, err := j.context.ConsumerInfo(stream, durable, nats.Context(ctx))
	if errors.Is(err, nats.ErrConsumerNotFound) {
		return j.context.PullSubscribe(subject, durable, nats.BindStream(stream), nats.ManualAck(), nats.AckAll(), nats.AckWait(j.ackWait), nats.MaxDeliver(j.maxDeliveries), nats.MaxAckPending(j.batchSize))
	}
	if err != nil {
		return nil, err
	}
	if info.Config.FilterSubject != subject || info.Config.AckPolicy != nats.AckAllPolicy {
		return nil, fmt.Errorf("existing consumer has incompatible filter or acknowledgement policy")
	}
	if info.Config.AckWait != j.ackWait || info.Config.MaxDeliver != j.maxDeliveries || info.Config.MaxAckPending != j.batchSize {
		config := info.Config
		config.AckWait = j.ackWait
		config.MaxDeliver = j.maxDeliveries
		config.MaxAckPending = j.batchSize
		if _, err = j.context.UpdateConsumer(stream, &config, nats.Context(ctx)); err != nil {
			return nil, fmt.Errorf("update consumer policy: %w", err)
		}
	}
	return j.context.PullSubscribe(subject, durable, nats.Bind(stream, durable))
}

func (j *JetStream) consumeBatch(ctx context.Context, sink Sink, partition int, messages []*nats.Msg) error {
	observations := make([]model.Observation, 0, len(messages))
	validMessages := make([]*nats.Msg, 0, len(messages))
	for _, message := range messages {
		var event signalsadapter.Event
		if err := json.Unmarshal(message.Data, &event); err != nil {
			if deadLetterErr := j.deadLetter(ctx, message, "decode event: "+err.Error()); deadLetterErr != nil {
				return deadLetterErr
			}
			continue
		}
		observation, err := signalsadapter.Observation(event)
		if err != nil {
			if deadLetterErr := j.deadLetter(ctx, message, "invalid event: "+err.Error()); deadLetterErr != nil {
				return deadLetterErr
			}
			continue
		}
		observations = append(observations, observation)
		validMessages = append(validMessages, message)
	}
	if len(validMessages) == 0 {
		return nil
	}
	if _, err := sink.IngestBatch(ctx, partition, observations); err != nil {
		for _, message := range validMessages {
			metadata, metadataErr := message.Metadata()
			if metadataErr == nil && int(metadata.NumDelivered) >= j.maxDeliveries {
				if deadLetterErr := j.deadLetter(ctx, message, "evaluation failed after retries: "+err.Error()); deadLetterErr != nil {
					return deadLetterErr
				}
				continue
			}
			delivery := uint64(1)
			if metadataErr == nil {
				delivery = metadata.NumDelivered
			}
			if nakErr := message.NakWithDelay(retryDelay(delivery)); nakErr != nil {
				return fmt.Errorf("evaluation failed (%v), nak failed: %w", err, nakErr)
			}
		}
		return fmt.Errorf("evaluation batch failed; retry scheduled: %w", err)
	}
	return validMessages[len(validMessages)-1].AckSync(nats.Context(ctx))
}

func retryDelay(delivery uint64) time.Duration {
	if delivery == 0 {
		delivery = 1
	}
	exponent := delivery - 1
	if exponent > 9 {
		exponent = 9
	}
	delay := time.Second * time.Duration(uint64(1)<<exponent)
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

func (j *JetStream) deadLetter(ctx context.Context, source *nats.Msg, reason string) error {
	message := nats.NewMsg(DeadLetterSubject)
	message.Data = source.Data
	message.Header.Set("X-Dead-Letter-Reason", reason)
	message.Header.Set("X-Original-Subject", source.Subject)
	if id := source.Header.Get(nats.MsgIdHdr); id != "" {
		message.Header.Set(nats.MsgIdHdr, "dlq-"+id)
	}
	if _, err := j.context.PublishMsg(message, nats.Context(ctx)); err != nil {
		_ = source.NakWithDelay(5 * time.Second)
		return fmt.Errorf("publish dead letter: %w", err)
	}
	if err := source.Term(); err != nil {
		return fmt.Errorf("terminate poison message: %w", err)
	}
	j.logger.Error("market fact moved to dead letter", "reason", reason)
	return nil
}

func alertSubject(alert model.Alert) string {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(alert.Series.Key()))
	return fmt.Sprintf("alerts.events.%03d", hash.Sum32()%EvaluationPartitions)
}
