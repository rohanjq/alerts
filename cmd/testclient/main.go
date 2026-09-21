package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rohanjq/alerts/internal/broker"
	"github.com/rohanjq/alerts/internal/model"
)

type options struct {
	apiURL, apiToken, natsURL, natsToken string
	dataset, symbol, timeframe, durable  string
	maxEvents                            int
	timeout                              time.Duration
	jsonOutput                           bool
}

type createRequest struct {
	Name            string       `json:"name"`
	Series          model.Series `json:"series"`
	Rule            model.Rule   `json:"rule"`
	EvaluationMode  string       `json:"evaluation_mode"`
	TriggerMode     string       `json:"trigger_mode"`
	CooldownSeconds int          `json:"cooldown_seconds"`
}

func main() {
	opts := parseOptions()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if opts.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.timeout)
		defer cancel()
	}
	if err := run(ctx, opts); err != nil {
		fmt.Fprintln(os.Stderr, "test client:", err)
		os.Exit(1)
	}
}

func parseOptions() options {
	var opts options
	flag.StringVar(&opts.apiURL, "api-url", env("ALERTS_API_URL", "http://127.0.0.1:8100"), "Alerts control API URL")
	flag.StringVar(&opts.apiToken, "api-token", os.Getenv("ALERTS_API_TOKEN"), "Alerts control API bearer token")
	flag.StringVar(&opts.natsURL, "nats-url", env("ALERTS_NATS_URL", nats.DefaultURL), "NATS server URL")
	flag.StringVar(&opts.natsToken, "nats-token", os.Getenv("ALERTS_NATS_TOKEN"), "NATS authentication token")
	flag.StringVar(&opts.dataset, "dataset", env("ALERTS_TEST_DATASET", "live"), "market dataset")
	flag.StringVar(&opts.symbol, "symbol", env("ALERTS_TEST_SYMBOL", "BTCUSDT"), "market symbol")
	flag.StringVar(&opts.timeframe, "timeframe", env("ALERTS_TEST_TIMEFRAME", "1m"), "single timeframe for all five test definitions")
	flag.StringVar(&opts.durable, "durable", env("ALERTS_TEST_DURABLE", "ALERTS_TEST_CLIENT_V1"), "durable consumer name")
	flag.IntVar(&opts.maxEvents, "max-events", 0, "exit successfully after this many alert events; zero waits forever")
	flag.DurationVar(&opts.timeout, "timeout", 0, "fail if max-events is not reached in this time; zero disables")
	flag.BoolVar(&opts.jsonOutput, "json", false, "print the complete alert event instead of a concise line")
	flag.Parse()
	return opts
}

func run(ctx context.Context, opts options) error {
	if opts.apiURL == "" || opts.natsURL == "" || opts.symbol == "" || opts.timeframe == "" || opts.durable == "" {
		return errors.New("API URL, NATS URL, symbol, timeframe, and durable are required")
	}
	natsOptions := []nats.Option{nats.Name("alerts-test-client"), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second), nats.Timeout(5 * time.Second)}
	if opts.natsToken != "" {
		natsOptions = append(natsOptions, nats.Token(opts.natsToken))
	}
	nc, err := nats.Connect(opts.natsURL, natsOptions...)
	if err != nil {
		return fmt.Errorf("connect NATS: %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("open JetStream: %w", err)
	}
	subscription, err := subscribe(ctx, js, opts.durable)
	if err != nil {
		return err
	}

	definitions, err := registerDefinitions(ctx, opts)
	if err != nil {
		return err
	}
	for _, definition := range definitions {
		fmt.Printf("registered %-28s id=%s\n", definition.Name, definition.ID)
	}
	fmt.Printf("subscribed durable=%s subject=%s; waiting for every generated alert\n", opts.durable, broker.AlertSubjects)

	matched := 0
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("stopped after %d alert events: %w", matched, err)
		}
		messages, fetchErr := subscription.Fetch(1, nats.MaxWait(time.Second))
		if fetchErr != nil && !errors.Is(fetchErr, nats.ErrTimeout) {
			return fmt.Errorf("fetch alert events: %w", fetchErr)
		}
		for _, message := range messages {
			var alert model.Alert
			if err := json.Unmarshal(message.Data, &alert); err != nil {
				_ = message.Term()
				fmt.Fprintf(os.Stderr, "discarded malformed alert event: %v\n", err)
				continue
			}
			if err := message.AckSync(nats.Context(ctx)); err != nil {
				return fmt.Errorf("ack alert %s: %w", alert.ID, err)
			}
			if opts.jsonOutput {
				encoded, _ := json.Marshal(alert)
				fmt.Println(string(encoded))
			} else {
				fmt.Printf("ALERT time=%s definition=%q id=%s reasons=%q\n", alert.TriggeredAt.Format(time.RFC3339), alert.DefinitionName, alert.ID, strings.Join(alert.Reasons, "; "))
			}
			matched++
			if opts.maxEvents > 0 && matched >= opts.maxEvents {
				return nil
			}
		}
	}
}

func subscribe(ctx context.Context, js nats.JetStreamContext, durable string) (*nats.Subscription, error) {
	info, err := js.ConsumerInfo(broker.AlertStream, durable, nats.Context(ctx))
	if errors.Is(err, nats.ErrConsumerNotFound) {
		subscription, createErr := js.PullSubscribe(broker.AlertSubjects, durable, nats.BindStream(broker.AlertStream), nats.ManualAck(), nats.AckExplicit(), nats.DeliverNew(), nats.AckWait(30*time.Second), nats.MaxDeliver(20))
		if createErr != nil {
			return nil, fmt.Errorf("create alert subscription: %w", createErr)
		}
		return subscription, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect alert subscription: %w", err)
	}
	if info.Config.FilterSubject != broker.AlertSubjects || info.Config.AckPolicy != nats.AckExplicitPolicy {
		return nil, errors.New("existing test-client durable has an incompatible configuration")
	}
	subscription, err := js.PullSubscribe(broker.AlertSubjects, durable, nats.Bind(broker.AlertStream, durable))
	if err != nil {
		return nil, fmt.Errorf("bind alert subscription: %w", err)
	}
	return subscription, nil
}

func registerDefinitions(ctx context.Context, opts options) ([]model.AlertDefinition, error) {
	series := model.Series{Dataset: opts.dataset, Symbol: strings.ToUpper(opts.symbol), Timeframe: opts.timeframe}
	requests := []createRequest{
		{Name: "Test: green candle", Series: series, Rule: model.Rule{Type: "candle_color", Direction: "green"}, EvaluationMode: "confirmed_close", TriggerMode: "once_per_bar"},
		{Name: "Test: red candle", Series: series, Rule: model.Rule{Type: "candle_color", Direction: "red"}, EvaluationMode: "confirmed_close", TriggerMode: "once_per_bar"},
		{Name: "Test: two consecutive red candles", Series: series, Rule: model.Rule{Type: "candle_streak", Direction: "red", Count: 2}, EvaluationMode: "confirmed_close", TriggerMode: "on_occurrence"},
		{Name: "Test: two consecutive green candles", Series: series, Rule: model.Rule{Type: "candle_streak", Direction: "green", Count: 2}, EvaluationMode: "confirmed_close", TriggerMode: "on_occurrence"},
		{Name: "Test: candle flips after 50%", Series: series, Rule: model.Rule{Type: "candle_color_flip", Direction: "either", MinElapsedPercent: 50}, EvaluationMode: "intrabar", TriggerMode: "once_per_bar"},
	}
	client := &http.Client{Timeout: 10 * time.Second}
	definitions := make([]model.AlertDefinition, 0, len(requests))
	for _, request := range requests {
		definition, err := register(ctx, client, opts.apiURL, opts.apiToken, request)
		if err != nil {
			return nil, fmt.Errorf("register %q: %w", request.Name, err)
		}
		definitions = append(definitions, definition)
	}
	return definitions, nil
}

func register(ctx context.Context, client *http.Client, baseURL, token string, definition createRequest) (model.AlertDefinition, error) {
	payload, err := json.Marshal(definition)
	if err != nil {
		return model.AlertDefinition{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/v1/definitions", bytes.NewReader(payload))
	if err != nil {
		return model.AlertDefinition{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return model.AlertDefinition{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return model.AlertDefinition{}, fmt.Errorf("API returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var result model.AlertDefinition
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return model.AlertDefinition{}, err
	}
	return result, nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
