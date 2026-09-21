package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr, APIToken, IngestToken, DatabaseURL                                                     string
	NATSURL, NATSToken, NATSDurable                                                              string
	NATSReplicas, NATSMaxDeliveries                                                              int
	WorkerIndex, WorkerCount, EvaluationBatchSize                                                int
	NATSAckWait                                                                                  time.Duration
	DefinitionRefreshInterval, DefinitionActivationDelay, DefinitionMaxStaleness                 time.Duration
	MaintenanceInterval, ProvisionalInboxRetention, ConfirmedInboxRetention, AlertEventRetention time.Duration
	PartitionLeaseTTL, PartitionLeaseRenewInterval                                               time.Duration
	MaintenanceBatchSize                                                                         int
	InstanceID                                                                                   string
	Insecure, AllowMemory                                                                        bool
}

func Load() (Config, error) {
	c := Config{Addr: env("ALERTS_HTTP_ADDR", ":8100"), APIToken: os.Getenv("ALERTS_API_TOKEN"), IngestToken: os.Getenv("ALERTS_INGEST_TOKEN"), DatabaseURL: os.Getenv("ALERTS_DATABASE_URL"), NATSURL: os.Getenv("ALERTS_NATS_URL"), NATSToken: os.Getenv("ALERTS_NATS_TOKEN"), NATSDurable: env("ALERTS_NATS_DURABLE", "ALERTS_EVALUATOR_V3"), Insecure: strings.EqualFold(os.Getenv("ALERTS_INSECURE_NO_AUTH"), "true"), AllowMemory: strings.EqualFold(os.Getenv("ALERTS_ALLOW_MEMORY_STORE"), "true")}
	var err error
	if c.NATSReplicas, err = integer("ALERTS_NATS_REPLICAS", 1, 1, 5); err != nil {
		return Config{}, err
	}
	if c.NATSMaxDeliveries, err = integer("ALERTS_NATS_MAX_DELIVERIES", 20, 1, 100); err != nil {
		return Config{}, err
	}
	if c.WorkerCount, err = integer("ALERTS_WORKER_COUNT", 1, 1, 256); err != nil {
		return Config{}, err
	}
	if c.WorkerIndex, err = integer("ALERTS_WORKER_INDEX", 0, 0, c.WorkerCount-1); err != nil {
		return Config{}, err
	}
	if c.EvaluationBatchSize, err = integer("ALERTS_EVALUATION_BATCH_SIZE", 32, 1, 1000); err != nil {
		return Config{}, err
	}
	if c.NATSAckWait, err = duration("ALERTS_NATS_ACK_WAIT", 30*time.Second); err != nil {
		return Config{}, err
	}
	if c.DefinitionRefreshInterval, err = duration("ALERTS_DEFINITION_REFRESH_INTERVAL", 250*time.Millisecond); err != nil {
		return Config{}, err
	}
	if c.DefinitionActivationDelay, err = duration("ALERTS_DEFINITION_ACTIVATION_DELAY", 2*time.Second); err != nil {
		return Config{}, err
	}
	if c.DefinitionMaxStaleness, err = duration("ALERTS_DEFINITION_MAX_STALENESS", time.Second); err != nil {
		return Config{}, err
	}
	if c.DefinitionActivationDelay < 2*c.DefinitionRefreshInterval || c.DefinitionMaxStaleness >= c.DefinitionActivationDelay {
		return Config{}, fmt.Errorf("definition activation delay must be at least twice the refresh interval and greater than max staleness")
	}
	if c.MaintenanceInterval, err = duration("ALERTS_MAINTENANCE_INTERVAL", time.Hour); err != nil {
		return Config{}, err
	}
	if c.ProvisionalInboxRetention, err = duration("ALERTS_PROVISIONAL_INBOX_RETENTION", 48*time.Hour); err != nil {
		return Config{}, err
	}
	if c.ConfirmedInboxRetention, err = duration("ALERTS_CONFIRMED_INBOX_RETENTION", 30*24*time.Hour); err != nil {
		return Config{}, err
	}
	if c.AlertEventRetention, err = duration("ALERTS_EVENT_RETENTION", 365*24*time.Hour); err != nil {
		return Config{}, err
	}
	if c.MaintenanceBatchSize, err = integer("ALERTS_MAINTENANCE_BATCH_SIZE", 5000, 1, 100000); err != nil {
		return Config{}, err
	}
	c.InstanceID = instanceID(env("ALERTS_INSTANCE_ID", "alertsd"))
	if c.PartitionLeaseTTL, err = duration("ALERTS_PARTITION_LEASE_TTL", 15*time.Second); err != nil {
		return Config{}, err
	}
	if c.PartitionLeaseRenewInterval, err = duration("ALERTS_PARTITION_LEASE_RENEW_INTERVAL", 5*time.Second); err != nil {
		return Config{}, err
	}
	if c.PartitionLeaseRenewInterval*2 >= c.PartitionLeaseTTL {
		return Config{}, fmt.Errorf("partition lease renew interval must be less than half the lease TTL")
	}
	if c.AllowMemory && c.WorkerCount > 1 {
		return Config{}, fmt.Errorf("memory store cannot coordinate multiple evaluator workers")
	}
	if !c.Insecure && (len(c.APIToken) < 32 || len(c.IngestToken) < 32) {
		return Config{}, fmt.Errorf("ALERTS_API_TOKEN and ALERTS_INGEST_TOKEN must each contain at least 32 characters")
	}
	if c.DatabaseURL == "" && !c.AllowMemory {
		return Config{}, fmt.Errorf("ALERTS_DATABASE_URL is required; set ALERTS_ALLOW_MEMORY_STORE=true only for development")
	}
	if c.NATSURL != "" && len(c.NATSToken) < 32 {
		return Config{}, fmt.Errorf("ALERTS_NATS_TOKEN must contain at least 32 characters when ALERTS_NATS_URL is set")
	}
	return c, nil
}

func instanceID(prefix string) string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s/%s/%d", prefix, hostname, os.Getpid())
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func integer(key string, fallback, minimum, maximum int) (int, error) {
	value, err := strconv.Atoi(env(key, strconv.Itoa(fallback)))
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be an integer from %d to %d", key, minimum, maximum)
	}
	return value, nil
}

func duration(key string, fallback time.Duration) (time.Duration, error) {
	value, err := time.ParseDuration(env(key, fallback.String()))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return value, nil
}
