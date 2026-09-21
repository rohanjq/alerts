package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rohanjq/alerts/internal/api"
	"github.com/rohanjq/alerts/internal/broker"
	"github.com/rohanjq/alerts/internal/config"
	"github.com/rohanjq/alerts/internal/service"
	"github.com/rohanjq/alerts/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("alertsd stopped", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var repo store.Repository
	if cfg.DatabaseURL != "" {
		repo, err = store.OpenPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("open alert store: %w", err)
		}
	} else {
		repo = store.NewMemory()
		logger.Warn("using volatile memory store")
	}
	defer repo.Close()
	ownedPartitions := make([]int, 0, 256/cfg.WorkerCount+1)
	for partition := 0; partition < 256; partition++ {
		if partition%cfg.WorkerCount == cfg.WorkerIndex {
			ownedPartitions = append(ownedPartitions, partition)
		}
	}
	acquired, err := repo.AcquirePartitions(ctx, cfg.InstanceID, ownedPartitions, cfg.PartitionLeaseTTL)
	if err != nil {
		return fmt.Errorf("acquire evaluator partition leases: %w", err)
	}
	if !acquired {
		return fmt.Errorf("one or more evaluator partitions are already owned")
	}
	defer repo.ReleasePartitions(context.Background(), cfg.InstanceID)
	var jetstream *broker.JetStream
	publishers := service.MultiPublisher{}
	readiness := []service.Readiness{}
	if cfg.NATSURL != "" {
		jetstream, err = broker.Open(ctx, broker.Options{URL: cfg.NATSURL, Token: cfg.NATSToken, Durable: cfg.NATSDurable, Replicas: cfg.NATSReplicas, MaxDeliveries: cfg.NATSMaxDeliveries, AckWait: cfg.NATSAckWait, WorkerIndex: cfg.WorkerIndex, WorkerCount: cfg.WorkerCount, BatchSize: cfg.EvaluationBatchSize, Logger: logger})
		if err != nil {
			return fmt.Errorf("open JetStream: %w", err)
		}
		defer jetstream.Close()
		publishers = append(publishers, jetstream)
		readiness = append(readiness, jetstream.Ready)
		logger.Info("JetStream fact consumer and alert publisher enabled", "durable", cfg.NATSDurable)
	}
	publishers = append(publishers, service.LogPublisher{Logger: logger})
	svc := service.NewWithOptions(repo, publishers, logger, service.Options{ActivationDelay: cfg.DefinitionActivationDelay, DefinitionMaxStaleness: cfg.DefinitionMaxStaleness, WorkerIndex: cfg.WorkerIndex, WorkerCount: cfg.WorkerCount}, readiness...)
	if err = svc.Initialize(ctx); err != nil {
		return fmt.Errorf("initialize compiled alert runtime: %w", err)
	}
	go svc.RunPublisher(ctx)
	go svc.RunDefinitionRefresh(ctx, cfg.DefinitionRefreshInterval)
	go svc.RunMaintenance(ctx, cfg.MaintenanceInterval, store.RetentionPolicy{ProvisionalInbox: cfg.ProvisionalInboxRetention, ConfirmedInbox: cfg.ConfirmedInboxRetention, AlertEvents: cfg.AlertEventRetention, BatchSize: cfg.MaintenanceBatchSize})
	server, err := api.New(api.Options{Addr: cfg.Addr, APIToken: cfg.APIToken, IngestToken: cfg.IngestToken, Insecure: cfg.Insecure}, svc)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}
	done := make(chan error, 1)
	go func() { logger.Info("alerts API listening", "addr", cfg.Addr); done <- server.Run() }()
	consumerDone := make(chan error, 1)
	leaseDone := make(chan error, 1)
	go renewLeases(ctx, repo, cfg.InstanceID, ownedPartitions, cfg.PartitionLeaseTTL, cfg.PartitionLeaseRenewInterval, leaseDone)
	if jetstream != nil {
		go func() { consumerDone <- jetstream.Consume(ctx, svc) }()
	}
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case err := <-consumerDone:
		if err != nil {
			return fmt.Errorf("JetStream consumer stopped: %w", err)
		}
	case err := <-leaseDone:
		stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return fmt.Errorf("partition lease lost: %w", err)
	}
	return nil
}

func renewLeases(ctx context.Context, repo store.Repository, owner string, partitions []int, ttl, interval time.Duration, done chan<- error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := repo.RenewPartitions(ctx, owner, partitions, ttl)
			if err != nil {
				done <- err
				return
			}
			if !ok {
				done <- errors.New("one or more evaluator partitions are no longer owned")
				return
			}
		}
	}
}
