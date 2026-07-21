package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"github.com/ilkerkorkut/connect-tickets/config"
	githubcache "github.com/ilkerkorkut/connect-tickets/internal/cache/github"
	githubclient "github.com/ilkerkorkut/connect-tickets/internal/client/github"
	"github.com/ilkerkorkut/connect-tickets/internal/event/kafka"
	"github.com/ilkerkorkut/connect-tickets/internal/producer/outbox"
	"github.com/ilkerkorkut/connect-tickets/internal/route"
	"github.com/ilkerkorkut/connect-tickets/internal/service/batch"
	"github.com/ilkerkorkut/connect-tickets/internal/service/changefeed"
	"github.com/ilkerkorkut/connect-tickets/internal/service/repository"
	"github.com/ilkerkorkut/connect-tickets/internal/service/worker"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

var (
	newLoggerFunc = func(cfg config.LogConfig) (*slog.Logger, error) {
		return logging.NewLogger(cfg, defaultLogSink)
	}

	kafkaNewConfig = func(cfg config.KafkaConfig) (*sarama.Config, error) {
		return kafka.NewSaramaConfig(cfg)
	}

	defaultLogSink io.Writer = os.Stdout

	runAPIFn = func(ctx context.Context, res *Resources, cfg config.Config) error {
		client, err := githubclient.NewClient(cfg.GitHub, nil)
		if err != nil {
			return err
		}
		gateway, err := githubcache.NewGateway(client, res.Redis, cfg.Cache, cfg.GitHub.Timeout)
		if err != nil {
			return err
		}
		readiness := newReadiness(res, cfg)
		router := route.NewRouter(route.Dependencies{
			Repositories: repository.NewService(res.Ent, gateway),
			Batches:      batch.NewService(res.Ent),
			Changes:      changefeed.NewService(changefeed.NewStore(res.Ent)),
			Health:       readiness,
		}, cfg.HTTP, res.Logger)
		server := route.NewServer(router, cfg.HTTP)
		return route.Serve(ctx, server, readiness, cfg.App.ShutdownTimeout, res.Logger)
	}
	runWorkerFn = func(ctx context.Context, res *Resources, cfg config.Config) error {
		client, err := githubclient.NewClient(cfg.GitHub, nil)
		if err != nil {
			return err
		}
		gateway, err := githubcache.NewGateway(client, res.Redis, cfg.Cache, cfg.GitHub.Timeout)
		if err != nil {
			return err
		}
		workerID := uuid.NewString()
		processor, err := worker.NewConsumer(worker.NewStore(res.Ent), gateway, workerID, cfg.Worker.LeaseDuration, cfg.Worker.MaxAttempts)
		if err != nil {
			return err
		}
		consumer, err := kafka.NewConsumer(cfg.Kafka, res.Kafka)
		if err != nil {
			return err
		}
		consumer.WithLogger(res.Logger)
		workerCtx := logging.WithLogger(logging.WithWorkerID(logging.WithConsumerGroup(ctx, kafka.ConsumerGroup), workerID), res.Logger)
		logging.Event(workerCtx, "worker started", "worker_id", workerID, "consumer_group", kafka.ConsumerGroup, "lease", cfg.Worker.LeaseDuration, "max_attempts", cfg.Worker.MaxAttempts)
		go forwardConsumerErrors(workerCtx, consumer, res.Logger)

		runErr := consumer.Consume(workerCtx, processor, nil)
		logging.Event(workerCtx, "worker stopped", "worker_id", workerID, "err", runErr)
		closeCtx, cancel := context.WithTimeout(context.Background(), cfg.Worker.ShutdownTimeout)
		closeErr := consumer.Close(closeCtx)
		cancel()
		if closeErr != nil {
			return closeErr
		}
		if errors.Is(runErr, context.Canceled) && ctx.Err() != nil {
			return ctx.Err()
		}
		return runErr
	}
	runPublisherFn = func(ctx context.Context, res *Resources, cfg config.Config) error {
		producer, err := kafka.NewProducer(cfg.Kafka, res.Kafka)
		if err != nil {
			return err
		}
		publisher, err := outbox.NewPublisher(outbox.NewStore(res.Ent), producer, cfg.Outbox)
		if err != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), cfg.Outbox.ShutdownTimeout)
			defer cancel()
			_ = producer.Close(closeCtx)
			return err
		}
		runErr := publisher.Run(logging.WithLogger(ctx, res.Logger))
		closeCtx, cancel := context.WithTimeout(context.Background(), cfg.Outbox.ShutdownTimeout)
		closeErr := producer.Close(closeCtx)
		cancel()
		if closeErr != nil {
			return closeErr
		}
		return runErr
	}
)

func newLogger(cfg config.LogConfig) (*slog.Logger, error) {
	return newLoggerFunc(cfg)
}

func forwardConsumerErrors(ctx context.Context, consumer interface{ Errors() <-chan error }, logger *slog.Logger) {
	if logger == nil {
		return
	}
	for err := range consumer.Errors() {
		logging.ScopedLogger(ctx, logger).Warn("kafka consumer group error", "consumer_group", kafka.ConsumerGroup, "err", err)
	}
}
