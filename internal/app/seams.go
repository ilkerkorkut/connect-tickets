package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"github.com/ilkerkorkut/connect-tickets/internal/batch"
	"github.com/ilkerkorkut/connect-tickets/internal/changefeed"
	githubgateway "github.com/ilkerkorkut/connect-tickets/internal/github"
	"github.com/ilkerkorkut/connect-tickets/internal/httpapi"
	"github.com/ilkerkorkut/connect-tickets/internal/outbox"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/kafka"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/logging"
	"github.com/ilkerkorkut/connect-tickets/internal/repository"
	"github.com/ilkerkorkut/connect-tickets/internal/worker"
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
		client, err := githubgateway.NewClient(cfg.GitHub, nil)
		if err != nil {
			return err
		}
		gateway, err := githubgateway.NewGateway(client, res.Redis, cfg.Cache, cfg.GitHub.Timeout)
		if err != nil {
			return err
		}
		readiness := newReadiness(res, cfg)
		router := httpapi.NewRouter(httpapi.Dependencies{
			Repositories: repository.NewService(res.Ent, gateway),
			Batches:      batch.NewService(res.Ent),
			Changes:      changefeed.NewService(changefeed.NewStore(res.Ent)),
			Health:       readiness,
		}, cfg.HTTP, res.Logger)
		server := httpapi.NewServer(router, cfg.HTTP)
		return httpapi.Serve(ctx, server, readiness, cfg.App.ShutdownTimeout)
	}
	runWorkerFn = func(ctx context.Context, res *Resources, cfg config.Config) error {
		client, err := githubgateway.NewClient(cfg.GitHub, nil)
		if err != nil {
			return err
		}
		gateway, err := githubgateway.NewGateway(client, res.Redis, cfg.Cache, cfg.GitHub.Timeout)
		if err != nil {
			return err
		}
		processor, err := worker.NewConsumer(worker.NewStore(res.DB), gateway, uuid.NewString(), cfg.Worker.LeaseDuration, cfg.Worker.MaxAttempts)
		if err != nil {
			return err
		}
		consumer, err := kafka.NewConsumer(cfg.Kafka, res.Kafka)
		if err != nil {
			return err
		}
		runErr := consumer.Consume(logging.WithLogger(ctx, res.Logger), processor, nil)
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
		publisher, err := outbox.NewPublisher(outbox.NewStore(res.DB), producer, cfg.Outbox)
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
