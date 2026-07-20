package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/ent"
	redisclient "github.com/ilkerkorkut/connect-tickets/internal/cache/redis"
	"github.com/ilkerkorkut/connect-tickets/pkg/postgresql"
)

type Closer = io.Closer

type namedCloser struct {
	name   string
	closer Closer
}

type Resources struct {
	logger  *slog.Logger
	closers []namedCloser

	DB     *database.Pool
	Ent    *ent.Client
	Redis  *redisclient.Client
	Kafka  *sarama.Config
	Logger *slog.Logger
}

func (r *Resources) add(name string, c Closer) {
	r.closers = append(r.closers, namedCloser{name: name, closer: c})
}

func (r *Resources) Close() error {
	var errs []error
	for i := len(r.closers) - 1; i >= 0; i-- {
		nc := r.closers[i]
		if nc.closer == nil {
			continue
		}
		if err := nc.closer.Close(); err != nil {
			if r.logger != nil {
				r.logger.Error("resource close failed", "component", nc.name, "err", err)
			}
			errs = append(errs, err)
		}
	}
	r.closers = nil
	return errors.Join(errs...)
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

func bootstrap(ctx context.Context, cfg config.Config) (*Resources, error) {
	res := &Resources{}

	logger, err := buildLogger(cfg, res)
	if err != nil {
		return nil, finalizeBootstrap(res, err)
	}
	res.logger = logger
	res.Logger = logger

	if err := buildDatabase(ctx, cfg, res); err != nil {
		return nil, finalizeBootstrap(res, err)
	}
	if err := buildEnt(res); err != nil {
		return nil, finalizeBootstrap(res, err)
	}
	if err := buildRedis(cfg, res); err != nil {
		return nil, finalizeBootstrap(res, err)
	}
	if err := buildKafka(cfg, res); err != nil {
		return nil, finalizeBootstrap(res, err)
	}

	logger.Info("bootstrap complete", "mode", cfg.App.Mode, "env", cfg.App.Environment)
	return res, nil
}

func buildLogger(cfg config.Config, res *Resources) (*slog.Logger, error) {
	logger, err := newLogger(cfg.Log)
	if err != nil {
		return nil, err
	}
	res.add("logger", closerFunc(func() error { return nil }))
	return logger, nil
}

func buildDatabase(ctx context.Context, cfg config.Config, res *Resources) error {
	pool, err := database.NewPool(ctx, cfg.Database)
	if err != nil {
		return err
	}
	res.DB = pool
	res.add("database", closerFunc(func() error { pool.Close(); return nil }))
	return nil
}

func buildEnt(res *Resources) error {
	if res == nil || res.DB == nil {
		return errors.New("database is required for ent")
	}
	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, stdlib.OpenDBFromPool(res.DB))))
	res.Ent = client
	res.add("ent", client)
	return nil
}

func buildRedis(cfg config.Config, res *Resources) error {
	client, err := redisclient.NewClient(cfg.Redis)
	if err != nil {
		return err
	}
	res.Redis = client
	res.add("redis", client)
	return nil
}

func buildKafka(cfg config.Config, res *Resources) error {
	scfg, err := kafkaNewConfig(cfg.Kafka)
	if err != nil {
		return err
	}
	res.Kafka = scfg
	res.add("kafka-config", closerFunc(func() error { return nil }))
	return nil
}

func finalizeBootstrap(res *Resources, cause error) error {
	if res == nil {
		return cause
	}
	if closeErr := res.Close(); closeErr != nil {
		return errors.Join(cause, closeErr)
	}
	return cause
}
