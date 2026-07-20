package app

import (
	"context"
	"errors"

	"github.com/ilkerkorkut/connect-tickets/internal/health"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/kafka"
)

func newReadiness(res *Resources, cfg config.Config) *health.Service {
	return health.New(
		health.Check{Name: "postgres", Timeout: cfg.Database.QueryTimeout, Run: func(ctx context.Context) error {
			if res == nil || res.DB == nil {
				return errors.New("postgres unavailable")
			}
			return res.DB.Ping(ctx)
		}},
		health.Check{Name: "redis", Timeout: cfg.Redis.ReadTimeout, Run: func(ctx context.Context) error {
			if res == nil || res.Redis == nil {
				return errors.New("redis unavailable")
			}
			return res.Redis.Ping(ctx).Err()
		}},
		health.Check{Name: "kafka", Timeout: cfg.Kafka.RequestTimeout, Run: func(ctx context.Context) error {
			if res == nil || res.Kafka == nil {
				return errors.New("kafka unavailable")
			}
			admin, err := kafka.NewTopicAdmin(cfg.Kafka, res.Kafka)
			if err != nil {
				return err
			}
			validateErr := kafka.ValidateMainTopic(ctx, admin)
			closeErr := admin.Close()
			return errors.Join(validateErr, closeErr)
		}},
	)
}
