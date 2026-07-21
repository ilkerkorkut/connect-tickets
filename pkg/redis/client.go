package redis

import (
	"errors"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ilkerkorkut/connect-tickets/config"
)

type Client = goredis.Client

var ErrMissingRedisAddr = errors.New("redis address is required")

func NewClient(cfg config.RedisConfig) (*Client, error) {
	if cfg.Addr == "" {
		return nil, ErrMissingRedisAddr
	}

	client := goredis.NewClient(&goredis.Options{
		Addr:         cfg.Addr,
		Username:     "",
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	return client, nil
}
