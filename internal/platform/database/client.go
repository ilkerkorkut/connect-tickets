package database

import (
	"context"
	"errors"

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidDatabaseURL = errors.New("invalid database URL")
)

type Pool = pgxpool.Pool

func NewPool(ctx context.Context, cfg config.DatabaseConfig) (*Pool, error) {
	parsed, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, ErrInvalidDatabaseURL
	}

	parsed.MaxConns = cfg.MaxOpenConns
	parsed.MinConns = cfg.MaxIdleConns
	parsed.MaxConnLifetime = cfg.ConnMaxLifetime
	parsed.MaxConnIdleTime = cfg.ConnMaxLifetime / 2
	parsed.HealthCheckPeriod = cfg.HealthCheckPeriod

	pool, err := pgxpool.NewWithConfig(ctx, parsed)
	if err != nil {
		return nil, ErrInvalidDatabaseURL
	}
	return pool, nil
}
