package app

import (
	"context"
	"fmt"

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
	"github.com/ilkerkorkut/connect-tickets/migrations"
)

func RunMigrate(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	res, err := bootstrap(ctx, cfg)
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	defer func() { _ = res.Close() }()

	if res.Logger != nil {
		res.Logger.Info("starting mode", "mode", string(ModeMigrate))
	}
	if err := migrations.Apply(ctx, res.DB); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	if res.Logger != nil {
		res.Logger.Info("migrations complete")
	}
	return nil
}
