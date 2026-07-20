package bootstrap

import (
	"context"
	"fmt"

	"github.com/ilkerkorkut/connect-tickets/config"
)

type runner func(ctx context.Context, res *Resources, cfg config.Config) error

func run(ctx context.Context, name string, start runner) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	res, err := bootstrap(ctx, cfg)
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	defer func() {
		_ = res.Close()
	}()

	if res.Logger != nil {
		res.Logger.Info("starting", "component", name)
	}

	return start(ctx, res, cfg)
}
