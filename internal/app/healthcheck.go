package app

import (
	"context"
	"fmt"

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
)

func RunHealthcheck(ctx context.Context) error {
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
		res.Logger.Info("starting mode", "mode", string(ModeHealthcheck))
	}
	return newReadiness(res, cfg).Check(ctx)
}
