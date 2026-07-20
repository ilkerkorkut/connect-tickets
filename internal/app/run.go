package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
)

type modeRunner func(ctx context.Context, res *Resources, cfg config.Config) error

func runMode(ctx context.Context, mode Mode, run modeRunner) error {
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
		res.Logger.Info("starting mode", "mode", string(mode))
	}

	return run(ctx, res, cfg)
}

func Run(ctx context.Context, mode Mode) error {
	switch mode {
	case ModeAPI:
		return RunAPI(ctx)
	case ModeWorker:
		return RunWorker(ctx)
	case ModePublisher:
		return RunPublisher(ctx)
	case ModeAll:
		return RunAll(ctx)
	case ModeMigrate:
		return RunMigrate(ctx)
	case ModeHealthcheck:
		return RunHealthcheck(ctx)
	default:
		return ErrUnknownMode
	}
}

var ErrNotImplemented = errors.New("runtime mode not implemented in this phase")
