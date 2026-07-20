package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"golang.org/x/sync/errgroup"

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
)

func RunAll(ctx context.Context) error {
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

	return runAllWith(ctx, res, cfg)
}

func runAllWith(ctx context.Context, res *Resources, cfg config.Config) error {
	logger := res.Logger
	if logger != nil {
		logger.Info("starting mode", "mode", string(ModeAll))
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return runAPIFn(gctx, res, cfg) })
	g.Go(func() error { return runWorkerFn(gctx, res, cfg) })
	g.Go(func() error { return runPublisherFn(gctx, res, cfg) })

	err := g.Wait()
	if err == nil || isContextShutdown(err) {
		if logger != nil {
			logger.Info("all modes completed", "mode", string(ModeAll))
		}
		return nil
	}
	return err
}

func isContextShutdown(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func newNoopResources() *Resources {
	return &Resources{Logger: slog.New(slog.NewTextHandler(devNull{}, &slog.HandlerOptions{}))}
}

type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }
