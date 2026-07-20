package bootstrap

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"

	"github.com/ilkerkorkut/connect-tickets/config"
)

func RunWorker(ctx context.Context) error {
	return run(ctx, "worker", func(runCtx context.Context, res *Resources, cfg config.Config) error {
		group, groupCtx := errgroup.WithContext(runCtx)
		group.Go(func() error { return runWorkerFn(groupCtx, res, cfg) })
		group.Go(func() error { return runPublisherFn(groupCtx, res, cfg) })
		err := group.Wait()
		if errors.Is(err, context.Canceled) && runCtx.Err() != nil {
			return runCtx.Err()
		}
		return err
	})
}
