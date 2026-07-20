package app

import "context"

func RunWorker(ctx context.Context) error {
	return runMode(ctx, ModeWorker, runWorkerFn)
}
