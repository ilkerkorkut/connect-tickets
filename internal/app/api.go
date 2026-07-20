package app

import "context"

func RunAPI(ctx context.Context) error {
	return runMode(ctx, ModeAPI, runAPIFn)
}
