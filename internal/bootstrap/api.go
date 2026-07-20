package bootstrap

import "context"

func RunAPI(ctx context.Context) error {
	return run(ctx, "api", runAPIFn)
}
