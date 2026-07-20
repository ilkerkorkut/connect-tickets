package app

import "context"

func RunPublisher(ctx context.Context) error {
	return runMode(ctx, ModePublisher, runPublisherFn)
}
