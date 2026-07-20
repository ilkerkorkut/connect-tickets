package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ilkerkorkut/connect-tickets/internal/app"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/signal"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	mode, err := app.ParseMode(args)
	if err != nil {
		return err
	}

	rootCtx, stop := signal.RootContext(ctx)
	defer stop()

	return app.Run(rootCtx, mode)
}
