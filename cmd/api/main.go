package main

import (
	"context"
	"fmt"
	"os"

	app "github.com/ilkerkorkut/connect-tickets/internal/bootstrap"
	"github.com/ilkerkorkut/connect-tickets/pkg/signal"
)

func main() {
	rootCtx, stop := signal.RootContext(context.Background())
	defer stop()
	if err := app.RunAPI(rootCtx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
