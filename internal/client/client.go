package client

import (
	"context"
	"time"
)

type GitHub interface {
	FetchRepository(context.Context, string, string) (Repository, error)
	RefreshRepository(context.Context, string, string) (Repository, error)
	Cooldown(context.Context) (time.Time, error)
}

type APIClient interface {
	GitHub() GitHub
}
