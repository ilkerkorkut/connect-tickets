package client

import (
	"context"
	"time"
)

type RateLimit struct {
	Status     int
	RetryAfter time.Duration
	Remaining  *int
	ResetAt    *time.Time
}

type Repository struct {
	Owner       string    `json:"owner"`
	Name        string    `json:"name"`
	FullName    string    `json:"full_name"`
	Description *string   `json:"description"`
	Stars       int       `json:"stars"`
	Language    *string   `json:"language"`
	HTMLURL     string    `json:"html_url"`
	FetchedAt   time.Time `json:"fetched_at"`
}

type GitHub interface {
	FetchRepository(context.Context, string, string) (Repository, error)
	RefreshRepository(context.Context, string, string) (Repository, error)
	Cooldown(context.Context) (time.Time, error)
}
