package client

import (
	"context"
	"errors"
	"fmt"
	"time"
)

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

type RateLimit struct {
	Status     int
	RetryAfter time.Duration
	Remaining  *int
	ResetAt    *time.Time
}

var (
	ErrNotFound                     = errors.New("github repository not found")
	ErrUnauthorized                 = errors.New("github request unauthorized")
	ErrRateLimited                  = errors.New("github rate limited")
	ErrServer                       = errors.New("github server error")
	ErrTimeout                      = errors.New("github request timed out")
	ErrNetwork                      = errors.New("github network failure")
	ErrInvalidResponse              = errors.New("github invalid response")
	ErrCanceled                     = errors.New("github request canceled")
	ErrInvalidRequest               = errors.New("invalid github request")
	ErrInvalidConfig                = errors.New("invalid github client configuration")
	ErrCacheCoordinationUnavailable = errors.New("cache coordination unavailable")
)

type RequestError struct {
	Kind       error
	Status     int
	RateLimit  RateLimit
	Diagnostic string
	Cause      error
}

func (e *RequestError) Error() string {
	if e == nil || e.Kind == nil {
		return "github request failed"
	}
	if e.Status > 0 {
		return fmt.Sprintf("github request failed: %s (status %d)", e.Kind, e.Status)
	}
	return fmt.Sprintf("github request failed: %s", e.Kind)
}

func (e *RequestError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *RequestError) Is(target error) bool {
	return e != nil && e.Kind == target
}

func NewRequestError(kind error, status int, rateLimit RateLimit, diagnostic string, cause error) error {
	return &RequestError{Kind: kind, Status: status, RateLimit: rateLimit, Diagnostic: diagnostic, Cause: cause}
}
