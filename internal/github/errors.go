package github

import (
	"errors"
	"fmt"
)

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

func requestError(kind error, status int, rateLimit RateLimit, diagnostic string, cause error) error {
	return &RequestError{
		Kind:       kind,
		Status:     status,
		RateLimit:  rateLimit,
		Diagnostic: diagnostic,
		Cause:      cause,
	}
}
