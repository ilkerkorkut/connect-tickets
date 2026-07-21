package client

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound                     = errors.New("provider repository not found")
	ErrUnauthorized                 = errors.New("provider request unauthorized")
	ErrRateLimited                  = errors.New("provider rate limited")
	ErrServer                       = errors.New("provider server error")
	ErrTimeout                      = errors.New("provider request timed out")
	ErrNetwork                      = errors.New("provider network failure")
	ErrInvalidResponse              = errors.New("provider invalid response")
	ErrCanceled                     = errors.New("provider request canceled")
	ErrInvalidRequest               = errors.New("invalid provider request")
	ErrInvalidConfig                = errors.New("invalid provider client configuration")
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
		return "provider request failed"
	}
	if e.Status > 0 {
		return fmt.Sprintf("provider request failed: %s (status %d)", e.Kind, e.Status)
	}
	return fmt.Sprintf("provider request failed: %s", e.Kind)
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
