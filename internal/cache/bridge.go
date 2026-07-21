package cache

import (
	"context"
	"errors"

	"github.com/ilkerkorkut/connect-tickets/internal/client"
)

type Repository = client.Repository
type RateLimit = client.RateLimit
type RequestError = client.RequestError

var (
	ErrNotFound                     = client.ErrNotFound
	ErrUnauthorized                 = client.ErrUnauthorized
	ErrRateLimited                  = client.ErrRateLimited
	ErrServer                       = client.ErrServer
	ErrTimeout                      = client.ErrTimeout
	ErrNetwork                      = client.ErrNetwork
	ErrInvalidResponse              = client.ErrInvalidResponse
	ErrCanceled                     = client.ErrCanceled
	ErrInvalidRequest               = client.ErrInvalidRequest
	ErrInvalidConfig                = client.ErrInvalidConfig
	ErrCacheCoordinationUnavailable = client.ErrCacheCoordinationUnavailable
)

func requestError(
	kind error,
	status int,
	rateLimit RateLimit,
	diagnostic string,
	cause error,
) error {
	return client.NewRequestError(kind, status, rateLimit, diagnostic, cause)
}

func cancellationError(cause error) error {
	if errors.Is(cause, context.DeadlineExceeded) {
		return requestError(ErrTimeout, 0, RateLimit{}, "request deadline exceeded", cause)
	}

	return requestError(ErrCanceled, 0, RateLimit{}, "request canceled", cause)
}
