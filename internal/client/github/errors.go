package github

import "github.com/ilkerkorkut/connect-tickets/internal/client"

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

func NewRequestError(kind error, status int, rateLimit client.RateLimit, diagnostic string, cause error) error {
	return client.NewRequestError(kind, status, rateLimit, diagnostic, cause)
}
