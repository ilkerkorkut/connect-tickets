package handlers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ilkerkorkut/connect-tickets/internal/client"
	"github.com/ilkerkorkut/connect-tickets/internal/middleware"
	"github.com/ilkerkorkut/connect-tickets/internal/service/batch"
	"github.com/ilkerkorkut/connect-tickets/internal/service/changefeed"
	"github.com/ilkerkorkut/connect-tickets/internal/service/repository"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

type Detail struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

type errorBody struct {
	Code      string   `json:"code"`
	Message   string   `json:"message"`
	RequestID string   `json:"request_id"`
	Details   []Detail `json:"details,omitempty"`
}

func WriteError(c *gin.Context, status int, code, message string, details ...Detail) {
	middleware.SetErrorCode(c, code)
	c.AbortWithStatusJSON(status, gin.H{"error": errorBody{Code: code, Message: message, RequestID: logging.RequestIDFrom(c.Request.Context()), Details: details}})
}

func WriteServiceError(c *gin.Context, err error) {
	classification, message := classify(err)
	args := []any{
		"error_code", classification.code,
		"http_status", classification.status,
		"error_kind", classification.kind,
		"github_status", classification.githubStatus,
		"github_retry_after_ms", classification.retryAfterMs,
		"github_reset_at", classification.resetAt,
	}

	if classification.kind == "internal" {
		args = append(args, "error_type", fmt.Sprintf("%T", err))
	}

	logging.Warn(c.Request.Context(), "request failed", args...)

	if classification.retryAfterMs > 0 {
		seconds := math.Ceil(float64(classification.retryAfterMs) / float64(time.Second.Milliseconds()))
		if seconds > 0 && seconds <= float64(math.MaxInt) {
			c.Header("Retry-After", strconv.Itoa(int(seconds)))
		}
	}

	WriteError(c, classification.status, classification.code, message)
}

type errorClass struct {
	status       int
	code         string
	kind         string
	githubStatus int
	retryAfterMs int64
	resetAt      string
}

func classify(err error) (errorClass, string) {
	if errors.Is(err, repository.ErrInvalidCursor) || errors.Is(err, changefeed.ErrInvalidCursor) {
		return errorClass{status: http.StatusBadRequest, code: "invalid_cursor", kind: "invalid_cursor"}, "The cursor is invalid."
	}
	if errors.Is(err, repository.ErrInvalidInput) {
		return errorClass{status: http.StatusBadRequest, code: "validation_failed", kind: "invalid_input"}, "The request is invalid."
	}
	if errors.Is(err, repository.ErrNotFound) {
		return errorClass{status: http.StatusNotFound, code: "tracked_repo_not_found", kind: "not_found"}, "The tracked repository was not found."
	}
	if errors.Is(err, batch.ErrNotFound) {
		return errorClass{status: http.StatusNotFound, code: "batch_not_found", kind: "not_found"}, "The batch was not found."
	}
	if errors.Is(err, repository.ErrDuplicate) {
		return errorClass{status: http.StatusConflict, code: "repo_already_tracked", kind: "duplicate"}, "The repository is already being tracked."
	}
	if errors.Is(err, client.ErrNotFound) {
		return errorClass{status: http.StatusNotFound, code: "github_repo_not_found", kind: "github_not_found"}, "The repository was not found on GitHub."
	}
	if errors.Is(err, client.ErrRateLimited) {
		class := githubClass(err, http.StatusServiceUnavailable, "github_rate_limited", "github_rate_limited")
		return class, "GitHub is temporarily rate limited."
	}
	if errors.Is(err, client.ErrTimeout) || errors.Is(err, client.ErrCanceled) {
		return errorClass{status: http.StatusGatewayTimeout, code: "github_timeout", kind: "github_timeout"}, "GitHub did not respond in time."
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errorClass{status: http.StatusGatewayTimeout, code: "request_timeout", kind: "context_deadline"}, "The request did not complete in time."
	}
	if errors.Is(err, client.ErrCacheCoordinationUnavailable) {
		return errorClass{status: http.StatusServiceUnavailable, code: "cache_unavailable", kind: "cache_unavailable"}, "The cache service is temporarily unavailable."
	}
	if errors.Is(err, client.ErrUnauthorized) || errors.Is(err, client.ErrServer) || errors.Is(err, client.ErrNetwork) || errors.Is(err, client.ErrInvalidResponse) {
		class := githubClass(err, http.StatusBadGateway, "github_unavailable", "github_unavailable")
		return class, "GitHub is temporarily unavailable."
	}
	return errorClass{status: http.StatusInternalServerError, code: "internal_error", kind: "internal", githubStatus: githubStatusFrom(err)}, "An unexpected error occurred."
}

func githubClass(err error, status int, code, kind string) errorClass {
	class := errorClass{status: status, code: code, kind: kind}
	var requestError *client.RequestError
	if errors.As(err, &requestError) {
		class.githubStatus = requestError.Status
		if requestError.RateLimit.RetryAfter > 0 {
			class.retryAfterMs = requestError.RateLimit.RetryAfter.Milliseconds()
		}
		if requestError.RateLimit.ResetAt != nil {
			class.resetAt = requestError.RateLimit.ResetAt.UTC().Format("2006-01-02T15:04:05Z07:00")
		}
	}
	return class
}

func githubStatusFrom(err error) int {
	var requestError *client.RequestError
	if errors.As(err, &requestError) {
		return requestError.Status
	}
	return 0
}
