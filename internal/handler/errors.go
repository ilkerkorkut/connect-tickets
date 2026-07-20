package handlers

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"

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
	if errors.Is(err, repository.ErrInvalidCursor) || errors.Is(err, changefeed.ErrInvalidCursor) {
		WriteError(c, http.StatusBadRequest, "invalid_cursor", "The cursor is invalid.")
		return
	}
	if errors.Is(err, repository.ErrInvalidInput) {
		WriteError(c, http.StatusBadRequest, "validation_failed", "The request is invalid.")
		return
	}
	if errors.Is(err, repository.ErrNotFound) {
		WriteError(c, http.StatusNotFound, "tracked_repo_not_found", "The tracked repository was not found.")
		return
	}
	if errors.Is(err, batch.ErrNotFound) {
		WriteError(c, http.StatusNotFound, "batch_not_found", "The batch was not found.")
		return
	}
	if errors.Is(err, repository.ErrDuplicate) {
		WriteError(c, http.StatusConflict, "repo_already_tracked", "The repository is already being tracked.")
		return
	}
	if errors.Is(err, client.ErrNotFound) {
		WriteError(c, http.StatusNotFound, "github_repo_not_found", "The repository was not found on GitHub.")
		return
	}
	if errors.Is(err, client.ErrRateLimited) {
		writeGitHubError(c, http.StatusServiceUnavailable, "github_rate_limited", "GitHub is temporarily rate limited.", err)
		return
	}
	if errors.Is(err, client.ErrTimeout) || errors.Is(err, client.ErrCanceled) {
		WriteError(c, http.StatusGatewayTimeout, "github_timeout", "GitHub did not respond in time.")
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		WriteError(c, http.StatusGatewayTimeout, "request_timeout", "The request did not complete in time.")
		return
	}
	if errors.Is(err, client.ErrCacheCoordinationUnavailable) {
		WriteError(c, http.StatusServiceUnavailable, "cache_unavailable", "The cache service is temporarily unavailable.")
		return
	}
	if errors.Is(err, client.ErrUnauthorized) || errors.Is(err, client.ErrServer) || errors.Is(err, client.ErrNetwork) || errors.Is(err, client.ErrInvalidResponse) {
		writeGitHubError(c, http.StatusBadGateway, "github_unavailable", "GitHub is temporarily unavailable.", err)
		return
	}
	WriteError(c, http.StatusInternalServerError, "internal_error", "An unexpected error occurred.")
}

func writeGitHubError(c *gin.Context, status int, code, message string, err error) {
	var requestError *client.RequestError
	if errors.As(err, &requestError) && requestError.RateLimit.RetryAfter > 0 {
		seconds := math.Ceil(requestError.RateLimit.RetryAfter.Seconds())
		if seconds > 0 && seconds <= float64(math.MaxInt) {
			c.Header("Retry-After", strconv.Itoa(int(seconds)))
		}
	}
	WriteError(c, status, code, message)
}
