package handlers

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/ilkerkorkut/connect-tickets/internal/batch"
	"github.com/ilkerkorkut/connect-tickets/internal/changefeed"
	githubgateway "github.com/ilkerkorkut/connect-tickets/internal/github"
	"github.com/ilkerkorkut/connect-tickets/internal/httpapi/middleware"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/logging"
	"github.com/ilkerkorkut/connect-tickets/internal/repository"
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
	if errors.Is(err, githubgateway.ErrNotFound) {
		WriteError(c, http.StatusNotFound, "github_repo_not_found", "The repository was not found on GitHub.")
		return
	}
	if errors.Is(err, githubgateway.ErrRateLimited) {
		writeGitHubError(c, http.StatusServiceUnavailable, "github_rate_limited", "GitHub is temporarily rate limited.", err)
		return
	}
	if errors.Is(err, githubgateway.ErrTimeout) || errors.Is(err, githubgateway.ErrCanceled) {
		WriteError(c, http.StatusGatewayTimeout, "github_timeout", "GitHub did not respond in time.")
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		WriteError(c, http.StatusGatewayTimeout, "request_timeout", "The request did not complete in time.")
		return
	}
	if errors.Is(err, githubgateway.ErrCacheCoordinationUnavailable) {
		WriteError(c, http.StatusServiceUnavailable, "cache_unavailable", "The cache service is temporarily unavailable.")
		return
	}
	if errors.Is(err, githubgateway.ErrUnauthorized) || errors.Is(err, githubgateway.ErrServer) || errors.Is(err, githubgateway.ErrNetwork) || errors.Is(err, githubgateway.ErrInvalidResponse) {
		writeGitHubError(c, http.StatusBadGateway, "github_unavailable", "GitHub is temporarily unavailable.", err)
		return
	}
	WriteError(c, http.StatusInternalServerError, "internal_error", "An unexpected error occurred.")
}

func writeGitHubError(c *gin.Context, status int, code, message string, err error) {
	var requestError *githubgateway.RequestError
	if errors.As(err, &requestError) && requestError.RateLimit.RetryAfter > 0 {
		seconds := math.Ceil(requestError.RateLimit.RetryAfter.Seconds())
		if seconds > 0 && seconds <= float64(math.MaxInt) {
			c.Header("Retry-After", strconv.Itoa(int(seconds)))
		}
	}
	WriteError(c, status, code, message)
}
