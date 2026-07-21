package handlers_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/internal/client"
	"github.com/ilkerkorkut/connect-tickets/internal/handler"
	"github.com/ilkerkorkut/connect-tickets/internal/middleware"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

const dsnSentinel = "postgres://user:ghp_SECRET_TOKEN@db.internal:5432/prod"

func newCaptureEngine(t *testing.T) (*bytes.Buffer, *gin.Engine) {
	t.Helper()
	buf := &bytes.Buffer{}
	logger, err := logging.NewLogger(config.LogConfig{Level: "debug", Format: "json"}, buf)
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(logging.WithLogger(c.Request.Context(), logger))
		c.Next()
	}, middleware.AccessLog(logger), middleware.Recovery(func(c *gin.Context) {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": gin.H{"code": "internal_error"}})
	}))
	return buf, engine
}

func findLog(t *testing.T, buf *bytes.Buffer, message string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if record["msg"] == message {
			return record
		}
	}
	t.Fatalf("log message %q not found in: %s", message, buf.String())
	return nil
}

func TestWriteServiceErrorLogsGitHubRateLimitWithoutDiagnostic(t *testing.T) {
	buf, engine := newCaptureEngine(t)
	diagnostic := "rc di }0* secret diagnostic with token ghp_LEAK"
	resetAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	rateErr := client.NewRequestError(client.ErrRateLimited, 429, client.RateLimit{RetryAfter: 1500 * time.Millisecond, ResetAt: &resetAt}, diagnostic, errors.New("cause leak"))
	engine.POST("/x", func(c *gin.Context) { handlers.WriteServiceError(c, rateErr) })

	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req = req.WithContext(logging.WithRequestID(req.Context(), "req-rl"))
	engine.ServeHTTP(httptest.NewRecorder(), req)

	out := buf.String()
	if strings.Contains(out, diagnostic) {
		t.Fatalf("diagnostic leaked into log: %s", out)
	}
	if strings.Contains(out, "cause leak") {
		t.Fatalf("cause leaked into log: %s", out)
	}
	record := findLog(t, buf, "request failed")
	if record["error_code"] != "github_rate_limited" {
		t.Errorf("error_code = %v", record["error_code"])
	}
	if record["error_kind"] != "github_rate_limited" {
		t.Errorf("error_kind = %v", record["error_kind"])
	}
	if record["github_retry_after_ms"] != float64(1500) {
		t.Errorf("github_retry_after_ms = %v", record["github_retry_after_ms"])
	}
	if record["github_status"] != float64(429) {
		t.Errorf("github_status = %v", record["github_status"])
	}
	if record["request_id"] != "req-rl" {
		t.Errorf("request_id = %v", record["request_id"])
	}
}

func TestWriteServiceErrorInternalLogsTypeNotMessage(t *testing.T) {
	buf, engine := newCaptureEngine(t)
	engine.POST("/x", func(c *gin.Context) {
		handlers.WriteServiceError(c, errors.New(dsnSentinel))
	})
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/x", nil))

	out := buf.String()
	if strings.Contains(out, dsnSentinel) {
		t.Fatalf("raw error message leaked: %s", out)
	}
	if strings.Contains(out, "SECRET_TOKEN") {
		t.Fatalf("token leaked: %s", out)
	}
	record := findLog(t, buf, "request failed")
	if record["error_kind"] != "internal" {
		t.Errorf("error_kind = %v", record["error_kind"])
	}
	if record["error_type"] == nil || record["error_type"] == "" {
		t.Errorf("error_type missing: %v", record["error_type"])
	}
}

func TestWriteServiceErrorSetsRetryAfterHeader(t *testing.T) {
	_, engine := newCaptureEngine(t)
	engine.POST("/x", func(c *gin.Context) {
		handlers.WriteServiceError(c, client.NewRequestError(client.ErrRateLimited, 429, client.RateLimit{RetryAfter: 2500 * time.Millisecond}, "", nil))
	})
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/x", nil))
	if got := w.Header().Get("Retry-After"); got != "3" {
		t.Fatalf("Retry-After = %q, want 3", got)
	}
}
