package middleware_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/internal/middleware"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

func newRecoveryLogger(t *testing.T) (*bytes.Buffer, *gin.Engine) {
	t.Helper()
	buf := &bytes.Buffer{}
	logger, err := logging.NewLogger(config.LogConfig{Level: "debug", Format: "json"}, buf)
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(logging.WithLogger(c.Request.Context(), logger))
		c.Next()
	}, middleware.Recovery(func(c *gin.Context) {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": gin.H{"code": "internal_error"}})
	}))
	return buf, engine
}

func TestRecoveryLogsPanicWithStack(t *testing.T) {
	gin.SetMode(gin.TestMode)
	buf, engine := newRecoveryLogger(t)
	engine.GET("/boom", func(c *gin.Context) { panic("secret stack value") })

	request := httptest.NewRequest(http.MethodGet, "/boom", nil)
	request = request.WithContext(logging.WithRequestID(request.Context(), "req-panic"))
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", response.Code)
	}
	out := buf.String()
	if !strings.Contains(out, "panic recovered") {
		t.Fatalf("missing panic log: %s", out)
	}
	if !strings.Contains(out, "goroutine") || !strings.Contains(out, "stack") {
		t.Fatalf("missing stack trace: %s", out)
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &record); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if record["request_id"] != "req-panic" {
		t.Errorf("request_id = %v, want req-panic", record["request_id"])
	}
	if record["panic"] != "secret stack value" {
		t.Errorf("panic value = %v", record["panic"])
	}
}

func TestRecoveryRendersSafeResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	_, engine := newRecoveryLogger(t)
	engine.GET("/boom", func(c *gin.Context) { panic("internal") })

	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", response.Code)
	}
	if !strings.Contains(response.Body.String(), "internal_error") {
		t.Fatalf("safe response missing: %s", response.Body.String())
	}
}
