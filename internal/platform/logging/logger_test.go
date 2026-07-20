package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/logging"
)

const tokenSentinel = "ghp_LEAK_ME_AND_FAIL_999"

func newCaptureLogger(t *testing.T, level, format string) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	logger, err := logging.NewLogger(config.LogConfig{Level: level, Format: format}, buf)
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	return logger, buf
}

func TestLevelParsing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		level string
		want  slog.Level
	}{
		{"", slog.LevelInfo},
		{"info", slog.LevelInfo},
		{"debug", slog.LevelDebug},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"ERROR", slog.LevelError},
	}

	for _, tc := range cases {
		_, err := logging.NewLogger(config.LogConfig{Level: tc.level, Format: "json"}, &bytes.Buffer{})
		if err != nil {
			t.Errorf("level %q: unexpected error %v", tc.level, err)
		}
	}
}

func TestUnknownLevelRejected(t *testing.T) {
	t.Parallel()
	_, err := logging.NewLogger(config.LogConfig{Level: "verbose", Format: "json"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error for unknown level")
	}
}

func TestUnknownFormatRejected(t *testing.T) {
	t.Parallel()
	_, err := logging.NewLogger(config.LogConfig{Level: "info", Format: "yaml"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error for unknown format")
	}
}

func TestJSONFormatEmitted(t *testing.T) {
	t.Parallel()
	logger, buf := newCaptureLogger(t, "info", "json")
	logger.Info("hello", "component", "test")

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("expected JSON output, got parse error: %v (raw=%q)", err, buf.String())
	}
	if record["msg"] != "hello" {
		t.Errorf("msg = %v, want hello", record["msg"])
	}
	if record["component"] != "test" {
		t.Errorf("component = %v, want test", record["component"])
	}
}

func TestSecretAttrsRedacted(t *testing.T) {
	t.Parallel()
	logger, buf := newCaptureLogger(t, "info", "json")

	logger.Info("upstream call",
		"owner", "octocat",
		"token", tokenSentinel,
		"authorization", "Bearer "+tokenSentinel,
		"password", tokenSentinel,
		"database_url", "postgres://u:"+tokenSentinel+"@host/db",
	)

	out := buf.String()
	if strings.Contains(out, tokenSentinel) {
		t.Fatalf("secret leaked into log output: %s", out)
	}

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if record["token"] != "[redacted]" {
		t.Errorf("token not redacted: %v", record["token"])
	}
	if record["password"] != "[redacted]" {
		t.Errorf("password not redacted: %v", record["password"])
	}
	if record["owner"] != "octocat" {
		t.Errorf("non-sensitive attr owner was altered: %v", record["owner"])
	}
}

func TestContextCorrelationRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx = logging.WithRequestID(ctx, "req-123")
	ctx = logging.WithJobID(ctx, "job-456")
	ctx = logging.WithBatchID(ctx, "batch-789")

	if got := logging.RequestIDFrom(ctx); got != "req-123" {
		t.Errorf("RequestIDFrom = %q, want req-123", got)
	}
	if got := logging.JobIDFrom(ctx); got != "job-456" {
		t.Errorf("JobIDFrom = %q, want job-456", got)
	}
	if got := logging.BatchIDFrom(ctx); got != "batch-789" {
		t.Errorf("BatchIDFrom = %q, want batch-789", got)
	}
}

func TestContextCorrelationMissing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if got := logging.RequestIDFrom(ctx); got != "" {
		t.Errorf("RequestIDFrom on empty ctx = %q, want empty", got)
	}
}

func TestEmptyCorrelationValueNotStored(t *testing.T) {
	t.Parallel()
	ctx := logging.WithRequestID(context.Background(), "")
	if got := logging.RequestIDFrom(ctx); got != "" {
		t.Errorf("empty request id should not be stored, got %q", got)
	}
}

func TestScopedLoggerCarriesCorrelation(t *testing.T) {
	t.Parallel()
	logger, buf := newCaptureLogger(t, "info", "json")

	ctx := logging.WithRequestID(context.Background(), "req-scope")
	ctx = logging.WithRepoID(ctx, "repo-1")

	scoped := logging.ScopedLogger(ctx, logger)
	scoped.Info("processing")

	out := buf.String()
	var record map[string]any
	if err := json.Unmarshal([]byte(out), &record); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if record["request_id"] != "req-scope" {
		t.Errorf("request_id = %v, want req-scope", record["request_id"])
	}
	if record["repo_id"] != "repo-1" {
		t.Errorf("repo_id = %v, want repo-1", record["repo_id"])
	}
}
