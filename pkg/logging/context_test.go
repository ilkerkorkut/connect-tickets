package logging_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

func TestPublisherAndConsumerGroupRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := logging.WithPublisherID(context.Background(), "pub-1")
	ctx = logging.WithConsumerGroup(ctx, "group-1")
	if got := logging.PublisherIDFrom(ctx); got != "pub-1" {
		t.Errorf("PublisherIDFrom = %q, want pub-1", got)
	}
	if got := logging.ConsumerGroupFrom(ctx); got != "group-1" {
		t.Errorf("ConsumerGroupFrom = %q, want group-1", got)
	}
}

func TestScopedLoggerCarriesPublisherAndConsumerGroup(t *testing.T) {
	t.Parallel()
	logger, buf := newCaptureLogger(t, "info", "json")
	ctx := logging.WithPublisherID(context.Background(), "pub-9")
	ctx = logging.WithConsumerGroup(ctx, "group-9")
	logging.ScopedLogger(ctx, logger).Info("cycle")
	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if record["publisher_id"] != "pub-9" {
		t.Errorf("publisher_id = %v, want pub-9", record["publisher_id"])
	}
	if record["consumer_group"] != "group-9" {
		t.Errorf("consumer_group = %v, want group-9", record["consumer_group"])
	}
}

func TestEventNoopWithoutLogger(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Event panicked without logger: %v", r)
		}
	}()
	logging.Event(context.Background(), "should not panic")
	logging.Debug(context.Background(), "should not panic")
	logging.Warn(context.Background(), "should not panic")
	logging.Error(context.Background(), "should not panic")
}

func TestEventEmitsScopedCorrelation(t *testing.T) {
	t.Parallel()
	logger, buf := newCaptureLogger(t, "debug", "json")
	ctx := logging.WithLogger(context.Background(), logger)
	ctx = logging.WithRequestID(ctx, "req-event")
	logging.Event(ctx, "repository created", "repo_id", "repo-7")
	out := buf.String()
	if !strings.Contains(out, "repository created") {
		t.Fatalf("missing message: %s", out)
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &record); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if record["request_id"] != "req-event" {
		t.Errorf("request_id = %v, want req-event", record["request_id"])
	}
	if record["repo_id"] != "repo-7" {
		t.Errorf("repo_id = %v, want repo-7", record["repo_id"])
	}
}

func TestRedactionCoversAllSensitiveKeys(t *testing.T) {
	t.Parallel()
	logger, buf := newCaptureLogger(t, "info", "json")
	logger.Info("call",
		"owner", "octocat",
		"token", tokenSentinel,
		"PASSWORD", tokenSentinel,
		"api_key", tokenSentinel,
		"database_url", "postgres://u:"+tokenSentinel+"@host/db",
		"nested", map[string]any{"secret": tokenSentinel},
	)
	out := buf.String()
	if strings.Contains(out, tokenSentinel) {
		t.Fatalf("secret leaked: %s", out)
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(out), &record); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if record["token"] != "[redacted]" {
		t.Errorf("token not redacted: %v", record["token"])
	}
	if record["owner"] != "octocat" {
		t.Errorf("owner altered: %v", record["owner"])
	}
	nested, ok := record["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested missing: %v", record["nested"])
	}
	if nested["secret"] != "[redacted]" {
		t.Errorf("nested secret not redacted: %v", nested["secret"])
	}
}

func TestRedactionTextFormat(t *testing.T) {
	t.Parallel()
	logger, buf := newCaptureLogger(t, "info", "text")
	logger.Info("call", "token", tokenSentinel, "owner", "octo")
	out := buf.String()
	if strings.Contains(out, tokenSentinel) {
		t.Fatalf("secret leaked in text format: %s", out)
	}
	if !strings.Contains(out, "token=[redacted]") {
		t.Fatalf("token not redacted in text: %s", out)
	}
}
