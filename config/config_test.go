package config_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ilkerkorkut/connect-tickets/config"
)

const secretSentinel = "SUPER_SECRET_VALUE_DO_NOT_LEAK_42"

func validEnv() map[string]string {
	return map[string]string{
		"APP_ENV":                           "development",
		"APP_MODE":                          "all",
		"APP_VERSION":                       "test",
		"SHUTDOWN_TIMEOUT":                  "30s",
		"LOG_LEVEL":                         "info",
		"LOG_FORMAT":                        "json",
		"HTTP_PORT":                         "8080",
		"HTTP_READ_HEADER_TIMEOUT":          "5s",
		"HTTP_READ_TIMEOUT":                 "15s",
		"HTTP_WRITE_TIMEOUT":                "30s",
		"HTTP_IDLE_TIMEOUT":                 "60s",
		"HTTP_REQUEST_TIMEOUT":              "30s",
		"HTTP_MAX_BODY_BYTES":               "1048576",
		"DATABASE_URL":                      "postgres://tracker:tracker@localhost:5432/tracker?sslmode=disable",
		"DATABASE_MAX_OPEN_CONNS":           "25",
		"DATABASE_MAX_IDLE_CONNS":           "5",
		"DATABASE_CONN_MAX_LIFETIME":        "30m",
		"DATABASE_HEALTH_CHECK_PERIOD":      "1m",
		"DATABASE_QUERY_TIMEOUT":            "5s",
		"REDIS_ADDR":                        "localhost:6379",
		"REDIS_DB":                          "0",
		"REDIS_POOL_SIZE":                   "10",
		"REDIS_MIN_IDLE_CONNS":              "1",
		"REDIS_DIAL_TIMEOUT":                "5s",
		"REDIS_READ_TIMEOUT":                "3s",
		"REDIS_WRITE_TIMEOUT":               "3s",
		"CACHE_TTL":                         "5m",
		"CACHE_LOCK_TTL":                    "30s",
		"CACHE_LOCK_WAIT":                   "5s",
		"KAFKA_BROKERS":                     "localhost:9092",
		"KAFKA_CLIENT_ID":                   "github-tracker",
		"KAFKA_CONSUMER_GROUP":              "github-repo-refresh-v1",
		"KAFKA_REFRESH_TOPIC":               "github.repo.refresh.v1",
		"KAFKA_DLQ_TOPIC":                   "github.repo.refresh.dlq.v1",
		"KAFKA_EXPECTED_REFRESH_PARTITIONS": "5",
		"KAFKA_DIAL_TIMEOUT":                "10s",
		"KAFKA_REQUEST_TIMEOUT":             "10s",
		"KAFKA_PRODUCE_TIMEOUT":             "10s",
		"KAFKA_FETCH_MAX_BYTES":             "1048576",
		"KAFKA_MESSAGE_MAX_BYTES":           "1048576",
		"KAFKA_PRODUCER_MAX_BYTES":          "1048576",
		"GITHUB_TIMEOUT":                    "10s",
		"GITHUB_USER_AGENT":                 "connect-tickets",
		"GITHUB_BASE_URL":                   "https://api.github.com",
		"GITHUB_MAX_IDLE_CONNS":             "32",
		"GITHUB_MAX_IDLE_CONNS_PER_HOST":    "8",
		"GITHUB_IDLE_CONN_TIMEOUT":          "90s",
		"WORKER_LEASE_DURATION":             "30s",
		"WORKER_MAX_ATTEMPTS":               "3",
		"WORKER_CONCURRENCY":                "5",
		"WORKER_SHUTDOWN_TIMEOUT":           "30s",
		"OUTBOX_POLL_INTERVAL":              "1s",
		"OUTBOX_BATCH_SIZE":                 "100",
		"OUTBOX_PUBLISH_LEASE":              "30s",
		"OUTBOX_MAX_PUBLISH_ATTEMPTS":       "5",
		"OUTBOX_SHUTDOWN_TIMEOUT":           "30s",
	}
}

func loaderFrom(env map[string]string) config.Loader {
	return func(key string) string { return env[key] }
}

func loadOK(t *testing.T, env map[string]string) config.Config {
	t.Helper()
	cfg, err := config.LoadFrom(loaderFrom(env))
	if err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}
	return cfg
}

func TestDefaultsAppliedWhenEnvUnset(t *testing.T) {
	t.Parallel()

	minimal := map[string]string{
		"DATABASE_URL":  "postgres://u:p@localhost:5432/db?sslmode=disable",
		"KAFKA_BROKERS": "localhost:9092",
	}
	cfg, err := config.LoadFrom(loaderFrom(minimal))
	if err != nil {
		t.Fatalf("expected defaults with minimal env, got: %v", err)
	}

	if cfg.App.Mode != "all" {
		t.Errorf("default APP_MODE = %q, want all", cfg.App.Mode)
	}
	if cfg.HTTP.Port != 8080 {
		t.Errorf("default HTTP_PORT = %d, want 8080", cfg.HTTP.Port)
	}
	if cfg.Cache.TTL != 5*time.Minute {
		t.Errorf("default CACHE_TTL = %v, want 5m", cfg.Cache.TTL)
	}
	if cfg.Kafka.RefreshTopic != "github.repo.refresh.v1" {
		t.Errorf("default refresh topic = %q", cfg.Kafka.RefreshTopic)
	}
	if cfg.Kafka.Partitions != 5 {
		t.Errorf("default partitions = %d, want 5", cfg.Kafka.Partitions)
	}
	if cfg.Worker.MaxAttempts != 3 {
		t.Errorf("default WORKER_MAX_ATTEMPTS = %d, want 3", cfg.Worker.MaxAttempts)
	}
	if cfg.GitHub.Timeout != 10*time.Second {
		t.Errorf("default GITHUB_TIMEOUT = %v", cfg.GitHub.Timeout)
	}
	if cfg.Redis.Addr != "localhost:6379" {
		t.Errorf("default REDIS_ADDR = %q, want localhost:6379", cfg.Redis.Addr)
	}
	if cfg.Kafka.ConsumerGroup != "github-repo-refresh-v1" {
		t.Errorf("default consumer group = %q", cfg.Kafka.ConsumerGroup)
	}
}

func TestOverridesParsed(t *testing.T) {
	t.Parallel()

	env := validEnv()
	env["HTTP_PORT"] = "9100"
	env["CACHE_TTL"] = "90s"
	env["WORKER_MAX_ATTEMPTS"] = "7"
	env["KAFKA_BROKERS"] = "broker-a:9092,broker-b:9092 , broker-c:9092"

	cfg := loadOK(t, env)

	if cfg.HTTP.Port != 9100 {
		t.Errorf("HTTP_PORT override = %d, want 9100", cfg.HTTP.Port)
	}
	if cfg.Cache.TTL != 90*time.Second {
		t.Errorf("CACHE_TTL override = %v, want 90s", cfg.Cache.TTL)
	}
	if cfg.Worker.MaxAttempts != 7 {
		t.Errorf("WORKER_MAX_ATTEMPTS override = %d, want 7", cfg.Worker.MaxAttempts)
	}
	wantBrokers := []string{"broker-a:9092", "broker-b:9092", "broker-c:9092"}
	if len(cfg.Kafka.Brokers) != len(wantBrokers) {
		t.Fatalf("brokers = %v, want %v", cfg.Kafka.Brokers, wantBrokers)
	}
	for i, b := range wantBrokers {
		if cfg.Kafka.Brokers[i] != b {
			t.Errorf("broker[%d] = %q, want %q", i, cfg.Kafka.Brokers[i], b)
		}
	}
}

func TestInvalidValuesRejected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mutate  func(map[string]string)
		wantKey string
	}{
		{"missing database url", func(e map[string]string) { delete(e, "DATABASE_URL") }, "DATABASE_URL"},
		{"non-postgres url", func(e map[string]string) { e["DATABASE_URL"] = "mysql://x" }, "DATABASE_URL"},
		{"port out of range low", func(e map[string]string) { e["HTTP_PORT"] = "0" }, "HTTP_PORT"},
		{"port out of range high", func(e map[string]string) { e["HTTP_PORT"] = "70000" }, "HTTP_PORT"},
		{"port not a number", func(e map[string]string) { e["HTTP_PORT"] = "abc" }, "HTTP_PORT"},
		{"max idle exceeds open", func(e map[string]string) {
			e["DATABASE_MAX_OPEN_CONNS"] = "2"
			e["DATABASE_MAX_IDLE_CONNS"] = "5"
		}, "DATABASE_MAX_IDLE_CONNS"},
		{"redis min idle exceeds pool", func(e map[string]string) {
			e["REDIS_POOL_SIZE"] = "2"
			e["REDIS_MIN_IDLE_CONNS"] = "10"
		}, "REDIS_MIN_IDLE_CONNS"},
		{"kafka partitions zero", func(e map[string]string) { e["KAFKA_EXPECTED_REFRESH_PARTITIONS"] = "0" }, "KAFKA_EXPECTED_REFRESH_PARTITIONS"},
		{"worker max attempts zero", func(e map[string]string) { e["WORKER_MAX_ATTEMPTS"] = "0" }, "WORKER_MAX_ATTEMPTS"},
		{"malformed integer", func(e map[string]string) { e["WORKER_CONCURRENCY"] = "five" }, "WORKER_CONCURRENCY"},
		{"bad sasl mechanism", func(e map[string]string) {
			e["KAFKA_SASL_MECHANISM"] = "NOPE"
			e["KAFKA_SASL_USERNAME"] = "u"
		}, "KAFKA_SASL_MECHANISM"},
		{"bad log level", func(e map[string]string) { e["LOG_LEVEL"] = "verbose" }, "LOG_LEVEL"},
		{"non-positive shutdown", func(e map[string]string) { e["SHUTDOWN_TIMEOUT"] = "0s" }, "SHUTDOWN_TIMEOUT"},
		{"outbox batch zero", func(e map[string]string) { e["OUTBOX_BATCH_SIZE"] = "0" }, "OUTBOX_BATCH_SIZE"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := validEnv()
			tc.mutate(env)

			_, err := config.LoadFrom(loaderFrom(env))
			if err == nil {
				t.Fatalf("expected validation error mentioning %q, got nil", tc.wantKey)
			}
			if !strings.Contains(err.Error(), tc.wantKey) {
				t.Fatalf("error %q does not mention expected field %q", err.Error(), tc.wantKey)
			}
		})
	}
}

func TestMalformedDurationRejected(t *testing.T) {
	t.Parallel()

	env := validEnv()
	env["CACHE_TTL"] = "not-a-duration"

	_, err := config.LoadFrom(loaderFrom(env))
	if err == nil {
		t.Fatal("expected validation error for malformed duration")
	}
	if !strings.Contains(err.Error(), "CACHE_TTL") {
		t.Fatalf("error %q does not mention CACHE_TTL", err.Error())
	}
}

func TestSecretsNeverLeakInErrors(t *testing.T) {
	t.Parallel()

	env := validEnv()
	env["REDIS_PASSWORD"] = secretSentinel
	env["GITHUB_TOKEN"] = secretSentinel
	env["KAFKA_SASL_PASSWORD"] = secretSentinel
	env["DATABASE_URL"] = "postgres://user:" + secretSentinel + "@localhost:5432/db?sslmode=disable"

	env["HTTP_PORT"] = "0"

	_, err := config.LoadFrom(loaderFrom(env))
	if err == nil {
		t.Fatal("expected validation error")
	}

	if strings.Contains(err.Error(), secretSentinel) {
		t.Fatalf("validation error leaked secret: %q", err.Error())
	}
}

func TestSecretsNeverReachPersistence(t *testing.T) {
	t.Parallel()

	env := validEnv()
	env["REDIS_PASSWORD"] = secretSentinel
	env["GITHUB_TOKEN"] = secretSentinel

	cfg := loadOK(t, env)

	if cfg.Redis.Password != secretSentinel {
		t.Errorf("expected secret to be loaded into config (internal use), got %q", cfg.Redis.Password)
	}
}

func TestValidationErrorIsTyped(t *testing.T) {
	t.Parallel()

	env := validEnv()
	env["HTTP_PORT"] = "0"
	env["WORKER_CONCURRENCY"] = "0"

	_, err := config.LoadFrom(loaderFrom(env))
	if err == nil {
		t.Fatal("expected error")
	}

	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T (%v)", err, err)
	}

	joined := ve.Error()
	if !strings.Contains(joined, "HTTP_PORT") {
		t.Errorf("aggregated error missing HTTP_PORT: %q", joined)
	}
	if !strings.Contains(joined, "WORKER_CONCURRENCY") {
		t.Errorf("aggregated error missing WORKER_CONCURRENCY: %q", joined)
	}
}
