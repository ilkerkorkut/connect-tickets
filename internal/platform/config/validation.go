package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var ErrValidation = errors.New("configuration validation failed")

type ValidationError struct {
	Fields []string
}

func (e *ValidationError) Error() string {
	if len(e.Fields) == 0 {
		return ErrValidation.Error()
	}
	if len(e.Fields) == 1 {
		return fmt.Sprintf("%s: %s", ErrValidation.Error(), e.Fields[0])
	}
	return fmt.Sprintf("%s: %s", ErrValidation.Error(), strings.Join(e.Fields, "; "))
}

func (e *ValidationError) add(field string) {
	e.Fields = append(e.Fields, field)
}

func (e *ValidationError) HasFailures() bool {
	return len(e.Fields) > 0
}

func (e *ValidationError) AsError() error {
	if !e.HasFailures() {
		return nil
	}
	return e
}

func (c Config) Validate() error {
	v := &ValidationError{}

	c.validateApp(v)
	c.validateLog(v)
	c.validateHTTP(v)
	c.validateDatabase(v)
	c.validateRedis(v)
	c.validateCache(v)
	c.validateKafka(v)
	c.validateGitHub(v)
	c.validateWorker(v)
	c.validateOutbox(v)

	return v.AsError()
}

func (c Config) validateApp(v *ValidationError) {
	if c.App.Mode == "" {
		v.add("APP_MODE is required")
	}
	if c.App.Environment == "" {
		v.add("APP_ENV is required")
	}
	if c.App.ShutdownTimeout <= 0 {
		v.add("SHUTDOWN_TIMEOUT must be positive")
	}
}

func (c Config) validateLog(v *ValidationError) {
	switch strings.ToLower(c.Log.Level) {
	case "", "debug", "info", "warn", "error":
	default:
		v.add("LOG_LEVEL must be one of debug|info|warn|error")
	}
	switch strings.ToLower(c.Log.Format) {
	case "", "json", "text":
	default:
		v.add("LOG_FORMAT must be one of json|text")
	}
}

func (c Config) validateHTTP(v *ValidationError) {
	if c.HTTP.Port < 1 || c.HTTP.Port > 65535 {
		v.add("HTTP_PORT must be between 1 and 65535")
	}
	if c.HTTP.ReadHeaderTimeout <= 0 {
		v.add("HTTP_READ_HEADER_TIMEOUT must be positive")
	}
	if c.HTTP.ReadTimeout <= 0 {
		v.add("HTTP_READ_TIMEOUT must be positive")
	}
	if c.HTTP.WriteTimeout <= 0 {
		v.add("HTTP_WRITE_TIMEOUT must be positive")
	}
	if c.HTTP.IdleTimeout <= 0 {
		v.add("HTTP_IDLE_TIMEOUT must be positive")
	}
	if c.HTTP.RequestTimeout <= 0 {
		v.add("HTTP_REQUEST_TIMEOUT must be positive")
	}
	if c.HTTP.MaxBodyBytes <= 0 {
		v.add("HTTP_MAX_BODY_BYTES must be positive")
	}
}

func (c Config) validateDatabase(v *ValidationError) {
	if strings.TrimSpace(c.Database.URL) == "" {
		v.add("DATABASE_URL is required")
	} else if !strings.HasPrefix(c.Database.URL, "postgres://") && !strings.HasPrefix(c.Database.URL, "postgresql://") {
		v.add("DATABASE_URL must be a postgres URL")
	}
	if c.Database.MaxOpenConns < 1 {
		v.add("DATABASE_MAX_OPEN_CONNS must be at least 1")
	}
	if c.Database.MaxIdleConns < 0 {
		v.add("DATABASE_MAX_IDLE_CONNS must not be negative")
	}
	if c.Database.MaxIdleConns > c.Database.MaxOpenConns {
		v.add("DATABASE_MAX_IDLE_CONNS must not exceed DATABASE_MAX_OPEN_CONNS")
	}
	if c.Database.ConnMaxLifetime < 0 {
		v.add("DATABASE_CONN_MAX_LIFETIME must not be negative")
	}
	if c.Database.HealthCheckPeriod < 0 {
		v.add("DATABASE_HEALTH_CHECK_PERIOD must not be negative")
	}
	if c.Database.QueryTimeout <= 0 {
		v.add("DATABASE_QUERY_TIMEOUT must be positive")
	}
}

func (c Config) validateRedis(v *ValidationError) {
	if strings.TrimSpace(c.Redis.Addr) == "" {
		v.add("REDIS_ADDR is required")
	}
	if c.Redis.PoolSize < 1 {
		v.add("REDIS_POOL_SIZE must be at least 1")
	}
	if c.Redis.MinIdleConns < 0 {
		v.add("REDIS_MIN_IDLE_CONNS must not be negative")
	}
	if c.Redis.MinIdleConns > c.Redis.PoolSize {
		v.add("REDIS_MIN_IDLE_CONNS must not exceed REDIS_POOL_SIZE")
	}
	if c.Redis.DialTimeout <= 0 {
		v.add("REDIS_DIAL_TIMEOUT must be positive")
	}
	if c.Redis.ReadTimeout <= 0 {
		v.add("REDIS_READ_TIMEOUT must be positive")
	}
	if c.Redis.WriteTimeout <= 0 {
		v.add("REDIS_WRITE_TIMEOUT must be positive")
	}
}

func (c Config) validateCache(v *ValidationError) {
	if c.Cache.TTL <= 0 {
		v.add("CACHE_TTL must be positive")
	}
	if c.Cache.LockTTL <= 0 {
		v.add("CACHE_LOCK_TTL must be positive")
	}
	if c.Cache.LockWait <= 0 {
		v.add("CACHE_LOCK_WAIT must be positive")
	}
	if c.Cache.LockTTL <= c.GitHub.Timeout {
		v.add("CACHE_LOCK_TTL must exceed GITHUB_TIMEOUT")
	}
}

func (c Config) validateKafka(v *ValidationError) {
	if len(c.Kafka.Brokers) == 0 {
		v.add("KAFKA_BROKERS is required")
	}
	for i, b := range c.Kafka.Brokers {
		if strings.TrimSpace(b) == "" {
			v.add(fmt.Sprintf("KAFKA_BROKERS[%d] must not be empty", i))
		}
	}
	if c.Kafka.ClientID == "" {
		v.add("KAFKA_CLIENT_ID is required")
	}
	if c.Kafka.ConsumerGroup == "" {
		v.add("KAFKA_CONSUMER_GROUP is required")
	}
	if c.Kafka.RefreshTopic == "" {
		v.add("KAFKA_REFRESH_TOPIC is required")
	}
	if c.Kafka.DLQTopic == "" {
		v.add("KAFKA_DLQ_TOPIC is required")
	}
	if c.Kafka.Partitions < 1 {
		v.add("KAFKA_EXPECTED_REFRESH_PARTITIONS must be at least 1")
	}
	if c.Kafka.DialTimeout <= 0 {
		v.add("KAFKA_DIAL_TIMEOUT must be positive")
	}
	if c.Kafka.RequestTimeout <= 0 {
		v.add("KAFKA_REQUEST_TIMEOUT must be positive")
	}
	if c.Kafka.ProduceTimeout <= 0 {
		v.add("KAFKA_PRODUCE_TIMEOUT must be positive")
	}
	if c.Kafka.FetchMaxBytes <= 0 {
		v.add("KAFKA_FETCH_MAX_BYTES must be positive")
	}
	if c.Kafka.MessageMaxBytes <= 0 {
		v.add("KAFKA_MESSAGE_MAX_BYTES must be positive")
	}
	if c.Kafka.ProducerMaxBytes <= 0 {
		v.add("KAFKA_PRODUCER_MAX_BYTES must be positive")
	}
	if c.Kafka.SASLMechanism != "" {
		switch strings.ToUpper(c.Kafka.SASLMechanism) {
		case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		default:
			v.add("KAFKA_SASL_MECHANISM must be one of PLAIN|SCRAM-SHA-256|SCRAM-SHA-512")
		}
		if c.Kafka.SASLUsername == "" {
			v.add("KAFKA_SASL_USERNAME is required when KAFKA_SASL_MECHANISM is set")
		}
	}
}

func (c Config) validateGitHub(v *ValidationError) {
	if c.GitHub.Timeout <= 0 {
		v.add("GITHUB_TIMEOUT must be positive")
	}
	if c.GitHub.UserAgent == "" {
		v.add("GITHUB_USER_AGENT is required")
	}
	if c.GitHub.BaseURL == "" {
		v.add("GITHUB_BASE_URL is required")
	}
	if c.GitHub.MaxIdleConns < 0 {
		v.add("GITHUB_MAX_IDLE_CONNS must not be negative")
	}
	if c.GitHub.MaxIdleConnsPerHost < 1 {
		v.add("GITHUB_MAX_IDLE_CONNS_PER_HOST must be at least 1")
	}
	if c.GitHub.IdleConnTimeout <= 0 {
		v.add("GITHUB_IDLE_CONN_TIMEOUT must be positive")
	}
}

func (c Config) validateWorker(v *ValidationError) {
	if c.Worker.LeaseDuration <= 0 {
		v.add("WORKER_LEASE_DURATION must be positive")
	}
	if c.Worker.MaxAttempts < 1 {
		v.add("WORKER_MAX_ATTEMPTS must be at least 1")
	}
	if c.Worker.Concurrency < 1 {
		v.add("WORKER_CONCURRENCY must be at least 1")
	}
	if c.Worker.ShutdownTimeout <= 0 {
		v.add("WORKER_SHUTDOWN_TIMEOUT must be positive")
	}
}

func (c Config) validateOutbox(v *ValidationError) {
	if c.Outbox.PollInterval <= 0 {
		v.add("OUTBOX_POLL_INTERVAL must be positive")
	}
	if c.Outbox.BatchSize < 1 {
		v.add("OUTBOX_BATCH_SIZE must be at least 1")
	}
	if c.Outbox.PublishLease <= 0 {
		v.add("OUTBOX_PUBLISH_LEASE must be positive")
	}
	if c.Outbox.MaxPublishAttempts < 1 {
		v.add("OUTBOX_MAX_PUBLISH_ATTEMPTS must be at least 1")
	}
	if c.Outbox.ShutdownTimeout <= 0 {
		v.add("OUTBOX_SHUTDOWN_TIMEOUT must be positive")
	}
}

func durationOr(v *ValidationError, getenv Loader, key string, fallback time.Duration) time.Duration {
	raw := getenv(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		v.add(key + " must be a valid duration (e.g. 5s, 1m)")
		return fallback
	}
	return d
}

func intOr(v *ValidationError, getenv Loader, key string, fallback int) int {
	raw := getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		v.add(key + " must be a valid integer")
		return fallback
	}
	return n
}

func int32Or(v *ValidationError, getenv Loader, key string, fallback int32) int32 {
	raw := getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		v.add(key + " must be a valid 32-bit integer")
		return fallback
	}
	return int32(n)
}

func int64Or(v *ValidationError, getenv Loader, key string, fallback int64) int64 {
	raw := getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		v.add(key + " must be a valid integer")
		return fallback
	}
	return n
}

func boolOr(v *ValidationError, getenv Loader, key string, fallback bool) bool {
	raw := getenv(key)
	if raw == "" {
		return fallback
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		v.add(key + " must be a valid boolean")
		return fallback
	}
	return b
}

func stringsCSVOr(getenv Loader, key string, fallback []string) []string {
	raw := getenv(key)
	if raw == "" {
		return fallback
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}
