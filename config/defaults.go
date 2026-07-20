package config

import (
	"os"
	"time"
)

//nolint:mnd
func defaults() Config {
	return Config{
		App: AppConfig{
			Environment:     "development",
			Mode:            "all",
			Version:         "dev",
			ShutdownTimeout: 30 * time.Second,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
		HTTP: HTTPConfig{
			Port:              8080,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
			RequestTimeout:    30 * time.Second,
			MaxBodyBytes:      1 << 20,
		},
		Database: DatabaseConfig{
			MaxOpenConns:      25,
			MaxIdleConns:      5,
			ConnMaxLifetime:   30 * time.Minute,
			HealthCheckPeriod: 1 * time.Minute,
			QueryTimeout:      5 * time.Second,
		},
		Redis: RedisConfig{
			Addr:         "localhost:6379",
			DB:           0,
			PoolSize:     10,
			MinIdleConns: 1,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		},
		Cache: CacheConfig{
			TTL:      5 * time.Minute,
			LockTTL:  30 * time.Second,
			LockWait: 5 * time.Second,
		},
		Kafka: KafkaConfig{
			ClientID:      "github-tracker",
			ConsumerGroup: "github-repo-refresh-v1",
			RefreshTopic:  "github.repo.refresh.v1",
			DLQTopic:      "github.repo.refresh.dlq.v1",
			Partitions:    5,

			DialTimeout:      10 * time.Second,
			RequestTimeout:   10 * time.Second,
			ProduceTimeout:   10 * time.Second,
			FetchMaxBytes:    1 << 20,
			MessageMaxBytes:  1 << 20,
			ProducerMaxBytes: 1 << 20,
		},
		GitHub: GitHubConfig{
			Timeout:             10 * time.Second,
			UserAgent:           "connect-tickets",
			BaseURL:             "https://api.github.com",
			MaxIdleConns:        32,
			MaxIdleConnsPerHost: 8,
			IdleConnTimeout:     90 * time.Second,
		},
		Worker: WorkerConfig{
			LeaseDuration:   30 * time.Second,
			MaxAttempts:     3,
			Concurrency:     5,
			ShutdownTimeout: 30 * time.Second,
		},
		Outbox: OutboxConfig{
			PollInterval:       1 * time.Second,
			BatchSize:          100,
			PublishLease:       30 * time.Second,
			MaxPublishAttempts: 5,
			ShutdownTimeout:    30 * time.Second,
		},
	}
}

func getenvOS(key string) string {
	return os.Getenv(key)
}
