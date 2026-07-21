package testkit

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

type Environment struct {
	DatabaseURL  string
	RedisAddress string
	KafkaBrokers []string
}

func Start(t *testing.T) Environment {
	t.Helper()
	ctx := context.Background()
	migrations := migrationFiles(t)
	t.Logf("testcontainers component=postgres state=starting image=postgres:16.4-alpine migration_scripts=%d", len(migrations))
	postgresContainer, err := postgres.Run(ctx, "postgres:16.4-alpine", postgres.WithDatabase("tracker"), postgres.WithUsername("tracker"), postgres.WithPassword("tracker"), postgres.WithInitScripts(migrations...), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { terminate(t, postgresContainer) })
	databaseURL, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("testcontainers component=postgres state=ready")
	t.Log("testcontainers component=redis state=starting image=redis:7.4.1-alpine")
	redisContainer, err := redis.Run(ctx, "redis:7.4.1-alpine")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { terminate(t, redisContainer) })
	redisURL, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	parsedRedisURL, err := url.Parse(redisURL)
	if err != nil || parsedRedisURL.Host == "" {
		t.Fatal("invalid Redis connection string")
	}
	t.Log("testcontainers component=redis state=ready")
	t.Log("testcontainers component=kafka state=starting image=confluentinc/confluent-local:7.5.0")
	kafkaContainer, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0", kafka.WithClusterID("connect-tickets-test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { terminate(t, kafkaContainer) })
	kafkaBrokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("testcontainers component=kafka state=ready brokers=%d", len(kafkaBrokers))
	return Environment{DatabaseURL: databaseURL, RedisAddress: parsedRedisURL.Host, KafkaBrokers: kafkaBrokers}
}

func migrationFiles(t *testing.T) []string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve testkit path")
	}
	entries, err := os.ReadDir(filepath.Join(filepath.Dir(file), "..", "..", "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(filepath.Dir(file), "..", "..", "migrations", entry.Name()))
	}
	if len(files) == 0 {
		t.Fatal("no migrations found")
	}
	return files
}

func terminate(t *testing.T, container testcontainers.Container) {
	t.Helper()
	if err := testcontainers.TerminateContainer(container); err != nil {
		t.Errorf("terminate container: %v", err)
	}
}
