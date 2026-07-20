//go:build integration

package github

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ilkerkorkut/connect-tickets/config"
	goredis "github.com/redis/go-redis/v9"
)

func TestRedisGatewayIntegration(t *testing.T) {
	address := os.Getenv("TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("TEST_REDIS_ADDR is required")
	}
	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush Redis: %v", err)
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	gateway, err := NewGateway(fetchFunc(func(context.Context, string, string) (Repository, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return testRepository("openai", "gpt", 1), nil
	}), client, config.CacheConfig{TTL: time.Minute, LockTTL: time.Second, LockWait: time.Second}, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewGateway() error = %v", err)
	}
	gateway.pollInterval = time.Millisecond
	gateway.jitter = func(time.Duration) time.Duration { return 0 }

	results := make(chan error, 8)
	for range 8 {
		go func() {
			_, err := gateway.FetchRepository(ctx, "OpenAI", "GPT")
			results <- err
		}()
	}
	<-started
	close(release)
	for range 8 {
		if err := <-results; err != nil {
			t.Fatalf("FetchRepository() error = %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want 1", calls.Load())
	}

	store := redisStore{client: client}
	if err := store.Set(ctx, "ownership", "new-owner", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.CompareAndDelete(ctx, "ownership", "old-owner"); err != nil {
		t.Fatal(err)
	}
	if value, err := store.Get(ctx, "ownership"); err != nil || value != "new-owner" {
		t.Fatalf("ownership-safe release value=%q error=%v", value, err)
	}

	if err := gateway.RecordCooldown(ctx, &RequestError{Kind: ErrRateLimited, RateLimit: RateLimit{RetryAfter: time.Second}}); err != nil {
		t.Fatalf("RecordCooldown() error = %v", err)
	}
	if _, err := gateway.FetchRepository(ctx, "openai", "other"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("cooldown error = %v", err)
	}
}
