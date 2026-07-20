//go:build integration

package outbox

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/ent"
	"github.com/ilkerkorkut/connect-tickets/pkg/postgresql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestStoreClaimsAndProtectsOwnership(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	pool, err := database.NewPool(ctx, config.DatabaseConfig{URL: url, MaxOpenConns: 10, MaxIdleConns: 1, ConnMaxLifetime: time.Minute, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	if _, err := pool.Exec(ctx, "TRUNCATE outbox_events"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 20, 13, 0, 0, 0, time.UTC)
	for _, status := range []string{"pending", "publishing"} {
		lease := any(nil)
		if status == "publishing" {
			lease = now.Add(-time.Second)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO outbox_events (id, event_id, event_type, aggregate_id, partition_key, payload, status, available_at, publishing_owner, publishing_lease_until, publish_attempts, created_at, updated_at) VALUES ($1, $2, 'github.repo.refresh.v1', $3, $4, $5::jsonb, $6, $7, 'old-owner', $8, 0, $7, $7)`, uuid.New(), uuid.NewString(), uuid.New(), uuid.NewString(), `{}`, status, now.Add(-time.Second), lease); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO outbox_events (id, event_id, event_type, aggregate_id, partition_key, payload, status, available_at, publish_attempts, created_at, updated_at) VALUES ($1, $2, 'github.repo.refresh.v1', $3, $4, $5::jsonb, 'pending', $6, 0, $7, $7)`, uuid.New(), uuid.NewString(), uuid.New(), uuid.NewString(), `{}`, now.Add(10*time.Second), now); err != nil {
		t.Fatal(err)
	}
	client, err := ent.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	store := NewStore(client)
	claimed, err := store.ClaimDue(ctx, "publisher-a", now, time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed = %d, want 2", len(claimed))
	}
	second, err := store.ClaimDue(ctx, "publisher-b", now, time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("second claim = %d, want 0", len(second))
	}
	delayed, err := store.ClaimDue(ctx, "publisher-b", now.Add(20*time.Second), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(delayed) != 1 {
		t.Fatalf("delayed claim = %d, want 1", len(delayed))
	}
	if err := store.MarkPublished(ctx, claimed[0].ID, "publisher-b", now); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("wrong owner error = %v", err)
	}
	if err := store.MarkPublished(ctx, claimed[0].ID, "publisher-a", now); err != nil {
		t.Fatal(err)
	}
}
