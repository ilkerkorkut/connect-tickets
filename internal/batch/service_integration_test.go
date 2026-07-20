//go:build integration

package batch

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/ent"
	"github.com/ilkerkorkut/connect-tickets/ent/batchjob"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/database"
	"github.com/ilkerkorkut/connect-tickets/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestCreateRefreshAllPersistsSnapshotAndEmptyBatch(t *testing.T) {
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
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	client, err := ent.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := pool.Exec(ctx, "TRUNCATE outbox_events, batch_jobs, batches, repositories CASCADE"); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		id := uuid.New()
		name := fmt.Sprintf("repo-%d", index)
		if _, err := client.Repository.Create().SetID(id).SetOwner("owner").SetName(name).SetFullName("owner/" + name).SetNormalizedFullName("owner/" + name).SetStars(0).SetHTMLURL("https://github.com/owner/" + name).SetFetchedAt(time.Now().UTC()).Save(ctx); err != nil {
			t.Fatal(err)
		}
	}
	service := NewService(client)
	batchID, err := service.CreateRefreshAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := client.BatchJob.Query().Where(batchjob.BatchIDEQ(batchID)).Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	events, err := client.OutboxEvent.Query().Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if jobs != 2 || events != 2 {
		t.Fatalf("jobs=%d events=%d", jobs, events)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE outbox_events, batch_jobs, batches, repositories CASCADE"); err != nil {
		t.Fatal(err)
	}
	emptyBatchID, err := service.CreateRefreshAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	emptyBatch, err := client.Batch.Get(ctx, emptyBatchID)
	if err != nil {
		t.Fatal(err)
	}
	if emptyBatch.CompletedAt == nil {
		t.Fatal("empty batch was not completed")
	}
}
