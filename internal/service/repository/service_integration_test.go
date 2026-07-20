//go:build integration

package repository

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/ent"
	"github.com/ilkerkorkut/connect-tickets/ent/repositorychange"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestDeleteTombstonesRepositoryWithBatchHistory(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, stdlib.OpenDBFromPool(pool))))
	t.Cleanup(func() { _ = client.Close() })
	now := time.Now().UTC().Truncate(time.Microsecond)
	repoID := uuid.New()
	recreatedID := uuid.New()
	batchID := uuid.New()
	if _, err := client.Repository.Create().SetID(repoID).SetOwner("owner").SetName("repo").SetFullName("owner/repo").SetNormalizedFullName("owner/repo-" + repoID.String()).SetStars(1).SetHTMLURL("https://github.com/owner/repo").SetFetchedAt(now).Save(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Batch.DeleteOneID(batchID).Exec(context.Background())
		_, _ = client.RepositoryChange.Delete().Where(repositorychange.RepoIDEQ(repoID)).Exec(context.Background())
		_ = client.Repository.DeleteOneID(recreatedID).Exec(context.Background())
		_ = client.Repository.DeleteOneID(repoID).Exec(context.Background())
	})
	if _, err := client.Batch.Create().SetID(batchID).SetCreatedAt(now).Save(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.BatchJob.Create().SetID(uuid.New()).SetBatchID(batchID).SetRepoID(repoID).SetCreatedAt(now).SetUpdatedAt(now).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	service := NewService(client, nil)
	if err := service.Delete(ctx, repoID.String()); err != nil {
		t.Fatal(err)
	}
	stored, err := client.Repository.Get(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.DeletedAt == nil {
		t.Fatal("repository was not tombstoned")
	}
	if _, err := client.Repository.Create().SetID(recreatedID).SetOwner("owner").SetName("repo").SetFullName("owner/repo").SetNormalizedFullName(stored.NormalizedFullName).SetStars(1).SetHTMLURL("https://github.com/owner/repo").SetFetchedAt(now).Save(ctx); err != nil {
		t.Fatalf("create a new active repository after tombstone: %v", err)
	}
	if _, err := service.Get(ctx, repoID.String()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get() error = %v, want ErrNotFound", err)
	}
	page, err := service.List(ctx, ListInput{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range page.Repositories {
		if row.ID == repoID {
			t.Fatal("List() returned a tombstoned repository")
		}
	}
	changes, err := client.RepositoryChange.Query().Where(repositorychange.RepoIDEQ(repoID), repositorychange.OperationEQ(repositorychange.OperationDeleted)).Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if changes != 1 {
		t.Fatalf("deleted changes = %d, want 1", changes)
	}
}
