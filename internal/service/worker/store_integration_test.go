//go:build integration

package worker

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/ent"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	githubgateway "github.com/ilkerkorkut/connect-tickets/internal/client/github"
	"github.com/ilkerkorkut/connect-tickets/internal/event/kafka"
)

func TestStoreSuccessPreservesNotesAndIsIdempotent(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	repoID, batchID, jobID := uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_, err = pool.Exec(ctx, `
		INSERT INTO repositories (id, created_at, updated_at, owner, name, full_name, normalized_full_name, description, stars, language, html_url, notes, fetched_at)
		VALUES ($1, $2, $2, 'owner', 'repo', 'owner/repo', 'owner/repo', NULL, 0, NULL, 'https://example.test/owner/repo', 'keep this note', $2)`, repoID, now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM repository_changes WHERE repo_id = $1", repoID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM batches WHERE id = $1", batchID)
		_, _ = pool.Exec(context.Background(), "DELETE FROM repositories WHERE id = $1", repoID)
	}()
	if _, err := pool.Exec(ctx, "INSERT INTO batches (id, created_at) VALUES ($1, $2)", batchID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO batch_jobs (id, batch_id, repo_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $4)", jobID, batchID, repoID, now); err != nil {
		t.Fatal(err)
	}

	message := kafka.RefreshMessage{SchemaVersion: kafka.RefreshSchemaVersion, EventID: uuid.NewString(), BatchJobID: jobID.String(), BatchID: batchID.String(), RepositoryID: repoID.String(), CreatedAt: now, CorrelationID: "integration-test"}
	store := NewStore(testEntClient(t, url))
	job, state, err := store.Claim(ctx, message, "worker-a", now, time.Minute)
	if err != nil || state != Claimed {
		t.Fatalf("Claim() job=%#v state=%d err=%v", job, state, err)
	}
	attempt, err := store.StartAttempt(ctx, job, "worker-a", now, 3)
	if err != nil || attempt != 1 {
		t.Fatalf("StartAttempt() attempt=%d err=%v", attempt, err)
	}
	description, language := "updated", "Go"
	remote := githubgateway.Repository{Owner: "owner", Name: "repo", FullName: "owner/repo", Description: &description, Stars: 42, Language: &language, HTMLURL: "https://example.test/owner/repo", FetchedAt: now}
	if err := store.CompleteSuccess(ctx, job, "worker-a", attempt, remote, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	var notes, status string
	var changes int
	if err := pool.QueryRow(ctx, "SELECT notes FROM repositories WHERE id = $1", repoID).Scan(&notes); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT status FROM batch_jobs WHERE id = $1", jobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM repository_changes WHERE repo_id = $1", repoID).Scan(&changes); err != nil {
		t.Fatal(err)
	}
	if notes != "keep this note" || status != "succeeded" || changes != 1 {
		t.Fatalf("notes=%q status=%q changes=%d", notes, status, changes)
	}
	_, state, err = store.Claim(ctx, message, "worker-b", now.Add(time.Second), time.Minute)
	if err != nil || state != ClaimTerminal {
		t.Fatalf("duplicate Claim() state=%d err=%v", state, err)
	}
}

func TestStoreTerminalFailuresCreateSafeDLQAndCompleteBatch(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	batchID := uuid.New()
	if _, err := pool.Exec(ctx, "INSERT INTO batches (id, created_at) VALUES ($1, $2)", batchID, now); err != nil {
		t.Fatal(err)
	}
	job, message := seedWorkerJob(t, ctx, pool, batchID, now)
	defer cleanupWorkerBatch(pool, batchID, []uuid.UUID{job.ID}, []uuid.UUID{job.RepositoryID})

	store := NewStore(testEntClient(t, url))
	claimed, state, err := store.Claim(ctx, message, "worker-a", now, time.Minute)
	if err != nil || state != Claimed {
		t.Fatalf("Claim() state=%d err=%v", state, err)
	}
	attempt, err := store.StartAttempt(ctx, claimed, "worker-a", now, 3)
	if err != nil || attempt != 1 {
		t.Fatalf("StartAttempt() attempt=%d err=%v", attempt, err)
	}
	if err := store.CompleteFailure(ctx, claimed, "worker-a", attempt, message, "github_repo_not_found", now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	var status string
	var completedAt *time.Time
	var payload []byte
	if err := pool.QueryRow(ctx, "SELECT status FROM batch_jobs WHERE id = $1", job.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT completed_at FROM batches WHERE id = $1", batchID).Scan(&completedAt); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT payload FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2", job.ID, kafka.DLQTopic).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var dlq kafka.DLQMessage
	if err := json.Unmarshal(payload, &dlq); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || completedAt == nil || dlq.EventID == message.EventID || dlq.OriginalEventID != message.EventID || dlq.ErrorCode != "github_repo_not_found" {
		t.Fatalf("status=%q completed=%v dlq=%#v", status, completedAt, dlq)
	}
	_, state, err = store.Claim(ctx, message, "worker-b", now.Add(time.Second), time.Minute)
	if err != nil || state != ClaimTerminal {
		t.Fatalf("duplicate Claim() state=%d err=%v", state, err)
	}
}

func TestStoreCompletesBatchWithConcurrentFinalJobs(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	batchID := uuid.New()
	if _, err := pool.Exec(ctx, "INSERT INTO batches (id, created_at) VALUES ($1, $2)", batchID, now); err != nil {
		t.Fatal(err)
	}
	jobA, messageA := seedWorkerJob(t, ctx, pool, batchID, now)
	jobB, messageB := seedWorkerJob(t, ctx, pool, batchID, now)
	defer cleanupWorkerBatch(pool, batchID, []uuid.UUID{jobA.ID, jobB.ID}, []uuid.UUID{jobA.RepositoryID, jobB.RepositoryID})

	store := NewStore(testEntClient(t, url))
	claimedA, stateA, err := store.Claim(ctx, messageA, "worker-a", now, time.Minute)
	if err != nil || stateA != Claimed {
		t.Fatalf("first Claim() state=%d err=%v", stateA, err)
	}
	claimedB, stateB, err := store.Claim(ctx, messageB, "worker-b", now, time.Minute)
	if err != nil || stateB != Claimed {
		t.Fatalf("second Claim() state=%d err=%v", stateB, err)
	}
	attemptA, err := store.StartAttempt(ctx, claimedA, "worker-a", now, 3)
	if err != nil {
		t.Fatal(err)
	}
	attemptB, err := store.StartAttempt(ctx, claimedB, "worker-b", now, 3)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for _, terminal := range []struct {
		job     Job
		owner   string
		attempt int
		message kafka.RefreshMessage
	}{
		{claimedA, "worker-a", attemptA, messageA},
		{claimedB, "worker-b", attemptB, messageB},
	} {
		wait.Add(1)
		go func(terminal struct {
			job     Job
			owner   string
			attempt int
			message kafka.RefreshMessage
		}) {
			defer wait.Done()
			<-start
			errs <- store.CompleteFailure(context.Background(), terminal.job, terminal.owner, terminal.attempt, terminal.message, "github_retry_exhausted", now.Add(time.Millisecond))
		}(terminal)
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var completedAt *time.Time
	if err := pool.QueryRow(ctx, "SELECT completed_at FROM batches WHERE id = $1", batchID).Scan(&completedAt); err != nil {
		t.Fatal(err)
	}
	if completedAt == nil {
		t.Fatal("batch was not completed")
	}
}

func seedWorkerJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, batchID uuid.UUID, now time.Time) (Job, kafka.RefreshMessage) {
	t.Helper()
	repositoryID, jobID := uuid.New(), uuid.New()
	name := "repo-" + repositoryID.String()
	fullName := "owner/" + name
	if _, err := pool.Exec(ctx, `
		INSERT INTO repositories (id, created_at, updated_at, owner, name, full_name, normalized_full_name, description, stars, language, html_url, notes, fetched_at)
		VALUES ($1, $2, $2, 'owner', $3, $4, $4, NULL, 0, NULL, $5, '', $2)`, repositoryID, now, name, fullName, "https://example.test/"+fullName); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO batch_jobs (id, batch_id, repo_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $4)", jobID, batchID, repositoryID, now); err != nil {
		t.Fatal(err)
	}
	message := kafka.RefreshMessage{SchemaVersion: kafka.RefreshSchemaVersion, EventID: uuid.NewString(), BatchJobID: jobID.String(), BatchID: batchID.String(), RepositoryID: repositoryID.String(), CreatedAt: now, CorrelationID: "integration-test"}
	return Job{ID: jobID, BatchID: batchID, RepositoryID: repositoryID}, message
}

func cleanupWorkerBatch(pool *pgxpool.Pool, batchID uuid.UUID, jobIDs, repositoryIDs []uuid.UUID) {
	ctx := context.Background()
	for _, jobID := range jobIDs {
		_, _ = pool.Exec(ctx, "DELETE FROM outbox_events WHERE aggregate_id = $1", jobID)
	}
	for _, repositoryID := range repositoryIDs {
		_, _ = pool.Exec(ctx, "DELETE FROM repository_changes WHERE repo_id = $1", repositoryID)
	}
	_, _ = pool.Exec(ctx, "DELETE FROM batches WHERE id = $1", batchID)
	for _, repositoryID := range repositoryIDs {
		_, _ = pool.Exec(ctx, "DELETE FROM repositories WHERE id = $1", repositoryID)
	}
}

func testEntClient(t *testing.T, url string) *ent.Client {
	t.Helper()
	client, err := ent.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
