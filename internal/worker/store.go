package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	githubgateway "github.com/ilkerkorkut/connect-tickets/internal/github"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/database"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/kafka"
)

type PostgresStore struct {
	pool *database.Pool
}

func NewStore(pool *database.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) RecordInvalid(ctx context.Context, eventKey string, payload []byte, code string, now time.Time) error {
	if s == nil || s.pool == nil || eventKey == "" || code == "" {
		return ErrInvalidMessage
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO invalid_refresh_messages (event_key, error_code, payload, recorded_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_key) DO NOTHING`, eventKey, code, payload, now)
	if err != nil {
		return fmt.Errorf("record invalid refresh message: %w", err)
	}
	return nil
}

func (s *PostgresStore) Claim(ctx context.Context, message kafka.RefreshMessage, owner string, now time.Time, lease time.Duration) (Job, ClaimState, error) {
	if s == nil || s.pool == nil || owner == "" || lease <= 0 {
		return Job{}, ClaimMissing, ErrInvalidMessage
	}
	jobID, batchID, repositoryID, err := messageIDs(message)
	if err != nil {
		return Job{}, ClaimMissing, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, ClaimMissing, fmt.Errorf("begin refresh claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var job Job
	var status string
	var processingOwner *string
	var leaseUntil, nextAttemptAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT id, batch_id, repo_id, status, attempt_count, processing_owner, lease_until, next_attempt_at
		FROM batch_jobs
		WHERE id = $1 AND batch_id = $2 AND repo_id = $3
		FOR UPDATE`, jobID, batchID, repositoryID).Scan(&job.ID, &job.BatchID, &job.RepositoryID, &status, &job.AttemptCount, &processingOwner, &leaseUntil, &nextAttemptAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return Job{}, ClaimMissing, fmt.Errorf("commit missing refresh claim: %w", err)
		}
		return Job{}, ClaimMissing, nil
	}
	if err != nil {
		return Job{}, ClaimMissing, fmt.Errorf("load refresh job: %w", err)
	}

	state := claimState(status, processingOwner, leaseUntil, nextAttemptAt, now)
	if state != Claimed {
		if err := tx.Commit(ctx); err != nil {
			return Job{}, ClaimMissing, fmt.Errorf("commit refresh claim decision: %w", err)
		}
		return job, state, nil
	}
	result, err := tx.Exec(ctx, `
		UPDATE batch_jobs
		SET status = 'processing', processing_owner = $1, lease_until = $2,
			started_at = COALESCE(started_at, $3), updated_at = $3
		WHERE id = $4`, owner, now.Add(lease), now, job.ID)
	if err != nil {
		return Job{}, ClaimMissing, fmt.Errorf("claim refresh job: %w", err)
	}
	if result.RowsAffected() != 1 {
		return Job{}, ClaimMissing, ErrClaimLost
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, ClaimMissing, fmt.Errorf("commit refresh claim: %w", err)
	}
	return job, Claimed, nil
}

func claimState(status string, owner *string, leaseUntil, nextAttemptAt *time.Time, now time.Time) ClaimState {
	switch status {
	case "succeeded", "failed":
		return ClaimTerminal
	case "pending", "queued":
		return Claimed
	case "retry_wait":
		if nextAttemptAt != nil && !nextAttemptAt.After(now) {
			return Claimed
		}
		return ClaimDeferred
	case "processing":
		if leaseUntil == nil || !leaseUntil.After(now) {
			return Claimed
		}
		if owner != nil {
			return ClaimBusy
		}
		return Claimed
	default:
		return ClaimMissing
	}
}

func (s *PostgresStore) LoadRepository(ctx context.Context, job Job) (Repository, error) {
	if s == nil || s.pool == nil {
		return Repository{}, ErrInvalidMessage
	}
	var repository Repository
	err := s.pool.QueryRow(ctx, `SELECT id, owner, name FROM repositories WHERE id = $1`, job.RepositoryID).Scan(&repository.ID, &repository.Owner, &repository.Name)
	if errors.Is(err, pgx.ErrNoRows) {
		return Repository{}, ErrInvalidMessage
	}
	if err != nil {
		return Repository{}, fmt.Errorf("load tracked repository: %w", err)
	}
	return repository, nil
}

func (s *PostgresStore) StartAttempt(ctx context.Context, job Job, owner string, now time.Time, maxAttempts int) (int, error) {
	if s == nil || s.pool == nil || owner == "" || maxAttempts < 1 {
		return 0, ErrInvalidMessage
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin refresh attempt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var attempts int
	var processingOwner *string
	var leaseUntil *time.Time
	err = tx.QueryRow(ctx, `SELECT attempt_count, processing_owner, lease_until FROM batch_jobs WHERE id = $1 FOR UPDATE`, job.ID).Scan(&attempts, &processingOwner, &leaseUntil)
	if err != nil {
		return 0, claimError(err)
	}
	if processingOwner == nil || *processingOwner != owner || leaseUntil == nil || !leaseUntil.After(now) {
		return 0, ErrClaimLost
	}
	if attempts >= maxAttempts {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit exhausted refresh attempt: %w", err)
		}
		return 0, nil
	}
	attempts++
	if _, err := tx.Exec(ctx, `UPDATE batch_jobs SET attempt_count = $1, updated_at = $2 WHERE id = $3`, attempts, now, job.ID); err != nil {
		return 0, fmt.Errorf("count refresh attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO job_attempts (id, batch_job_id, attempt_number, worker_id, started_at, result, duration_ms)
		VALUES ($1, $2, $3, $4, $5, 'retry_scheduled', 0)`, uuid.New(), job.ID, attempts, owner, now); err != nil {
		return 0, fmt.Errorf("record refresh attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit refresh attempt: %w", err)
	}
	return attempts, nil
}

func (s *PostgresStore) CompleteSuccess(ctx context.Context, job Job, owner string, attempt int, remote githubgateway.Repository, now time.Time) error {
	return s.finish(ctx, job, owner, attempt, now, func(tx pgx.Tx) error {
		var notes string
		fetchedAt := remote.FetchedAt.UTC()
		if fetchedAt.IsZero() {
			fetchedAt = now
		}
		if err := tx.QueryRow(ctx, `
			UPDATE repositories
			SET owner = $1, name = $2, full_name = $3, normalized_full_name = $4,
				description = $5, stars = $6, language = $7, html_url = $8, fetched_at = $9, updated_at = $10
			WHERE id = $11
			RETURNING notes`, remote.Owner, remote.Name, remote.FullName, strings.ToLower(remote.FullName), remote.Description, remote.Stars, remote.Language, remote.HTMLURL, fetchedAt, now, job.RepositoryID).Scan(&notes); err != nil {
			return fmt.Errorf("refresh repository: %w", err)
		}
		var sequence int64
		if err := tx.QueryRow(ctx, `UPDATE change_sequences SET last_value = last_value + 1 WHERE name = 'repository_changes' RETURNING last_value`).Scan(&sequence); err != nil {
			return fmt.Errorf("advance repository change sequence: %w", err)
		}
		snapshot, err := json.Marshal(map[string]any{
			"id": job.RepositoryID.String(), "owner": remote.Owner, "name": remote.Name, "full_name": remote.FullName,
			"description": remote.Description, "stars": remote.Stars, "language": remote.Language, "html_url": remote.HTMLURL,
			"notes": notes, "fetched_at": fetchedAt,
		})
		if err != nil {
			return fmt.Errorf("encode refreshed repository snapshot: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO repository_changes (id, sequence, event_id, repo_id, operation, repository_snapshot, changed_at)
			VALUES ($1, $2, $3, $4, 'refreshed', $5, $6)`, uuid.New(), sequence, uuid.NewString(), job.RepositoryID, snapshot, now); err != nil {
			return fmt.Errorf("record repository refresh change: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE batch_jobs
			SET status = 'succeeded', processing_owner = NULL, lease_until = NULL, next_attempt_at = NULL,
				last_error_code = NULL, last_error_message = NULL, completed_at = $1, updated_at = $1
			WHERE id = $2`, now, job.ID); err != nil {
			return fmt.Errorf("complete refresh job: %w", err)
		}
		if err := finishAttempt(ctx, tx, job.ID, attempt, "succeeded", "", now); err != nil {
			return err
		}
		return completeBatch(ctx, tx, job.BatchID, now)
	})
}

func (s *PostgresStore) CompleteFailure(ctx context.Context, job Job, owner string, attempt int, message kafka.RefreshMessage, code string, now time.Time) error {
	return s.finish(ctx, job, owner, attempt, now, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE batch_jobs
			SET status = 'failed', processing_owner = NULL, lease_until = NULL, next_attempt_at = NULL,
				last_error_code = $1, last_error_message = $2, completed_at = $3, updated_at = $3
			WHERE id = $4`, code, safeMessage(code), now, job.ID); err != nil {
			return fmt.Errorf("fail refresh job: %w", err)
		}
		if err := finishAttempt(ctx, tx, job.ID, attempt, "failed", code, now); err != nil {
			return err
		}
		if err := insertDLQ(ctx, tx, job, message, attempt, code, now); err != nil {
			return err
		}
		return completeBatch(ctx, tx, job.BatchID, now)
	})
}

func (s *PostgresStore) ScheduleRetry(ctx context.Context, job Job, owner string, attempt int, message kafka.RefreshMessage, code string, retryAt, now time.Time) error {
	if retryAt.Before(now) {
		retryAt = now
	}
	return s.finish(ctx, job, owner, attempt, now, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE batch_jobs
			SET status = 'retry_wait', processing_owner = NULL, lease_until = NULL, next_attempt_at = $1,
				last_error_code = $2, last_error_message = $3, updated_at = $4
			WHERE id = $5`, retryAt, code, safeMessage(code), now, job.ID); err != nil {
			return fmt.Errorf("schedule refresh retry: %w", err)
		}
		if err := finishAttempt(ctx, tx, job.ID, attempt, "retry_scheduled", code, now); err != nil {
			return err
		}
		retry := message
		retry.EventID = uuid.NewString()
		retry.Attempt = attempt
		retry.CreatedAt = now
		payload, err := json.Marshal(retry)
		if err != nil {
			return fmt.Errorf("encode refresh retry: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_id, event_type, aggregate_id, partition_key, payload, status, available_at, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $8)`, uuid.New(), retry.EventID, kafka.RefreshTopic, job.ID, job.RepositoryID.String(), payload, retryAt, now); err != nil {
			return fmt.Errorf("create refresh retry outbox event: %w", err)
		}
		return nil
	})
}

func (s *PostgresStore) finish(ctx context.Context, job Job, owner string, attempt int, now time.Time, apply func(pgx.Tx) error) error {
	if s == nil || s.pool == nil || owner == "" {
		return ErrInvalidMessage
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin refresh outcome: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var processingOwner *string
	var leaseUntil *time.Time
	err = tx.QueryRow(ctx, `SELECT processing_owner, lease_until FROM batch_jobs WHERE id = $1 FOR UPDATE`, job.ID).Scan(&processingOwner, &leaseUntil)
	if err != nil {
		return claimError(err)
	}
	if processingOwner == nil || *processingOwner != owner || leaseUntil == nil || !leaseUntil.After(now) {
		return ErrClaimLost
	}
	if err := apply(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit refresh outcome: %w", err)
	}
	return nil
}

func finishAttempt(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, attempt int, result, code string, now time.Time) error {
	if attempt == 0 {
		return nil
	}
	command, err := tx.Exec(ctx, `
		UPDATE job_attempts
		SET result = $1, error_code = NULLIF($2, ''), finished_at = $3,
			duration_ms = GREATEST(0, FLOOR(EXTRACT(EPOCH FROM ($3 - started_at)) * 1000)::integer)
		WHERE batch_job_id = $4 AND attempt_number = $5`, result, code, now, jobID, attempt)
	if err != nil {
		return fmt.Errorf("finish refresh attempt: %w", err)
	}
	if command.RowsAffected() != 1 {
		return ErrClaimLost
	}
	return nil
}

func completeBatch(ctx context.Context, tx pgx.Tx, batchID uuid.UUID, now time.Time) error {
	var lockedBatchID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM batches WHERE id = $1 FOR UPDATE`, batchID).Scan(&lockedBatchID); err != nil {
		return fmt.Errorf("lock refresh batch completion: %w", err)
	}
	_, err := tx.Exec(ctx, `
		UPDATE batches
		SET completed_at = $1
		WHERE id = $2 AND completed_at IS NULL
		  AND NOT EXISTS (SELECT 1 FROM batch_jobs WHERE batch_id = $2 AND status NOT IN ('succeeded', 'failed'))`, now, batchID)
	if err != nil {
		return fmt.Errorf("complete refresh batch: %w", err)
	}
	return nil
}

func insertDLQ(ctx context.Context, tx pgx.Tx, job Job, message kafka.RefreshMessage, attempt int, code string, now time.Time) error {
	dlq := kafka.DLQMessage{
		SchemaVersion: kafka.DLQSchemaVersion, EventID: uuid.NewString(), OriginalEventID: message.EventID, BatchJobID: job.ID.String(), BatchID: job.BatchID.String(),
		RepositoryID: job.RepositoryID.String(), Attempt: attempt, FailedAt: now, ErrorCode: code, CorrelationID: message.CorrelationID,
	}
	payload, err := json.Marshal(dlq)
	if err != nil {
		return fmt.Errorf("encode refresh DLQ: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, event_id, event_type, aggregate_id, partition_key, payload, status, available_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $7, $7)`, uuid.New(), dlq.EventID, kafka.DLQTopic, job.ID, job.RepositoryID.String(), payload, now)
	if err != nil {
		return fmt.Errorf("create refresh DLQ outbox event: %w", err)
	}
	return nil
}

func messageIDs(message kafka.RefreshMessage) (uuid.UUID, uuid.UUID, uuid.UUID, error) {
	jobID, err := uuid.Parse(message.BatchJobID)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, ErrInvalidMessage
	}
	batchID, err := uuid.Parse(message.BatchID)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, ErrInvalidMessage
	}
	repositoryID, err := uuid.Parse(message.RepositoryID)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, ErrInvalidMessage
	}
	return jobID, batchID, repositoryID, nil
}

func claimError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrClaimLost
	}
	return fmt.Errorf("load refresh claim: %w", err)
}

func safeMessage(code string) string {
	return strings.ReplaceAll(code, "_", " ")
}
