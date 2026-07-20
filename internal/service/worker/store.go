package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/ent"
	"github.com/ilkerkorkut/connect-tickets/ent/batchjob"
	"github.com/ilkerkorkut/connect-tickets/ent/changesequence"
	"github.com/ilkerkorkut/connect-tickets/ent/jobattempt"
	"github.com/ilkerkorkut/connect-tickets/ent/outboxevent"
	"github.com/ilkerkorkut/connect-tickets/ent/repository"
	"github.com/ilkerkorkut/connect-tickets/ent/repositorychange"

	"github.com/ilkerkorkut/connect-tickets/internal/client"
	"github.com/ilkerkorkut/connect-tickets/internal/event/kafka"
)

type EntStore struct {
	client *ent.Client
}

func NewStore(client *ent.Client) *EntStore {
	return &EntStore{client: client}
}

func (s *EntStore) RecordInvalid(ctx context.Context, eventKey string, payload []byte, code string, now time.Time) error {
	if s == nil || s.client == nil || eventKey == "" || code == "" {
		return ErrInvalidMessage
	}
	err := s.client.InvalidRefreshMessage.Create().SetID(eventKey).SetErrorCode(code).SetPayload(payload).SetRecordedAt(now).Exec(ctx)
	if ent.IsConstraintError(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("record invalid refresh message: %w", err)
	}
	return nil
}

func (s *EntStore) Claim(ctx context.Context, message kafka.RefreshMessage, owner string, now time.Time, lease time.Duration) (Job, ClaimState, error) {
	if s == nil || s.client == nil || owner == "" || lease <= 0 {
		return Job{}, ClaimMissing, ErrInvalidMessage
	}
	jobID, batchID, repositoryID, err := messageIDs(message)
	if err != nil {
		return Job{}, ClaimMissing, err
	}
	tx, err := s.client.Tx(ctx)
	if err != nil {
		return Job{}, ClaimMissing, fmt.Errorf("begin refresh claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	row, err := tx.Client().BatchJob.Query().Where(batchjob.IDEQ(jobID), batchjob.BatchIDEQ(batchID), batchjob.RepoIDEQ(repositoryID)).ForUpdate().Only(ctx)
	if ent.IsNotFound(err) {
		if err := tx.Commit(); err != nil {
			return Job{}, ClaimMissing, fmt.Errorf("commit missing refresh claim: %w", err)
		}
		return Job{}, ClaimMissing, nil
	}
	if err != nil {
		return Job{}, ClaimMissing, fmt.Errorf("load refresh job: %w", err)
	}
	job := jobFromEnt(row)
	state := claimState(row.Status, row.ProcessingOwner, row.LeaseUntil, row.NextAttemptAt, now)
	if state != Claimed {
		if err := tx.Commit(); err != nil {
			return Job{}, ClaimMissing, fmt.Errorf("commit refresh claim decision: %w", err)
		}
		return job, state, nil
	}
	update := tx.Client().BatchJob.UpdateOne(row).SetStatus(batchjob.StatusProcessing).SetProcessingOwner(owner).SetLeaseUntil(now.Add(lease)).SetUpdatedAt(now)
	if row.StartedAt == nil {
		update.SetStartedAt(now)
	}
	if _, err := update.Save(ctx); err != nil {
		return Job{}, ClaimMissing, fmt.Errorf("claim refresh job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Job{}, ClaimMissing, fmt.Errorf("commit refresh claim: %w", err)
	}
	return job, Claimed, nil
}

func claimState(status batchjob.Status, owner string, leaseUntil, nextAttemptAt *time.Time, now time.Time) ClaimState {
	switch status {
	case batchjob.StatusSucceeded, batchjob.StatusFailed:
		return ClaimTerminal
	case batchjob.StatusPending, batchjob.StatusQueued:
		return Claimed
	case batchjob.StatusRetryWait:
		if nextAttemptAt != nil && !nextAttemptAt.After(now) {
			return Claimed
		}
		return ClaimDeferred
	case batchjob.StatusProcessing:
		if owner != "" && leaseUntil != nil && leaseUntil.After(now) {
			return ClaimBusy
		}
		return Claimed
	default:
		return ClaimDeferred
	}
}

func (s *EntStore) LoadRepository(ctx context.Context, job Job) (Repository, error) {
	if s == nil || s.client == nil {
		return Repository{}, ErrInvalidMessage
	}
	row, err := s.client.Repository.Query().Where(repository.IDEQ(job.RepositoryID)).Only(ctx)
	if ent.IsNotFound(err) {
		return Repository{}, ErrRepositoryNotFound
	}
	if err != nil {
		return Repository{}, fmt.Errorf("load tracked repository: %w", err)
	}
	return Repository{ID: row.ID, Owner: row.Owner, Name: row.Name}, nil
}

func (s *EntStore) StartAttempt(ctx context.Context, job Job, owner string, now time.Time, maxAttempts int) (int, error) {
	if s == nil || s.client == nil || owner == "" || maxAttempts < 1 {
		return 0, ErrInvalidMessage
	}
	tx, err := s.client.Tx(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin refresh attempt: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	row, err := tx.Client().BatchJob.Query().Where(batchjob.IDEQ(job.ID)).ForUpdate().Only(ctx)
	if err != nil {
		return 0, claimError(err)
	}
	if !owns(row, owner, now) {
		return 0, ErrClaimLost
	}
	if row.AttemptCount >= maxAttempts {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("commit exhausted refresh attempt: %w", err)
		}
		return 0, nil
	}
	attempt := row.AttemptCount + 1
	if _, err := tx.Client().BatchJob.UpdateOne(row).SetAttemptCount(attempt).SetUpdatedAt(now).Save(ctx); err != nil {
		return 0, fmt.Errorf("count refresh attempt: %w", err)
	}
	if err := tx.Client().JobAttempt.Create().SetID(uuid.New()).SetBatchJobID(job.ID).SetAttemptNumber(attempt).SetWorkerID(owner).SetStartedAt(now).SetResult(jobattempt.ResultRetryScheduled).SetDurationMs(0).Exec(ctx); err != nil {
		return 0, fmt.Errorf("record refresh attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit refresh attempt: %w", err)
	}
	return attempt, nil
}

func (s *EntStore) CompleteSuccess(ctx context.Context, job Job, owner string, attempt int, remote client.Repository, now time.Time) error {
	return s.finish(ctx, job, owner, attempt, now, func(tx *ent.Tx) error {
		fetchedAt := remote.FetchedAt.UTC()
		if fetchedAt.IsZero() {
			fetchedAt = now
		}
		updated, err := tx.Client().Repository.UpdateOneID(job.RepositoryID).SetOwner(remote.Owner).SetName(remote.Name).SetFullName(remote.FullName).SetNormalizedFullName(strings.ToLower(remote.FullName)).SetNillableDescription(remote.Description).SetStars(remote.Stars).SetNillableLanguage(remote.Language).SetHTMLURL(remote.HTMLURL).SetFetchedAt(fetchedAt).SetUpdatedAt(now).Save(ctx)
		if err != nil {
			return fmt.Errorf("refresh repository: %w", err)
		}
		sequence, err := nextSequence(ctx, tx)
		if err != nil {
			return err
		}
		snapshot, err := jsonSnapshot(map[string]any{"id": job.RepositoryID.String(), "owner": remote.Owner, "name": remote.Name, "full_name": remote.FullName, "description": remote.Description, "stars": remote.Stars, "language": remote.Language, "html_url": remote.HTMLURL, "notes": updated.Notes, "fetched_at": fetchedAt})
		if err != nil {
			return fmt.Errorf("encode refreshed repository snapshot: %w", err)
		}
		if err := tx.Client().RepositoryChange.Create().SetID(uuid.New()).SetSequence(sequence).SetEventID(uuid.NewString()).SetRepoID(job.RepositoryID).SetOperation(repositorychange.OperationRefreshed).SetRepositorySnapshot(snapshot).SetChangedAt(now).Exec(ctx); err != nil {
			return fmt.Errorf("create refreshed repository change: %w", err)
		}
		if err := updateJob(tx, job.ID, func(update *ent.BatchJobUpdateOne) {
			update.SetStatus(batchjob.StatusSucceeded).ClearProcessingOwner().ClearLeaseUntil().ClearNextAttemptAt().SetCompletedAt(now).SetUpdatedAt(now)
		}, ctx); err != nil {
			return err
		}
		if err := finishAttempt(ctx, tx, job.ID, attempt, jobattempt.ResultSucceeded, "", now); err != nil {
			return err
		}
		return completeBatch(ctx, tx, job.BatchID, now)
	})
}

func (s *EntStore) CompleteFailure(ctx context.Context, job Job, owner string, attempt int, message kafka.RefreshMessage, code string, now time.Time) error {
	return s.finish(ctx, job, owner, attempt, now, func(tx *ent.Tx) error {
		if err := updateJob(tx, job.ID, func(update *ent.BatchJobUpdateOne) {
			update.SetStatus(batchjob.StatusFailed).ClearProcessingOwner().ClearLeaseUntil().ClearNextAttemptAt().SetLastErrorCode(code).SetLastErrorMessage(safeMessage(code)).SetCompletedAt(now).SetUpdatedAt(now)
		}, ctx); err != nil {
			return err
		}
		if err := finishAttempt(ctx, tx, job.ID, attempt, jobattempt.ResultFailed, code, now); err != nil {
			return err
		}
		if err := insertDLQ(ctx, tx, job, message, attempt, code, now); err != nil {
			return err
		}
		return completeBatch(ctx, tx, job.BatchID, now)
	})
}

func (s *EntStore) ScheduleRetry(ctx context.Context, job Job, owner string, attempt int, message kafka.RefreshMessage, code string, retryAt, now time.Time) error {
	if retryAt.Before(now) {
		retryAt = now
	}
	return s.finish(ctx, job, owner, attempt, now, func(tx *ent.Tx) error {
		if err := updateJob(tx, job.ID, func(update *ent.BatchJobUpdateOne) {
			update.SetStatus(batchjob.StatusRetryWait).ClearProcessingOwner().ClearLeaseUntil().SetNextAttemptAt(retryAt).SetLastErrorCode(code).SetLastErrorMessage(safeMessage(code)).SetUpdatedAt(now)
		}, ctx); err != nil {
			return err
		}
		if err := finishAttempt(ctx, tx, job.ID, attempt, jobattempt.ResultRetryScheduled, code, now); err != nil {
			return err
		}
		retry := message
		retry.EventID = uuid.NewString()
		retry.Attempt = attempt
		retry.CreatedAt = now
		payload, err := jsonSnapshot(retry)
		if err != nil {
			return fmt.Errorf("encode refresh retry: %w", err)
		}
		return tx.Client().OutboxEvent.Create().SetID(uuid.New()).SetEventID(retry.EventID).SetEventType(kafka.RefreshTopic).SetAggregateID(job.ID).SetPartitionKey(job.RepositoryID.String()).SetPayload(payload).SetStatus(outboxevent.StatusPending).SetAvailableAt(retryAt).SetCreatedAt(now).SetUpdatedAt(now).Exec(ctx)
	})
}

func (s *EntStore) finish(ctx context.Context, job Job, owner string, attempt int, now time.Time, apply func(*ent.Tx) error) error {
	if s == nil || s.client == nil || owner == "" {
		return ErrInvalidMessage
	}
	tx, err := s.client.Tx(ctx)
	if err != nil {
		return fmt.Errorf("begin refresh outcome: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	row, err := tx.Client().BatchJob.Query().Where(batchjob.IDEQ(job.ID)).ForUpdate().Only(ctx)
	if err != nil {
		return claimError(err)
	}
	if !owns(row, owner, now) {
		return ErrClaimLost
	}
	if err := apply(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit refresh outcome: %w", err)
	}
	return nil
}

func owns(job *ent.BatchJob, owner string, now time.Time) bool {
	return job.ProcessingOwner == owner && job.LeaseUntil != nil && job.LeaseUntil.After(now)
}

func updateJob(tx *ent.Tx, jobID uuid.UUID, apply func(*ent.BatchJobUpdateOne), ctx context.Context) error {
	update := tx.Client().BatchJob.UpdateOneID(jobID)
	apply(update)
	if err := update.Exec(ctx); err != nil {
		return fmt.Errorf("update refresh job: %w", err)
	}
	return nil
}

func finishAttempt(ctx context.Context, tx *ent.Tx, jobID uuid.UUID, attempt int, result jobattempt.Result, code string, now time.Time) error {
	if attempt == 0 {
		return nil
	}
	row, err := tx.Client().JobAttempt.Query().Where(jobattempt.BatchJobIDEQ(jobID), jobattempt.AttemptNumberEQ(attempt)).Only(ctx)
	if err != nil {
		return claimError(err)
	}
	duration := int(now.Sub(row.StartedAt).Milliseconds())
	if duration < 0 {
		duration = 0
	}
	update := tx.Client().JobAttempt.UpdateOne(row).SetResult(result).SetFinishedAt(now).SetDurationMs(duration)
	if code == "" {
		update.ClearErrorCode()
	} else {
		update.SetErrorCode(code)
	}
	if err := update.Exec(ctx); err != nil {
		return fmt.Errorf("finish refresh attempt: %w", err)
	}
	return nil
}

func completeBatch(ctx context.Context, tx *ent.Tx, batchID uuid.UUID, now time.Time) error {
	pending, err := tx.Client().BatchJob.Query().Where(batchjob.BatchIDEQ(batchID), batchjob.StatusNotIn(batchjob.StatusSucceeded, batchjob.StatusFailed)).Exist(ctx)
	if err != nil {
		return fmt.Errorf("check batch completion: %w", err)
	}
	if pending {
		return nil
	}
	if err := tx.Client().Batch.UpdateOneID(batchID).SetCompletedAt(now).Exec(ctx); err != nil && !ent.IsNotFound(err) {
		return fmt.Errorf("complete refresh batch: %w", err)
	}
	return nil
}

func nextSequence(ctx context.Context, tx *ent.Tx) (int64, error) {
	row, err := tx.Client().ChangeSequence.Query().Where(changesequence.NameEQ("repository_changes")).ForUpdate().Only(ctx)
	if err != nil {
		return 0, fmt.Errorf("lock change sequence: %w", err)
	}
	updated, err := tx.Client().ChangeSequence.UpdateOne(row).AddLastValue(1).Save(ctx)
	if err != nil {
		return 0, fmt.Errorf("advance repository change sequence: %w", err)
	}
	return updated.LastValue, nil
}

func insertDLQ(ctx context.Context, tx *ent.Tx, job Job, message kafka.RefreshMessage, attempt int, code string, now time.Time) error {
	dlq := kafka.DLQMessage{SchemaVersion: kafka.DLQSchemaVersion, EventID: uuid.NewString(), OriginalEventID: message.EventID, BatchJobID: job.ID.String(), BatchID: job.BatchID.String(), RepositoryID: job.RepositoryID.String(), Attempt: attempt, FailedAt: now, ErrorCode: code, CorrelationID: message.CorrelationID}
	payload, err := jsonSnapshot(dlq)
	if err != nil {
		return fmt.Errorf("encode refresh DLQ: %w", err)
	}
	if err := tx.Client().OutboxEvent.Create().SetID(uuid.New()).SetEventID(dlq.EventID).SetEventType(kafka.DLQTopic).SetAggregateID(job.ID).SetPartitionKey(job.RepositoryID.String()).SetPayload(payload).SetStatus(outboxevent.StatusPending).SetAvailableAt(now).SetCreatedAt(now).SetUpdatedAt(now).Exec(ctx); err != nil {
		return fmt.Errorf("create refresh DLQ outbox event: %w", err)
	}
	return nil
}

func jsonSnapshot(value any) (map[string]any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func jobFromEnt(row *ent.BatchJob) Job {
	return Job{ID: row.ID, BatchID: row.BatchID, RepositoryID: row.RepoID, AttemptCount: row.AttemptCount}
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
	if ent.IsNotFound(err) {
		return ErrClaimLost
	}
	return fmt.Errorf("load refresh claim: %w", err)
}

func safeMessage(code string) string {
	return strings.ReplaceAll(code, "_", " ")
}
