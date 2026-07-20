package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/ent"
	entbatch "github.com/ilkerkorkut/connect-tickets/ent/batch"
	"github.com/ilkerkorkut/connect-tickets/ent/batchjob"
	"github.com/ilkerkorkut/connect-tickets/ent/outboxevent"
	"github.com/ilkerkorkut/connect-tickets/ent/repository"
	"github.com/ilkerkorkut/connect-tickets/internal/event/kafka"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

type Service struct {
	client *ent.Client
	now    func() time.Time
	newID  func() uuid.UUID
}

func NewService(client *ent.Client) *Service {
	return &Service{client: client, now: time.Now, newID: uuid.New}
}
func (s *Service) CreateRefreshAll(ctx context.Context) (uuid.UUID, error) {
	tx, err := s.client.Tx(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("begin batch transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	repos, err := tx.Client().Repository.Query().Where(repository.DeletedAtIsNil()).All(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("snapshot repositories: %w", err)
	}
	now := s.now().UTC()
	batchID := s.newID()
	create := tx.Client().Batch.Create().SetID(batchID).SetCreatedAt(now)
	if len(repos) == 0 {
		create.SetCompletedAt(now)
	}
	if _, err := create.Save(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("create batch: %w", err)
	}
	correlationID := logging.RequestIDFrom(ctx)
	if correlationID == "" {
		correlationID = batchID.String()
	}
	for _, repo := range repos {
		jobID := s.newID()
		if _, err := tx.Client().BatchJob.Create().SetID(jobID).SetBatchID(batchID).SetRepoID(repo.ID).SetStatus(batchjob.StatusPending).Save(ctx); err != nil {
			return uuid.Nil, fmt.Errorf("create batch job: %w", err)
		}
		eventID := s.newID().String()
		payload, err := refreshPayload(kafka.RefreshMessage{SchemaVersion: kafka.RefreshSchemaVersion, EventID: eventID, BatchJobID: jobID.String(), BatchID: batchID.String(), RepositoryID: repo.ID.String(), Attempt: 0, CreatedAt: now, CorrelationID: correlationID})
		if err != nil {
			return uuid.Nil, err
		}
		if _, err := tx.Client().OutboxEvent.Create().SetID(s.newID()).SetEventID(eventID).SetEventType(kafka.RefreshTopic).SetAggregateID(jobID).SetPartitionKey(repo.ID.String()).SetPayload(payload).SetStatus(outboxevent.StatusPending).SetAvailableAt(now).Save(ctx); err != nil {
			return uuid.Nil, fmt.Errorf("create refresh outbox event: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return uuid.Nil, fmt.Errorf("commit batch: %w", err)
	}
	return batchID, nil
}

func refreshPayload(message kafka.RefreshMessage) (map[string]any, error) {
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode refresh outbox payload: %w", err)
	}
	var result map[string]any
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, fmt.Errorf("decode refresh outbox payload: %w", err)
	}
	return result, nil
}
func (s *Service) Status(ctx context.Context, id uuid.UUID) (Status, error) {
	b, err := s.client.Batch.Query().Where(entbatch.IDEQ(id)).Only(ctx)
	if ent.IsNotFound(err) {
		return Status{}, ErrNotFound
	}
	if err != nil {
		return Status{}, err
	}
	jobs, err := s.client.BatchJob.Query().Where(batchjob.BatchIDEQ(id)).All(ctx)
	if err != nil {
		return Status{}, err
	}
	result := Status{ID: id, CompletedAt: b.CompletedAt, Total: len(jobs)}
	for _, job := range jobs {
		switch job.Status {
		case batchjob.StatusSucceeded:
			result.Succeeded++
		case batchjob.StatusFailed:
			result.Failed = append(result.Failed, Failure{RepositoryID: job.RepoID, Code: job.LastErrorCode, Message: job.LastErrorMessage})
		default:
			result.Pending++
		}
	}
	return result, nil
}
