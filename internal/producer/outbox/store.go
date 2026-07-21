package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/ent"
	"github.com/ilkerkorkut/connect-tickets/ent/outboxevent"
)

type Store struct {
	client *ent.Client
}

func NewStore(client *ent.Client) *Store {
	return &Store{client: client}
}

func (s *Store) ClaimDue(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]Event, error) {
	if s == nil || s.client == nil || owner == "" || lease <= 0 || limit < 1 {
		return nil, ErrInvalidEvent
	}

	tx, err := s.client.Tx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.Client().OutboxEvent.Query().
		Where(
			outboxevent.Or(
				outboxevent.And(
					outboxevent.StatusEQ(outboxevent.StatusPending),
					outboxevent.AvailableAtLTE(now),
				),
				outboxevent.And(
					outboxevent.StatusEQ(outboxevent.StatusPublishing),
					outboxevent.PublishingLeaseUntilLTE(now),
				),
			),
		).
		Order(outboxevent.ByAvailableAt(), outboxevent.ByCreatedAt(), outboxevent.ByID()).
		Limit(limit).
		ForUpdate(entsql.WithLockAction(entsql.SkipLocked)).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("select due outbox events: %w", err)
	}

	events := make([]Event, 0, len(rows))
	leaseUntil := now.Add(lease)
	for _, row := range rows {
		payload, err := json.Marshal(row.Payload)
		if err != nil {
			return nil, fmt.Errorf("encode due outbox event: %w", err)
		}
		if _, err := tx.Client().OutboxEvent.UpdateOne(row).
			SetStatus(outboxevent.StatusPublishing).
			SetPublishingOwner(owner).
			SetPublishingLeaseUntil(leaseUntil).
			AddPublishAttempts(1).
			SetUpdatedAt(now).
			Save(ctx); err != nil {
			return nil, fmt.Errorf("claim outbox event: %w", err)
		}
		events = append(events, Event{ID: row.ID, EventID: row.EventID, EventType: row.EventType, AggregateID: row.AggregateID, PartitionKey: row.PartitionKey, Payload: payload, PublishAttempts: row.PublishAttempts + 1, AvailableAt: row.AvailableAt})
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit outbox claim: %w", err)
	}
	return events, nil
}

func (s *Store) MarkPublished(ctx context.Context, id uuid.UUID, owner string, now time.Time) error {
	if s == nil || s.client == nil || owner == "" {
		return ErrInvalidEvent
	}
	updated, err := s.client.OutboxEvent.Update().
		Where(outboxevent.IDEQ(id), outboxevent.StatusEQ(outboxevent.StatusPublishing), outboxevent.PublishingOwnerEQ(owner), outboxevent.PublishingLeaseUntilGT(now)).
		SetStatus(outboxevent.StatusPublished).
		ClearPublishingOwner().
		ClearPublishingLeaseUntil().
		SetPublishedAt(now).
		SetUpdatedAt(now).
		ClearLastError().
		Save(ctx)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	if updated != 1 {
		return ErrClaimLost
	}
	return nil
}

func (s *Store) Release(
	ctx context.Context,
	id uuid.UUID,
	owner string,
	availableAt, now time.Time,
	safeError string,
) error {
	if s == nil || s.client == nil || owner == "" {
		return ErrInvalidEvent
	}
	updated, err := s.client.OutboxEvent.Update().
		Where(outboxevent.IDEQ(id), outboxevent.StatusEQ(outboxevent.StatusPublishing), outboxevent.PublishingOwnerEQ(owner)).
		SetStatus(outboxevent.StatusPending).
		ClearPublishingOwner().
		ClearPublishingLeaseUntil().
		SetAvailableAt(availableAt).
		SetLastError(safeError).
		SetUpdatedAt(now).Save(ctx)
	if err != nil {
		return fmt.Errorf("release outbox event: %w", err)
	}
	if updated != 1 {
		return ErrClaimLost
	}
	return nil
}
