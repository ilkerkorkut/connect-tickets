package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/database"
)

type Store struct {
	pool *database.Pool
}

func NewStore(pool *database.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) ClaimDue(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]Event, error) {
	if s == nil || s.pool == nil || owner == "" || lease <= 0 || limit < 1 {
		return nil, ErrInvalidEvent
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT id, event_id, event_type, aggregate_id, partition_key, payload, publish_attempts, available_at
		FROM outbox_events
		WHERE (status = 'pending' AND available_at <= $1)
		   OR (status = 'publishing' AND publishing_lease_until <= $1)
		ORDER BY available_at ASC, created_at ASC, id ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("select due outbox events: %w", err)
	}
	defer rows.Close()
	events := make([]Event, 0, limit)
	for rows.Next() {
		var event Event
		var payload []byte
		if err := rows.Scan(&event.ID, &event.EventID, &event.EventType, &event.AggregateID, &event.PartitionKey, &payload, &event.PublishAttempts, &event.AvailableAt); err != nil {
			return nil, fmt.Errorf("scan due outbox event: %w", err)
		}
		if !json.Valid(payload) {
			return nil, ErrInvalidEvent
		}
		event.Payload = append([]byte(nil), payload...)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due outbox events: %w", err)
	}
	leaseUntil := now.Add(lease)
	for index := range events {
		event := &events[index]
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'publishing', publishing_owner = $2, publishing_lease_until = $3,
				publish_attempts = publish_attempts + 1, updated_at = $1
			WHERE id = $4`, now, owner, leaseUntil, event.ID); err != nil {
			return nil, fmt.Errorf("claim outbox event: %w", err)
		}
		event.PublishAttempts++
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit outbox claim: %w", err)
	}
	return events, nil
}

func (s *Store) MarkPublished(ctx context.Context, id uuid.UUID, owner string, now time.Time) error {
	if s == nil || s.pool == nil || owner == "" {
		return ErrInvalidEvent
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'published', publishing_owner = NULL, publishing_lease_until = NULL,
			published_at = $1, updated_at = $1, last_error = NULL
		WHERE id = $2 AND status = 'publishing' AND publishing_owner = $3
		  AND publishing_lease_until > $1`, now, id, owner)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrClaimLost
	}
	return nil
}

func (s *Store) Release(ctx context.Context, id uuid.UUID, owner string, availableAt, now time.Time, safeError string) error {
	if s == nil || s.pool == nil || owner == "" {
		return ErrInvalidEvent
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'pending', publishing_owner = NULL, publishing_lease_until = NULL,
			available_at = $1, last_error = $2, updated_at = $3
		WHERE id = $4 AND status = 'publishing' AND publishing_owner = $5`, availableAt, safeError, now, id, owner)
	if err != nil {
		return fmt.Errorf("release outbox event: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrClaimLost
	}
	return nil
}
