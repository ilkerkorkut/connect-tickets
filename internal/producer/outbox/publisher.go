package outbox

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/internal/event/kafka"
)

type Producer interface {
	Publish(context.Context, *sarama.ProducerMessage) (kafka.Delivery, error)
}

type ClaimStore interface {
	ClaimDue(context.Context, string, time.Time, time.Duration, int) ([]Event, error)
	MarkPublished(context.Context, uuid.UUID, string, time.Time) error
	Release(context.Context, uuid.UUID, string, time.Time, time.Time, string) error
}

type Publisher struct {
	store       ClaimStore
	producer    Producer
	owner       string
	poll        time.Duration
	lease       time.Duration
	batchSize   int
	maxAttempts int
	now         func() time.Time
}

func NewPublisher(store ClaimStore, producer Producer, cfg config.OutboxConfig) (*Publisher, error) {
	if store == nil || producer == nil || cfg.PollInterval <= 0 || cfg.PublishLease <= 0 || cfg.BatchSize < 1 || cfg.MaxPublishAttempts < 1 {
		return nil, ErrInvalidEvent
	}
	return &Publisher{store: store, producer: producer, owner: uuid.NewString(), poll: cfg.PollInterval, lease: cfg.PublishLease, batchSize: cfg.BatchSize, maxAttempts: cfg.MaxPublishAttempts, now: time.Now}, nil
}

func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.poll)
	defer ticker.Stop()
	for {
		if _, err := p.RunOnce(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return ctx.Err()
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Publisher) RunOnce(ctx context.Context) (int, error) {
	if p == nil {
		return 0, ErrInvalidEvent
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	events, err := p.store.ClaimDue(ctx, p.owner, p.now().UTC(), p.lease, p.batchSize)
	if err != nil {
		return 0, err
	}
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := p.publish(ctx, event); err != nil {
			return 0, err
		}
	}
	return len(events), nil
}

func (p *Publisher) publish(ctx context.Context, event Event) error {
	record, err := recordFor(event)
	if err != nil {
		return p.release(ctx, event, "invalid_outbox_payload")
	}
	if _, err := p.producer.Publish(ctx, record); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return p.release(ctx, event, "kafka_publish_failed")
	}
	if err := p.store.MarkPublished(ctx, event.ID, p.owner, p.now().UTC()); err != nil {
		return fmt.Errorf("finalize published outbox event: %w", err)
	}
	return nil
}

func (p *Publisher) release(ctx context.Context, event Event, safeError string) error {
	delay := p.retryDelay(event.PublishAttempts)
	now := p.now().UTC()
	if err := p.store.Release(ctx, event.ID, p.owner, now.Add(delay), now, safeError); err != nil {
		return fmt.Errorf("return outbox event to pending: %w", err)
	}
	return nil
}

func (p *Publisher) retryDelay(attempts int) time.Duration {
	step := min(attempts, p.maxAttempts)
	if step < 1 {
		step = 1
	}
	if step > 30 {
		step = 30
	}
	delay := time.Duration(math.Pow(2, float64(step-1))) * p.poll
	if delay > p.lease {
		return p.lease
	}
	return delay
}

func recordFor(event Event) (*sarama.ProducerMessage, error) {
	switch event.EventType {
	case kafka.RefreshTopic:
		message, err := kafka.DecodeRefresh(event.Payload, 0)
		if err != nil || message.EventID != event.EventID || message.RepositoryID != event.PartitionKey {
			return nil, ErrInvalidEvent
		}
		return kafka.RefreshRecord(message, 0)
	case kafka.DLQTopic:
		message, err := kafka.DecodeDLQ(event.Payload, 0)
		if err != nil || message.EventID != event.EventID || message.RepositoryID != event.PartitionKey {
			return nil, ErrInvalidEvent
		}
		return kafka.DLQRecord(message, 0)
	default:
		return nil, ErrInvalidEvent
	}
}
