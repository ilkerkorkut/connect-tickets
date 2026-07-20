package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ilkerkorkut/connect-tickets/internal/platform/config"
)

type Consumer struct {
	group     sarama.ConsumerGroup
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
	errors    chan error
}

type Settlement struct {
	Commit bool
}

type RecordProcessor interface {
	Process(context.Context, *sarama.ConsumerMessage) (Settlement, error)
}

type RebalanceObserver interface {
	Assigned(context.Context, map[string][]int32) error
	Revoked(context.Context, map[string][]int32) error
}

func NewConsumer(cfg config.KafkaConfig, saramaConfig *sarama.Config) (*Consumer, error) {
	group, err := sarama.NewConsumerGroup(cfg.Brokers, ConsumerGroup, saramaConfig)
	if err != nil {
		return nil, err
	}
	return newConsumer(group), nil
}

func newConsumer(group sarama.ConsumerGroup) *Consumer {
	c := &Consumer{group: group, closeDone: make(chan struct{}), errors: make(chan error, 1)}
	go c.forwardErrors()
	return c
}

func (c *Consumer) Consume(ctx context.Context, processor RecordProcessor, observer RebalanceObserver) error {
	if c == nil || c.group == nil || processor == nil {
		return ErrInvalidConfig
	}
	handler := groupHandler{processor: processor, observer: observer}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.group.Consume(ctx, []string{RefreshTopic}, handler); err != nil {
			return err
		}
	}
}

func (c *Consumer) Errors() <-chan error {
	if c == nil {
		return nil
	}
	return c.errors
}

func (c *Consumer) Close(ctx context.Context) error {
	if c == nil || c.group == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		go func() {
			c.closeErr = c.group.Close()
			close(c.closeDone)
		}()
	})
	select {
	case <-c.closeDone:
		return c.closeErr
	case <-ctx.Done():
		return ErrShutdownTimeout
	}
}

func (c *Consumer) Shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.Close(ctx)
}

func (c *Consumer) forwardErrors() {
	defer close(c.errors)
	for err := range c.group.Errors() {
		select {
		case c.errors <- err:
		default:
		}
	}
}

type groupHandler struct {
	processor RecordProcessor
	observer  RebalanceObserver
}

func (h groupHandler) Setup(session sarama.ConsumerGroupSession) error {
	if h.observer == nil {
		return nil
	}
	return h.observer.Assigned(session.Context(), session.Claims())
}

func (h groupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	if h.observer == nil {
		return nil
	}
	return h.observer.Revoked(session.Context(), session.Claims())
}

func (h groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			settlement, err := h.processor.Process(session.Context(), message)
			if err != nil {
				return err
			}
			if settlement.Commit {
				session.MarkMessage(message, "")
				session.Commit()
			}
		}
	}
}
