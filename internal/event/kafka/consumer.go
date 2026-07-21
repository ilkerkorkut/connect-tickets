package kafka

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
)

type Consumer struct {
	group     sarama.ConsumerGroup
	groupName string
	logger    *slog.Logger
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
	return newConsumer(group, ConsumerGroup), nil
}

func newConsumer(group sarama.ConsumerGroup, groupName string) *Consumer {
	c := &Consumer{group: group, groupName: groupName, closeDone: make(chan struct{}), errors: make(chan error, 1)}
	go c.forwardErrors()
	return c
}

func (c *Consumer) WithLogger(logger *slog.Logger) *Consumer {
	if c != nil {
		c.logger = logger
	}
	return c
}

func (c *Consumer) Consume(ctx context.Context, processor RecordProcessor, observer RebalanceObserver) error {
	if c == nil || c.group == nil || processor == nil {
		return ErrInvalidConfig
	}

	consumeCtx := ctx

	if c.logger != nil {
		consumeCtx = logging.WithConsumerGroup(ctx, c.groupName)
		logging.ScopedLogger(consumeCtx, c.logger).Info("kafka consume started", "topic", RefreshTopic, "consumer_group", c.groupName)
	}

	handler := groupHandler{processor: processor, observer: observer, logger: c.logger, groupName: c.groupName}
	var terminal error
	for {
		if err := consumeCtx.Err(); err != nil {
			terminal = err
			break
		}
		if err := c.group.Consume(consumeCtx, []string{RefreshTopic}, handler); err != nil {
			terminal = err
			break
		}
	}

	if c.logger != nil {
		scoped := logging.ScopedLogger(consumeCtx, c.logger)
		if terminal != nil {
			scoped.Error("kafka consume stopped", "consumer_group", c.groupName, "err", terminal)
		} else {
			scoped.Info("kafka consume stopped", "consumer_group", c.groupName)
		}
	}

	return terminal
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
	logger    *slog.Logger
	groupName string
}

func (h groupHandler) Setup(session sarama.ConsumerGroupSession) error {
	claims := session.Claims()
	h.logRebalance(session.Context(), "kafka session claimed", claims)
	if h.observer == nil {
		return nil
	}
	return h.observer.Assigned(session.Context(), claims)
}

func (h groupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	claims := session.Claims()
	h.logRebalance(session.Context(), "kafka session released", claims)
	if h.observer == nil {
		return nil
	}
	return h.observer.Revoked(session.Context(), claims)
}

func (h groupHandler) logRebalance(ctx context.Context, message string, claims map[string][]int32) {
	if h.logger == nil {
		return
	}
	logging.ScopedLogger(logging.WithConsumerGroup(ctx, h.groupName), h.logger).Info(message, "consumer_group", h.groupName, "claims", claimSummary(claims))
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

func claimSummary(claims map[string][]int32) map[string]int {
	summary := make(map[string]int, len(claims))
	for topic, partitions := range claims {
		summary[topic] = len(partitions)
	}
	return summary
}
