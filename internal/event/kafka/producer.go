package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ilkerkorkut/connect-tickets/config"
)

type Producer struct {
	producer   asyncProducer
	maxBytes   int
	closeOnce  sync.Once
	closeDone  chan struct{}
	closeError error
}

type Delivery struct {
	Partition int32
	Offset    int64
}

type deliveryResult struct {
	delivery Delivery
	err      error
}

type pendingDelivery struct {
	result chan deliveryResult
}

type asyncProducer interface {
	Input() chan<- *sarama.ProducerMessage
	Successes() <-chan *sarama.ProducerMessage
	Errors() <-chan *sarama.ProducerError
	AsyncClose()
}

func NewProducer(cfg config.KafkaConfig, saramaConfig *sarama.Config) (*Producer, error) {
	producer, err := sarama.NewAsyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	return newProducer(producer, int(cfg.ProducerMaxBytes)), nil
}

func newProducer(producer asyncProducer, maxBytes int) *Producer {
	p := &Producer{producer: producer, maxBytes: maxBytes, closeDone: make(chan struct{})}

	go p.collectDeliveries()

	return p
}

func (p *Producer) PublishRefresh(ctx context.Context, message RefreshMessage) (Delivery, error) {
	if p == nil {
		return Delivery{}, ErrProducerClosed
	}
	record, err := RefreshRecord(message, p.maxBytes)
	if err != nil {
		return Delivery{}, err
	}
	return p.Publish(ctx, record)
}

func (p *Producer) PublishDLQ(ctx context.Context, message DLQMessage) (Delivery, error) {
	if p == nil {
		return Delivery{}, ErrProducerClosed
	}
	record, err := DLQRecord(message, p.maxBytes)
	if err != nil {
		return Delivery{}, err
	}
	return p.Publish(ctx, record)
}

func (p *Producer) Publish(ctx context.Context, record *sarama.ProducerMessage) (Delivery, error) {
	if p == nil || p.producer == nil || record == nil {
		return Delivery{}, ErrProducerClosed
	}
	if err := validateRecord(record, p.maxBytes); err != nil {
		return Delivery{}, err
	}
	pending := &pendingDelivery{result: make(chan deliveryResult, 1)}
	record.Metadata = pending
	select {
	case <-p.closeDone:
		return Delivery{}, ErrProducerClosed
	case <-ctx.Done():
		return Delivery{}, ctx.Err()
	case p.producer.Input() <- record:
	}
	select {
	case result := <-pending.result:
		return result.delivery, result.err
	case <-ctx.Done():
		return Delivery{}, ctx.Err()
	case <-p.closeDone:
		select {
		case result := <-pending.result:
			return result.delivery, result.err
		default:
			return Delivery{}, ErrProducerClosed
		}
	}
}

func validateRecord(record *sarama.ProducerMessage, maxBytes int) error {
	if record.Topic != RefreshTopic && record.Topic != DLQTopic {
		return ErrInvalidTopic
	}
	if record.Value == nil {
		return ErrInvalidMessage
	}
	if err := validateHeaders(record.Headers); err != nil {
		return err
	}
	payload, err := record.Value.Encode()
	if err != nil {
		return err
	}
	if len(payload) == 0 || (maxBytes > 0 && len(payload) > maxBytes) {
		return ErrMessageTooLarge
	}
	return nil
}

func (p *Producer) Close(ctx context.Context) error {
	if p == nil || p.producer == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		p.producer.AsyncClose()
	})
	select {
	case <-p.closeDone:
		return p.closeError
	case <-ctx.Done():
		return ErrShutdownTimeout
	}
}

func (p *Producer) Shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.Close(ctx)
}

func (p *Producer) collectDeliveries() {
	successes := p.producer.Successes()
	errors := p.producer.Errors()

	for successes != nil || errors != nil {
		select {
		case message, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			p.complete(message, Delivery{Partition: message.Partition, Offset: message.Offset}, nil)
		case producerError, ok := <-errors:
			if !ok {
				errors = nil
				continue
			}
			if producerError != nil {
				p.complete(producerError.Msg, Delivery{}, producerError.Err)
			}
		}
	}
	close(p.closeDone)
}

func (p *Producer) complete(message *sarama.ProducerMessage, delivery Delivery, err error) {
	if message == nil {
		return
	}

	pending, ok := message.Metadata.(*pendingDelivery)
	if !ok || pending == nil {
		return
	}

	select {
	case pending.result <- deliveryResult{delivery: delivery, err: err}:
	default:
	}
}
