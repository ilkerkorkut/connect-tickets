package kafka

import "errors"

var (
	ErrTopicMismatch    = errors.New("kafka topic configuration does not match locked values")
	ErrInvalidConfig    = errors.New("invalid kafka configuration")
	ErrInvalidMessage   = errors.New("invalid kafka message")
	ErrMessageTooLarge  = errors.New("kafka message exceeds configured limit")
	ErrHeaderTooLarge   = errors.New("kafka header exceeds configured limit")
	ErrSchemaVersion    = errors.New("unsupported kafka message schema version")
	ErrPartitionCount   = errors.New("kafka main topic partition count is invalid")
	ErrProducerClosed   = errors.New("kafka producer is closed")
	ErrShutdownTimeout  = errors.New("kafka shutdown timed out")
	ErrInvalidTopic     = errors.New("invalid kafka topic")
	ErrMessageUnsettled = errors.New("kafka message was not settled")
)
