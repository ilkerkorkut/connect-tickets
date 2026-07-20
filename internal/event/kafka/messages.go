package kafka

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

const (
	RefreshSchemaVersion = 1
	DLQSchemaVersion     = 1
	maxHeaderBytes       = 1024
)

type RefreshMessage struct {
	SchemaVersion int       `json:"schema_version"`
	EventID       string    `json:"event_id"`
	BatchJobID    string    `json:"batch_job_id"`
	BatchID       string    `json:"batch_id"`
	RepositoryID  string    `json:"repo_id"`
	Attempt       int       `json:"attempt"`
	CreatedAt     time.Time `json:"created_at"`
	CorrelationID string    `json:"correlation_id"`
}

type DLQMessage struct {
	SchemaVersion   int       `json:"schema_version"`
	EventID         string    `json:"event_id"`
	OriginalEventID string    `json:"original_event_id"`
	BatchJobID      string    `json:"batch_job_id"`
	BatchID         string    `json:"batch_id"`
	RepositoryID    string    `json:"repo_id"`
	Attempt         int       `json:"attempt"`
	FailedAt        time.Time `json:"failed_at"`
	ErrorCode       string    `json:"error_code"`
	CorrelationID   string    `json:"correlation_id"`
}

func EncodeRefresh(message RefreshMessage, maxBytes int) ([]byte, error) {
	if err := validateRefresh(message); err != nil {
		return nil, err
	}
	return marshalBounded(message, maxBytes)
}

func DecodeRefresh(payload []byte, maxBytes int) (RefreshMessage, error) {
	if len(payload) == 0 || (maxBytes > 0 && len(payload) > maxBytes) {
		return RefreshMessage{}, ErrMessageTooLarge
	}
	var message RefreshMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return RefreshMessage{}, ErrInvalidMessage
	}
	if err := validateRefresh(message); err != nil {
		return RefreshMessage{}, err
	}
	return message, nil
}

func EncodeDLQ(message DLQMessage, maxBytes int) ([]byte, error) {
	if err := validateDLQ(message); err != nil {
		return nil, err
	}
	return marshalBounded(message, maxBytes)
}

func DecodeDLQ(payload []byte, maxBytes int) (DLQMessage, error) {
	if len(payload) == 0 || (maxBytes > 0 && len(payload) > maxBytes) {
		return DLQMessage{}, ErrMessageTooLarge
	}
	var message DLQMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return DLQMessage{}, ErrInvalidMessage
	}
	if err := validateDLQ(message); err != nil {
		return DLQMessage{}, err
	}
	return message, nil
}

func RefreshRecord(message RefreshMessage, maxBytes int) (*sarama.ProducerMessage, error) {
	payload, err := EncodeRefresh(message, maxBytes)
	if err != nil {
		return nil, err
	}
	return producerRecord(RefreshTopic, message.RepositoryID, payload, refreshHeaders(message))
}

func DLQRecord(message DLQMessage, maxBytes int) (*sarama.ProducerMessage, error) {
	payload, err := EncodeDLQ(message, maxBytes)
	if err != nil {
		return nil, err
	}
	return producerRecord(DLQTopic, message.RepositoryID, payload, dlqHeaders(message))
}

func producerRecord(topic, repositoryID string, payload []byte, headers []sarama.RecordHeader) (*sarama.ProducerMessage, error) {
	if topic != RefreshTopic && topic != DLQTopic {
		return nil, ErrInvalidTopic
	}
	if err := validateHeaders(headers); err != nil {
		return nil, err
	}
	return &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(repositoryID), Value: sarama.ByteEncoder(payload), Headers: headers}, nil
}

func Headers(message *sarama.ConsumerMessage) map[string]string {
	values := make(map[string]string, len(message.Headers))
	for _, header := range message.Headers {
		if header == nil {
			continue
		}
		values[string(header.Key)] = string(header.Value)
	}
	return values
}

func validateRefresh(message RefreshMessage) error {
	if message.SchemaVersion != RefreshSchemaVersion {
		return ErrSchemaVersion
	}
	if !validUUID(message.EventID) || !validUUID(message.BatchJobID) || !validUUID(message.BatchID) || !validUUID(message.RepositoryID) || message.Attempt < 0 || message.CreatedAt.IsZero() || strings.TrimSpace(message.CorrelationID) == "" {
		return ErrInvalidMessage
	}
	return nil
}

func validateDLQ(message DLQMessage) error {
	if message.SchemaVersion != DLQSchemaVersion {
		return ErrSchemaVersion
	}
	if !validUUID(message.EventID) || !validUUID(message.OriginalEventID) || !validUUID(message.BatchJobID) || !validUUID(message.BatchID) || !validUUID(message.RepositoryID) || message.Attempt < 0 || message.FailedAt.IsZero() || strings.TrimSpace(message.ErrorCode) == "" || strings.TrimSpace(message.CorrelationID) == "" {
		return ErrInvalidMessage
	}
	return nil
}

func validUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}

func marshalBounded(message any, maxBytes int) ([]byte, error) {
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshal kafka message: %w", err)
	}
	if maxBytes > 0 && len(payload) > maxBytes {
		return nil, ErrMessageTooLarge
	}
	return payload, nil
}

func refreshHeaders(message RefreshMessage) []sarama.RecordHeader {
	return identityHeaders(message.EventID, message.BatchJobID, message.BatchID, message.RepositoryID, message.CorrelationID, message.SchemaVersion)
}

func dlqHeaders(message DLQMessage) []sarama.RecordHeader {
	return identityHeaders(message.EventID, message.BatchJobID, message.BatchID, message.RepositoryID, message.CorrelationID, message.SchemaVersion)
}

func identityHeaders(eventID, batchJobID, batchID, repositoryID, correlationID string, version int) []sarama.RecordHeader {
	return []sarama.RecordHeader{
		{Key: []byte("x-event-id"), Value: []byte(eventID)},
		{Key: []byte("x-batch-job-id"), Value: []byte(batchJobID)},
		{Key: []byte("x-batch-id"), Value: []byte(batchID)},
		{Key: []byte("x-repo-id"), Value: []byte(repositoryID)},
		{Key: []byte("x-correlation-id"), Value: []byte(correlationID)},
		{Key: []byte("x-schema-version"), Value: []byte(strconv.Itoa(version))},
	}
}

func validateHeaders(headers []sarama.RecordHeader) error {
	total := 0
	for _, header := range headers {
		if len(header.Key) == 0 || len(header.Key)+len(header.Value) > maxHeaderBytes {
			return ErrHeaderTooLarge
		}
		total += len(header.Key) + len(header.Value)
		if total > maxHeaderBytes {
			return ErrHeaderTooLarge
		}
	}
	return nil
}
