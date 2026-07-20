package outbox

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrClaimLost    = errors.New("outbox claim is no longer owned")
	ErrInvalidEvent = errors.New("invalid outbox event")
)

type Event struct {
	ID              uuid.UUID
	EventID         string
	EventType       string
	AggregateID     uuid.UUID
	PartitionKey    string
	Payload         []byte
	PublishAttempts int
	AvailableAt     time.Time
}
