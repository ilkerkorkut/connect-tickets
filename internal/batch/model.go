package batch

import (
	"errors"
	"github.com/google/uuid"
	"time"
)

var ErrNotFound = errors.New("batch not found")

type Status struct {
	ID          uuid.UUID
	Total       int
	Pending     int
	Succeeded   int
	Failed      []Failure
	CompletedAt *time.Time
}
type Failure struct {
	RepositoryID uuid.UUID
	Code         string
	Message      string
}
