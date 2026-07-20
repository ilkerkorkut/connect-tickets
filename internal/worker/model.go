package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	githubgateway "github.com/ilkerkorkut/connect-tickets/internal/github"
)

var (
	ErrInvalidMessage = errors.New("invalid refresh message")
	ErrClaimLost      = errors.New("worker claim is no longer owned")
)

type ClaimState int

const (
	Claimed ClaimState = iota
	ClaimTerminal
	ClaimDeferred
	ClaimBusy
	ClaimMissing
)

type Job struct {
	ID           uuid.UUID
	BatchID      uuid.UUID
	RepositoryID uuid.UUID
	AttemptCount int
}

type Repository struct {
	ID    uuid.UUID
	Owner string
	Name  string
}

type Gateway interface {
	Cooldown(context.Context) (time.Time, error)
	RefreshRepository(context.Context, string, string) (githubgateway.Repository, error)
}

func invalidRecordID(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "invalid-" + fmt.Sprintf("%x", sum[:])
}
