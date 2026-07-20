package worker

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/google/uuid"
)

var (
	ErrInvalidMessage     = errors.New("invalid refresh message")
	ErrClaimLost          = errors.New("worker claim is no longer owned")
	ErrRepositoryNotFound = errors.New("tracked repository not found")
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

func invalidRecordID(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "invalid-" + fmt.Sprintf("%x", sum[:])
}
