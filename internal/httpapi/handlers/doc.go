package handlers

import (
	"context"

	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/internal/batch"
	"github.com/ilkerkorkut/connect-tickets/internal/changefeed"
	"github.com/ilkerkorkut/connect-tickets/internal/repository"
)

type RepositoryService interface {
	Create(context.Context, string, string) (repository.Repository, error)
	Get(context.Context, string) (repository.Repository, error)
	List(context.Context, repository.ListInput) (repository.Page, error)
	UpdateNotes(context.Context, string, string) (repository.Repository, error)
	Delete(context.Context, string) error
	Refresh(context.Context, string) (repository.Repository, error)
	Statistics(context.Context) (repository.Statistics, error)
}

type BatchService interface {
	CreateRefreshAll(context.Context) (uuid.UUID, error)
	Status(context.Context, uuid.UUID) (batch.Status, error)
}

type ChangeService interface {
	List(context.Context, string, int) (changefeed.Page, error)
}

type Handler struct {
	repositories RepositoryService
	batches      BatchService
	changes      ChangeService
}

func New(repositories RepositoryService, batches BatchService, changes ChangeService) *Handler {
	return &Handler{repositories: repositories, batches: batches, changes: changes}
}
