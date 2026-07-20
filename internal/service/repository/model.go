package repository

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrNotFound      = errors.New("tracked repository not found")
	ErrDuplicate     = errors.New("repository already tracked")
	ErrInvalidInput  = errors.New("invalid repository input")
	ErrInvalidCursor = errors.New("invalid repository cursor")
)

type Repository struct {
	ID          uuid.UUID
	Owner       string
	Name        string
	FullName    string
	Description *string
	Stars       int
	Language    *string
	HTMLURL     string
	Notes       string
	FetchedAt   time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type GitHubRepository struct {
	Owner       string
	Name        string
	FullName    string
	Description *string
	Stars       int
	Language    *string
	HTMLURL     string
	FetchedAt   time.Time
}

type Sort string

const (
	SortStarsDesc   Sort = "stars_desc"
	SortUpdatedDesc Sort = "updated_desc"
)

type ListInput struct {
	Language string
	Sort     Sort
	Limit    int
	Cursor   string
}

type Page struct {
	Repositories []Repository
	NextCursor   string
	HasMore      bool
}

type Statistics struct {
	Total       int
	TotalStars  int64
	TopLanguage *string
}
