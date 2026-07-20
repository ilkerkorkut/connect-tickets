package repository

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/ent"
	"github.com/ilkerkorkut/connect-tickets/ent/changesequence"
	"github.com/ilkerkorkut/connect-tickets/ent/repository"
	"github.com/ilkerkorkut/connect-tickets/ent/repositorychange"
	githubgateway "github.com/ilkerkorkut/connect-tickets/internal/github"
)

const (
	maxNotesLength = 4000
	defaultLimit   = 25
	maxLimit       = 100
)

type Gateway interface {
	FetchRepository(context.Context, string, string) (githubgateway.Repository, error)
	RefreshRepository(context.Context, string, string) (githubgateway.Repository, error)
}

type Service struct {
	client *ent.Client
	github Gateway
	now    func() time.Time
	newID  func() uuid.UUID
}

func NewService(client *ent.Client, github Gateway) *Service {
	return &Service{client: client, github: github, now: time.Now, newID: uuid.New}
}

func (s *Service) Create(ctx context.Context, owner, name string) (Repository, error) {
	owner, name, err := normalize(owner, name)
	if err != nil {
		return Repository{}, err
	}
	if s.github == nil {
		return Repository{}, ErrInvalidInput
	}
	remote, err := s.github.FetchRepository(ctx, owner, name)
	if err != nil {
		return Repository{}, err
	}
	return s.createStored(ctx, remote)
}

func (s *Service) Get(ctx context.Context, id string) (Repository, error) {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return Repository{}, ErrInvalidInput
	}
	row, err := s.client.Repository.Query().Where(repository.IDEQ(parsed)).Only(ctx)
	if ent.IsNotFound(err) {
		return Repository{}, ErrNotFound
	}
	if err != nil {
		return Repository{}, fmt.Errorf("query repository: %w", err)
	}
	return fromEnt(row), nil
}

func (s *Service) List(ctx context.Context, input ListInput) (Page, error) {
	if input.Sort == "" {
		input.Sort = SortUpdatedDesc
	}
	if input.Sort != SortStarsDesc && input.Sort != SortUpdatedDesc {
		return Page{}, ErrInvalidInput
	}
	if input.Limit <= 0 {
		input.Limit = defaultLimit
	}
	if input.Limit > maxLimit {
		input.Limit = maxLimit
	}
	input.Language = strings.TrimSpace(input.Language)
	query := s.client.Repository.Query()
	if input.Language != "" {
		query = query.Where(repository.LanguageEQ(input.Language))
	}
	var cursor listCursor
	if input.Cursor != "" {
		var err error
		cursor, err = decodeCursor(input.Cursor)
		if err != nil || cursor.Sort != input.Sort || cursor.Language != input.Language {
			return Page{}, ErrInvalidCursor
		}
		switch input.Sort {
		case SortStarsDesc:
			query = query.Where(repository.Or(repository.StarsLT(cursor.Stars), repository.And(repository.StarsEQ(cursor.Stars), repository.IDLT(cursor.ID))))
		case SortUpdatedDesc:
			query = query.Where(repository.Or(repository.UpdatedAtLT(cursor.UpdatedAt), repository.And(repository.UpdatedAtEQ(cursor.UpdatedAt), repository.IDLT(cursor.ID))))
		}
	}
	switch input.Sort {
	case SortStarsDesc:
		query = query.Order(repository.ByStars(sql.OrderDesc()), repository.ByID(sql.OrderDesc()))
	case SortUpdatedDesc:
		query = query.Order(repository.ByUpdatedAt(sql.OrderDesc()), repository.ByID(sql.OrderDesc()))
	}
	rows, err := query.Limit(input.Limit + 1).All(ctx)
	if err != nil {
		return Page{}, fmt.Errorf("list repositories: %w", err)
	}
	page := Page{HasMore: len(rows) > input.Limit}
	if page.HasMore {
		rows = rows[:input.Limit]
	}
	page.Repositories = make([]Repository, len(rows))
	for i, row := range rows {
		page.Repositories[i] = fromEnt(row)
	}
	if len(rows) > 0 {
		last := rows[len(rows)-1]
		cursor := listCursor{Version: 1, Sort: input.Sort, Language: input.Language, ID: last.ID}
		if input.Sort == SortStarsDesc {
			cursor.Stars = last.Stars
		} else {
			cursor.UpdatedAt = last.UpdatedAt
		}
		page.NextCursor, err = encodeCursor(cursor)
		if err != nil {
			return Page{}, fmt.Errorf("encode repository cursor: %w", err)
		}
	}
	return page, nil
}

func (s *Service) Statistics(ctx context.Context) (Statistics, error) {
	total, err := s.client.Repository.Query().Count(ctx)
	if err != nil {
		return Statistics{}, fmt.Errorf("count repositories: %w", err)
	}
	var totals []struct {
		Total int64 `json:"total"`
	}
	if err := s.client.Repository.Query().Aggregate(ent.As(ent.Sum(repository.FieldStars), "total")).Scan(ctx, &totals); err != nil {
		return Statistics{}, fmt.Errorf("sum repository stars: %w", err)
	}
	result := Statistics{Total: total}
	if len(totals) > 0 {
		result.TotalStars = totals[0].Total
	}
	var languages []struct {
		Language string `json:"language"`
		Count    int    `json:"count"`
	}
	if err := s.client.Repository.Query().Where(repository.LanguageNotNil(), repository.LanguageNEQ("")).GroupBy(repository.FieldLanguage).Aggregate(ent.Count()).Scan(ctx, &languages); err != nil {
		return Statistics{}, fmt.Errorf("group repository languages: %w", err)
	}
	for _, language := range languages {
		if result.TopLanguage == nil || language.Count > languageCount(languages, *result.TopLanguage) || (language.Count == languageCount(languages, *result.TopLanguage) && language.Language < *result.TopLanguage) {
			value := language.Language
			result.TopLanguage = &value
		}
	}
	return result, nil
}

func languageCount(languages []struct {
	Language string `json:"language"`
	Count    int    `json:"count"`
}, target string) int {
	for _, language := range languages {
		if language.Language == target {
			return language.Count
		}
	}
	return 0
}

func (s *Service) UpdateNotes(ctx context.Context, id, notes string) (Repository, error) {
	if len(notes) > maxNotesLength {
		return Repository{}, ErrInvalidInput
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return Repository{}, ErrInvalidInput
	}
	tx, err := s.client.Tx(ctx)
	if err != nil {
		return Repository{}, fmt.Errorf("begin notes transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	row, err := tx.Client().Repository.Query().Where(repository.IDEQ(parsed)).Only(ctx)
	if ent.IsNotFound(err) {
		return Repository{}, ErrNotFound
	}
	if err != nil {
		return Repository{}, fmt.Errorf("query repository: %w", err)
	}
	if row.Notes == notes {
		if err := tx.Commit(); err != nil {
			return Repository{}, fmt.Errorf("commit notes no-op: %w", err)
		}
		return fromEnt(row), nil
	}
	sequence, err := nextSequence(ctx, tx)
	if err != nil {
		return Repository{}, err
	}
	updated, err := tx.Client().Repository.UpdateOneID(parsed).SetNotes(notes).Save(ctx)
	if err != nil {
		return Repository{}, fmt.Errorf("update notes: %w", err)
	}
	if err := insertChange(ctx, tx.Client(), sequence, s.newID(), updated.ID, "notes_updated", snapshot(updated), s.now()); err != nil {
		return Repository{}, err
	}
	if err := tx.Commit(); err != nil {
		return Repository{}, fmt.Errorf("commit notes update: %w", err)
	}
	return fromEnt(updated), nil
}

func (s *Service) Delete(ctx context.Context, id string) error {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return ErrInvalidInput
	}
	tx, err := s.client.Tx(ctx)
	if err != nil {
		return fmt.Errorf("begin deletion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	row, err := tx.Client().Repository.Query().Where(repository.IDEQ(parsed)).Only(ctx)
	if ent.IsNotFound(err) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("query repository: %w", err)
	}
	sequence, err := nextSequence(ctx, tx)
	if err != nil {
		return err
	}
	if err := tx.Client().Repository.DeleteOneID(parsed).Exec(ctx); err != nil {
		return fmt.Errorf("delete repository: %w", err)
	}
	if err := insertChange(ctx, tx.Client(), sequence, s.newID(), parsed, "deleted", snapshot(row), s.now()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit deletion: %w", err)
	}
	return nil
}

func (s *Service) Refresh(ctx context.Context, id string) (Repository, error) {
	local, err := s.Get(ctx, id)
	if err != nil {
		return Repository{}, err
	}
	if s.github == nil {
		return Repository{}, ErrInvalidInput
	}
	remote, err := s.github.RefreshRepository(ctx, local.Owner, local.Name)
	if err != nil {
		return Repository{}, err
	}
	parsed := local.ID
	tx, err := s.client.Tx(ctx)
	if err != nil {
		return Repository{}, fmt.Errorf("begin refresh transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	sequence, err := nextSequence(ctx, tx)
	if err != nil {
		return Repository{}, err
	}
	updated, err := tx.Client().Repository.UpdateOneID(parsed).
		SetOwner(remote.Owner).SetName(remote.Name).SetFullName(remote.FullName).
		SetNormalizedFullName(strings.ToLower(remote.FullName)).SetNillableDescription(remote.Description).
		SetStars(remote.Stars).SetNillableLanguage(remote.Language).SetHTMLURL(remote.HTMLURL).
		SetFetchedAt(remote.FetchedAt).Save(ctx)
	if err != nil {
		return Repository{}, fmt.Errorf("refresh repository: %w", err)
	}
	if err := insertChange(ctx, tx.Client(), sequence, s.newID(), updated.ID, "refreshed", snapshot(updated), s.now()); err != nil {
		return Repository{}, err
	}
	if err := tx.Commit(); err != nil {
		return Repository{}, fmt.Errorf("commit refresh: %w", err)
	}
	return fromEnt(updated), nil
}

func (s *Service) createStored(ctx context.Context, remote githubgateway.Repository) (Repository, error) {
	if _, _, err := normalize(remote.Owner, remote.Name); err != nil || remote.FullName == "" || remote.HTMLURL == "" || remote.Stars < 0 {
		return Repository{}, ErrInvalidInput
	}
	tx, err := s.client.Tx(ctx)
	if err != nil {
		return Repository{}, fmt.Errorf("begin creation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	sequence, err := nextSequence(ctx, tx)
	if err != nil {
		return Repository{}, err
	}
	created, err := tx.Client().Repository.Create().SetID(s.newID()).SetOwner(remote.Owner).SetName(remote.Name).
		SetFullName(remote.FullName).SetNormalizedFullName(strings.ToLower(remote.FullName)).
		SetNillableDescription(remote.Description).SetStars(remote.Stars).SetNillableLanguage(remote.Language).
		SetHTMLURL(remote.HTMLURL).SetFetchedAt(remote.FetchedAt).Save(ctx)
	if ent.IsConstraintError(err) {
		return Repository{}, ErrDuplicate
	}
	if err != nil {
		return Repository{}, fmt.Errorf("create repository: %w", err)
	}
	if err := insertChange(ctx, tx.Client(), sequence, s.newID(), created.ID, "created", snapshot(created), s.now()); err != nil {
		return Repository{}, err
	}
	if err := tx.Commit(); err != nil {
		return Repository{}, fmt.Errorf("commit creation: %w", err)
	}
	return fromEnt(created), nil
}

func nextSequence(ctx context.Context, tx *ent.Tx) (int64, error) {
	row, err := tx.Client().ChangeSequence.Query().Where(changesequence.NameEQ("repository_changes")).Only(ctx)
	if err != nil {
		return 0, fmt.Errorf("lock change sequence: %w", err)
	}
	updated, err := tx.Client().ChangeSequence.UpdateOneID(row.ID).AddLastValue(1).Save(ctx)
	if err != nil {
		return 0, fmt.Errorf("advance change sequence: %w", err)
	}
	return updated.LastValue, nil
}

func insertChange(ctx context.Context, client *ent.Client, sequence int64, eventID, repoID uuid.UUID, operation string, state map[string]any, changedAt time.Time) error {
	if err := client.RepositoryChange.Create().SetID(uuid.New()).SetSequence(sequence).SetEventID(eventID.String()).SetRepoID(repoID).SetOperation(repositorychange.Operation(operation)).SetRepositorySnapshot(state).SetChangedAt(changedAt).Exec(ctx); err != nil {
		return fmt.Errorf("insert repository change: %w", err)
	}
	return nil
}

func normalize(owner, name string) (string, string, error) {
	owner, name = strings.TrimSpace(owner), strings.TrimSpace(name)
	if owner == "" || name == "" || strings.Contains(owner, "/") || strings.Contains(name, "/") {
		return "", "", ErrInvalidInput
	}
	return owner, name, nil
}

func fromEnt(row *ent.Repository) Repository {
	return Repository{ID: row.ID, Owner: row.Owner, Name: row.Name, FullName: row.FullName, Description: stringPointer(row.Description), Stars: row.Stars, Language: stringPointer(row.Language), HTMLURL: row.HTMLURL, Notes: row.Notes, FetchedAt: row.FetchedAt, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}

func snapshot(row *ent.Repository) map[string]any {
	value := fromEnt(row)
	return map[string]any{"id": value.ID.String(), "owner": value.Owner, "name": value.Name, "full_name": value.FullName, "description": value.Description, "stars": value.Stars, "language": value.Language, "html_url": value.HTMLURL, "notes": value.Notes, "fetched_at": value.FetchedAt, "created_at": value.CreatedAt, "updated_at": value.UpdatedAt}
}

type listCursor struct {
	Version   int       `json:"v"`
	Sort      Sort      `json:"s"`
	Language  string    `json:"l"`
	Stars     int       `json:"x,omitempty"`
	UpdatedAt time.Time `json:"u,omitempty"`
	ID        uuid.UUID `json:"i"`
}

func encodeCursor(cursor listCursor) (string, error) {
	bytes, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}
func decodeCursor(value string) (listCursor, error) {
	var cursor listCursor
	bytes, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || json.Unmarshal(bytes, &cursor) != nil || cursor.Version != 1 {
		return listCursor{}, ErrInvalidCursor
	}
	return cursor, nil
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
