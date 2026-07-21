package changefeed

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ilkerkorkut/connect-tickets/ent"
	"github.com/ilkerkorkut/connect-tickets/ent/repositorychange"
)

const (
	defaultLimit = 25
	maxLimit     = 100
)

var ErrInvalidCursor = errors.New("invalid change cursor")

type Change struct {
	Cursor     string
	EventID    string
	Operation  string
	Repository map[string]any
}

type Page struct {
	Changes    []Change
	NextCursor string
	HasMore    bool
}

type cursor struct {
	Version  int   `json:"v"`
	Sequence int64 `json:"s"`
}

type Store struct{ client *ent.Client }

func NewStore(client *ent.Client) *Store { return &Store{client: client} }

func (s *Store) List(ctx context.Context, since string, limit int) (Page, error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	value, err := decode(since)
	if err != nil {
		return Page{}, err
	}
	rows, err := s.client.RepositoryChange.Query().Where(repositorychange.SequenceGT(value.Sequence)).Order(repositorychange.BySequence()).Limit(limit + 1).All(ctx)
	if err != nil {
		return Page{}, fmt.Errorf("query changes: %w", err)
	}
	page := Page{HasMore: len(rows) > limit}
	if page.HasMore {
		rows = rows[:limit]
	}
	for _, row := range rows {
		encoded, err := encode(cursor{Version: 1, Sequence: row.Sequence})
		if err != nil {
			return Page{}, err
		}
		page.Changes = append(page.Changes, Change{Cursor: encoded, EventID: row.EventID, Operation: string(row.Operation), Repository: row.RepositorySnapshot})
		page.NextCursor = encoded
	}
	return page, nil
}
func encode(value cursor) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
func decode(value string) (cursor, error) {
	if value == "" {
		return cursor{Version: 1}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(value)
	var c cursor
	if err != nil || json.Unmarshal(b, &c) != nil || c.Version != 1 || c.Sequence < 0 {
		return cursor{}, ErrInvalidCursor
	}
	return c, nil
}
