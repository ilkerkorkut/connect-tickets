package changefeed

import "context"

type Service struct{ store *Store }

func NewService(store *Store) *Service { return &Service{store: store} }

func (s *Service) List(ctx context.Context, since string, limit int) (Page, error) {
	return s.store.List(ctx, since, limit)
}
