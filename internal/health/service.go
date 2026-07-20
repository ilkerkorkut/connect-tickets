package health

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var ErrNotReady = errors.New("service is not ready")

type Check struct {
	Name    string
	Timeout time.Duration
	Run     func(context.Context) error
}

type Service struct {
	checks []Check
	ready  atomic.Bool
}

func New(checks ...Check) *Service {
	return &Service{checks: append([]Check(nil), checks...)}
}

func (s *Service) SetReady(ready bool) {
	if s != nil {
		s.ready.Store(ready)
	}
}

func (s *Service) Live() bool { return s != nil }

func (s *Service) Ready(ctx context.Context) error {
	if s == nil || !s.ready.Load() {
		return ErrNotReady
	}
	return s.Check(ctx)
}

func (s *Service) Check(ctx context.Context) error {
	if s == nil {
		return ErrNotReady
	}
	for _, check := range s.checks {
		if check.Run == nil || check.Timeout <= 0 {
			return ErrNotReady
		}
		checkCtx, cancel := context.WithTimeout(ctx, check.Timeout)
		err := check.Run(checkCtx)
		cancel()
		if err != nil {
			return ErrNotReady
		}
	}
	return nil
}
