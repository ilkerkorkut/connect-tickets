package github

import (
	"context"
	"errors"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func (g *Gateway) RecordCooldown(ctx context.Context, fetchErr error) error {
	until, ok := cooldownUntil(fetchErr, g.now())
	if !ok {
		return nil
	}
	if err := g.store.SetLater(ctx, cooldownKey, until, g.now()); err != nil {
		return coordinationError(err)
	}
	return nil
}

func (g *Gateway) Cooldown(ctx context.Context) (time.Time, error) {
	if g == nil || g.store == nil {
		return time.Time{}, requestError(ErrInvalidConfig, 0, RateLimit{}, "gateway is not initialized", nil)
	}
	value, err := g.store.Get(ctx, cooldownKey)
	if errors.Is(err, goredis.Nil) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, coordinationError(err)
	}
	milliseconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		if deleteErr := g.store.Delete(ctx, cooldownKey); deleteErr != nil {
			return time.Time{}, coordinationError(deleteErr)
		}
		return time.Time{}, nil
	}
	until := time.UnixMilli(milliseconds).UTC()
	if !until.After(g.now()) {
		if err := g.store.Delete(ctx, cooldownKey); err != nil {
			return time.Time{}, coordinationError(err)
		}
		return time.Time{}, nil
	}
	return until, nil
}

func (g *Gateway) WaitForCooldown(ctx context.Context) error {
	until, err := g.Cooldown(ctx)
	if err != nil || until.IsZero() {
		return err
	}
	wait := until.Sub(g.now()) + g.jitter(g.pollInterval)
	if err := g.sleep(ctx, wait); err != nil {
		return cancellationError(err)
	}
	return nil
}

func (g *Gateway) checkCooldown(ctx context.Context) error {
	until, err := g.Cooldown(ctx)
	if err != nil || until.IsZero() {
		return err
	}
	resetAt := until
	return requestError(ErrRateLimited, 0, RateLimit{RetryAfter: until.Sub(g.now()), ResetAt: &resetAt}, "shared github cooldown active", nil)
}

func cooldownUntil(fetchErr error, now time.Time) (time.Time, bool) {
	if !errors.Is(fetchErr, ErrRateLimited) {
		return time.Time{}, false
	}
	var requestError *RequestError
	if !errors.As(fetchErr, &requestError) {
		return time.Time{}, false
	}
	var until time.Time
	if requestError.RateLimit.RetryAfter > 0 {
		until = now.Add(requestError.RateLimit.RetryAfter)
	}
	if resetAt := requestError.RateLimit.ResetAt; resetAt != nil && resetAt.After(until) {
		until = *resetAt
	}
	return until, !until.IsZero() && until.After(now)
}
