package github

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/internal/client"
	"github.com/ilkerkorkut/connect-tickets/pkg/logging"
	goredis "github.com/redis/go-redis/v9"
)

const (
	repositoryCachePrefix = "github:repo:v1:"
	repositoryLockPrefix  = "github:repo-lock:v1:"
	cooldownKey           = "github:rate-limit:cooldown-until:v1"
)

type Gateway struct {
	upstream     client.GitHub
	store        coordinationStore
	cacheTTL     time.Duration
	lockTTL      time.Duration
	lockWait     time.Duration
	pollInterval time.Duration
	now          func() time.Time
	jitter       func(time.Duration) time.Duration
	sleep        func(context.Context, time.Duration) error
	newToken     func() (string, error)
}

func NewGateway(
	upstream client.GitHub,
	redisClient *goredis.Client,
	cacheConfig config.CacheConfig,
	githubTimeout time.Duration,
) (*Gateway, error) {
	if upstream == nil || redisClient == nil || cacheConfig.TTL <= 0 || cacheConfig.LockTTL <= githubTimeout || cacheConfig.LockWait <= 0 || githubTimeout <= 0 {
		return nil, ErrInvalidConfig
	}
	return newGateway(upstream, redisStore{client: redisClient}, cacheConfig), nil
}

func newGateway(
	upstream client.GitHub,
	store coordinationStore,
	cacheConfig config.CacheConfig,
) *Gateway {
	return &Gateway{
		upstream:     upstream,
		store:        store,
		cacheTTL:     cacheConfig.TTL,
		lockTTL:      cacheConfig.LockTTL,
		lockWait:     cacheConfig.LockWait,
		pollInterval: 50 * time.Millisecond,
		now:          time.Now,
		jitter:       defaultJitter,
		sleep:        sleepContext,
		newToken:     randomToken,
	}
}

func (g *Gateway) FetchRepository(ctx context.Context, owner, name string) (Repository, error) {
	return g.fetch(ctx, owner, name, false)
}

func (g *Gateway) RefreshRepository(ctx context.Context, owner, name string) (Repository, error) {
	if g == nil {
		return Repository{}, requestError(ErrInvalidConfig, 0, RateLimit{}, "gateway is not initialized", nil)
	}

	key, err := repositoryKey(owner, name)
	if err != nil {
		return Repository{}, err
	}
	if err := g.store.Delete(ctx, key); err != nil {
		return Repository{}, coordinationError(err)
	}

	return g.fetch(ctx, owner, name, true)
}

func (g *Gateway) fetch(
	ctx context.Context,
	owner, name string,
	refreshed bool,
) (Repository, error) {
	if g == nil || g.upstream == nil || g.store == nil {
		return Repository{}, requestError(ErrInvalidConfig, 0, RateLimit{}, "gateway is not initialized", nil)
	}

	if err := ctx.Err(); err != nil {
		return Repository{}, cancellationError(err)
	}

	key, err := repositoryKey(owner, name)
	if err != nil {
		return Repository{}, err
	}

	if repository, hit, err := g.cacheGet(ctx, key); err != nil {
		return Repository{}, err
	} else if hit && !refreshed {
		logging.Debug(ctx, "github cache hit", "owner", owner, "name", name)
		return repository, nil
	}
	logging.Debug(ctx, "github cache miss", "owner", owner, "name", name, "refreshed", refreshed)

	deadline := g.now().Add(g.lockWait)
	for {
		if err := ctx.Err(); err != nil {
			return Repository{}, cancellationError(err)
		}
		if repository, hit, err := g.cacheGet(ctx, key); err != nil {
			return Repository{}, err
		} else if hit {
			logging.Debug(ctx, "github cache hit", "owner", owner, "name", name)
			return repository, nil
		}
		if err := g.checkCooldown(ctx); err != nil {
			return Repository{}, err
		}

		token, err := g.newToken()
		if err != nil {
			return Repository{}, coordinationError(err)
		}
		acquired, err := g.store.SetNX(ctx, lockKey(key), token, g.lockTTL)
		if err != nil {
			return Repository{}, coordinationError(err)
		}
		if acquired {
			logging.Debug(ctx, "github cache lock acquired", "owner", owner, "name", name)
			return g.fetchWithLock(ctx, owner, name, key, token)
		}
		if !g.now().Before(deadline) {
			return Repository{}, coordinationError(errors.New("cache lock wait exceeded"))
		}
		logging.Debug(ctx, "github cache lock waiting", "owner", owner, "name", name)
		if err := g.sleep(ctx, g.lockWaitStep(deadline)); err != nil {
			return Repository{}, cancellationError(err)
		}
	}
}

func (g *Gateway) lockWaitStep(deadline time.Time) time.Duration {
	wait := g.pollInterval + g.jitter(g.pollInterval/2)
	if remaining := deadline.Sub(g.now()); wait > remaining {
		return remaining
	}
	return wait
}

func (g *Gateway) fetchWithLock(
	ctx context.Context,
	owner, name, key, token string,
) (Repository, error) {
	defer func() { _ = g.store.CompareAndDelete(ctx, lockKey(key), token) }()

	if repository, hit, err := g.cacheGet(ctx, key); err != nil {
		return Repository{}, err
	} else if hit {
		return repository, nil
	}

	if err := g.checkCooldown(ctx); err != nil {
		return Repository{}, err
	}

	repository, err := g.upstream.FetchRepository(ctx, owner, name)
	if err != nil {
		logging.Debug(ctx, "github upstream fetch failed", "owner", owner, "name", name, "error_kind", gatewayErrorKind(err))
		if cooldownErr := g.RecordCooldown(ctx, err); cooldownErr != nil {
			return Repository{}, cooldownErr
		}
		return Repository{}, err
	}
	logging.Debug(ctx, "github upstream fetch succeeded", "owner", owner, "name", name, "stars", repository.Stars)

	payload, err := json.Marshal(repository)
	if err != nil {
		return Repository{}, requestError(ErrInvalidResponse, 0, RateLimit{}, "encode repository cache entry", err)
	}
	if err := g.store.Set(ctx, key, string(payload), g.cacheTTL); err != nil {
		return Repository{}, coordinationError(err)
	}
	logging.Debug(ctx, "github cache stored", "owner", owner, "name", name, "ttl", g.cacheTTL)

	return repository, nil
}

func (g *Gateway) cacheGet(
	ctx context.Context,
	key string,
) (Repository, bool, error) {
	payload, err := g.store.Get(ctx, key)
	if errors.Is(err, goredis.Nil) {
		return Repository{}, false, nil
	}
	if err != nil {
		return Repository{}, false, coordinationError(err)
	}
	var repository Repository
	if err := json.Unmarshal([]byte(payload), &repository); err != nil {
		if deleteErr := g.store.Delete(ctx, key); deleteErr != nil {
			return Repository{}, false, coordinationError(deleteErr)
		}
		return Repository{}, false, nil
	}
	return repository, true, nil
}

func repositoryKey(owner, name string) (string, error) {
	owner = strings.ToLower(strings.TrimSpace(owner))
	name = strings.ToLower(strings.TrimSpace(name))
	if !validPathSegment(owner) || !validPathSegment(name) {
		return "", requestError(ErrInvalidRequest, 0, RateLimit{}, "owner and name must be non-empty path segments", nil)
	}
	return repositoryCachePrefix + owner + "/" + name, nil
}

func validPathSegment(value string) bool {
	return strings.TrimSpace(value) != "" && !strings.Contains(value, "/")
}

func lockKey(cacheKey string) string {
	return repositoryLockPrefix + strings.TrimPrefix(cacheKey, repositoryCachePrefix)
}

func randomToken() (string, error) {
	bytes := make([]byte, 32)

	if _, err := cryptorand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate cache lock token: %w", err)
	}

	return hex.EncodeToString(bytes), nil
}

func defaultJitter(maxDuration time.Duration) time.Duration {
	if maxDuration <= 0 {
		return 0
	}

	value, err := cryptorand.Int(cryptorand.Reader, new(big.Int).SetInt64(int64(maxDuration)+1))

	if err != nil {
		return 0
	}

	return time.Duration(value.Int64())
}

func sleepContext(
	ctx context.Context,
	duration time.Duration,
) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func coordinationError(cause error) error {
	return requestError(ErrCacheCoordinationUnavailable, 0, RateLimit{}, "redis cache coordination unavailable", cause)
}

func gatewayErrorKind(err error) string {
	switch {
	case errors.Is(err, ErrNotFound):
		return "github_not_found"
	case errors.Is(err, ErrUnauthorized):
		return "github_unauthorized"
	case errors.Is(err, ErrRateLimited):
		return "github_rate_limited"
	case errors.Is(err, ErrServer):
		return "github_server"
	case errors.Is(err, ErrTimeout):
		return "github_timeout"
	case errors.Is(err, ErrNetwork):
		return "github_network"
	case errors.Is(err, ErrInvalidResponse):
		return "github_invalid_response"
	case errors.Is(err, ErrCanceled):
		return "github_canceled"
	case errors.Is(err, ErrCacheCoordinationUnavailable):
		return "cache_unavailable"
	}
	return "unknown"
}
