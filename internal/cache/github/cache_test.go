package github

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/internal/client"
	goredis "github.com/redis/go-redis/v9"
)

func TestGatewayCoalescesConcurrentCacheMisses(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 20, 10, 0, 0, 0, time.UTC))
	store := newFakeStore(clock)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	gateway := newTestGateway(t, fetchFunc(func(context.Context, string, string) (Repository, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return testRepository("openai", "gpt", 7), nil
	}), store, clock)
	gateway.pollInterval = time.Millisecond
	gateway.jitter = func(time.Duration) time.Duration { return 0 }

	const callers = 20
	results := make(chan error, callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			<-start
			_, err := gateway.FetchRepository(context.Background(), "OpenAI", "GPT")
			results <- err
		}()
	}
	close(start)
	<-started
	close(release)
	for range callers {
		if err := <-results; err != nil {
			t.Fatalf("FetchRepository() error = %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
}

func TestGatewayCacheHitRecheckAndDifferentKeys(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 20, 10, 0, 0, 0, time.UTC))
	store := newFakeStore(clock)
	var calls atomic.Int32
	gateway := newTestGateway(t, fetchFunc(func(_ context.Context, owner, name string) (Repository, error) {
		calls.Add(1)
		return testRepository(owner, name, 1), nil
	}), store, clock)

	first, err := gateway.FetchRepository(context.Background(), "OpenAI", "GPT")
	if err != nil {
		t.Fatalf("first fetch error = %v", err)
	}
	second, err := gateway.FetchRepository(context.Background(), "openai", "gpt")
	if err != nil {
		t.Fatalf("second fetch error = %v", err)
	}
	if first.FullName != second.FullName || calls.Load() != 1 {
		t.Fatalf("cache result=%#v calls=%d", second, calls.Load())
	}
	if _, err := gateway.FetchRepository(context.Background(), "openai", "other"); err != nil {
		t.Fatalf("different key fetch error = %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("different repository calls = %d, want 2", calls.Load())
	}
	key, _ := repositoryKey("openai", "gpt")
	if ttl := store.ttl(key); ttl != 5*time.Minute {
		t.Fatalf("cache TTL = %s, want 5m", ttl)
	}

	store.onSetNX = func(key, _ string) {
		if key != lockKey(repositoryCachePrefix+"openai/recheck") {
			return
		}
		payload, _ := json.Marshal(testRepository("openai", "recheck", 2))
		_ = store.Set(context.Background(), repositoryCachePrefix+"openai/recheck", string(payload), time.Minute)
	}
	if repository, err := gateway.FetchRepository(context.Background(), "openai", "recheck"); err != nil || repository.Stars != 2 {
		t.Fatalf("cache recheck repository=%#v error=%v", repository, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("cache recheck called upstream %d times", calls.Load())
	}
}

func TestGatewayRefreshCoalescesAndInvalidates(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 20, 10, 0, 0, 0, time.UTC))
	store := newFakeStore(clock)
	key, _ := repositoryKey("openai", "gpt")
	payload, _ := json.Marshal(testRepository("openai", "gpt", 1))
	if err := store.Set(context.Background(), key, string(payload), time.Minute); err != nil {
		t.Fatal(err)
	}
	deletes := make(chan struct{}, 2)
	store.onDelete = func(deleted string) {
		if deleted == key {
			deletes <- struct{}{}
		}
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	gateway := newTestGateway(t, fetchFunc(func(context.Context, string, string) (Repository, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return testRepository("openai", "gpt", 2), nil
	}), store, clock)
	gateway.pollInterval = time.Millisecond
	gateway.jitter = func(time.Duration) time.Duration { return 0 }

	results := make(chan Repository, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			repository, err := gateway.RefreshRepository(context.Background(), "openai", "gpt")
			results <- repository
			errs <- err
		}()
	}
	<-started
	<-deletes
	<-deletes
	close(release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("RefreshRepository() error = %v", err)
		}
		if repository := <-results; repository.Stars != 2 {
			t.Fatalf("refreshed repository = %#v", repository)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("refresh upstream calls = %d, want 1", calls.Load())
	}
}

func TestGatewayCancellationLockExpiryAndOwnershipSafeRelease(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 20, 10, 0, 0, 0, time.UTC))
	store := newFakeStore(clock)
	gateway := newTestGateway(t, fetchFunc(func(context.Context, string, string) (Repository, error) {
		return testRepository("openai", "gpt", 1), nil
	}), store, clock)
	key, _ := repositoryKey("openai", "gpt")
	if _, err := store.SetNX(context.Background(), lockKey(key), "owner", time.Hour); err != nil {
		t.Fatal(err)
	}
	waiting := make(chan struct{})
	gateway.sleep = func(ctx context.Context, _ time.Duration) error {
		close(waiting)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := gateway.FetchRepository(ctx, "openai", "gpt")
		done <- err
	}()
	<-waiting
	cancel()
	err := <-done
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled waiter error = %v", err)
	}

	store = newFakeStore(clock)
	gateway = newTestGateway(t, fetchFunc(func(context.Context, string, string) (Repository, error) {
		return testRepository("openai", "gpt", 3), nil
	}), store, clock)
	gateway.pollInterval = 10 * time.Millisecond
	gateway.sleep = func(context.Context, time.Duration) error {
		clock.Advance(10 * time.Millisecond)
		return nil
	}
	if _, err := store.SetNX(context.Background(), lockKey(key), "dead-owner", 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.FetchRepository(context.Background(), "openai", "gpt"); err != nil {
		t.Fatalf("expired lock recovery error = %v", err)
	}
	if err := store.Set(context.Background(), lockKey(key), "new-owner", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := store.CompareAndDelete(context.Background(), lockKey(key), "old-owner"); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Get(context.Background(), lockKey(key)); err != nil || got != "new-owner" {
		t.Fatalf("old owner deleted newer lock: value=%q error=%v", got, err)
	}
}

func TestGatewayCooldownAndRedisFailurePolicy(t *testing.T) {
	clock := newFakeClock(time.Date(2026, time.July, 20, 10, 0, 0, 0, time.UTC))
	store := newFakeStore(clock)
	gateway := newTestGateway(t, fetchFunc(func(context.Context, string, string) (Repository, error) {
		return Repository{}, errors.New("upstream must not be called")
	}), store, clock)
	if err := gateway.RecordCooldown(context.Background(), &RequestError{Kind: ErrRateLimited, RateLimit: RateLimit{RetryAfter: time.Minute}}); err != nil {
		t.Fatalf("first cooldown error = %v", err)
	}
	if err := gateway.RecordCooldown(context.Background(), &RequestError{Kind: ErrRateLimited, RateLimit: RateLimit{RetryAfter: 2 * time.Minute}}); err != nil {
		t.Fatalf("later cooldown error = %v", err)
	}
	if err := gateway.RecordCooldown(context.Background(), &RequestError{Kind: ErrRateLimited, RateLimit: RateLimit{RetryAfter: 30 * time.Second}}); err != nil {
		t.Fatalf("earlier cooldown error = %v", err)
	}
	until, err := gateway.Cooldown(context.Background())
	if err != nil || !until.Equal(clock.Now().Add(2*time.Minute)) {
		t.Fatalf("cooldown = %s, %v", until, err)
	}
	if _, err := gateway.FetchRepository(context.Background(), "openai", "gpt"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("active cooldown error = %v", err)
	}
	clock.Advance(3 * time.Minute)
	if until, err := gateway.Cooldown(context.Background()); err != nil || !until.IsZero() {
		t.Fatalf("expired cooldown = %s, %v", until, err)
	}

	store.err = errors.New("redis down")
	if _, err := gateway.FetchRepository(context.Background(), "openai", "gpt"); !errors.Is(err, ErrCacheCoordinationUnavailable) {
		t.Fatalf("redis outage error = %v", err)
	}
}

type fetchFunc func(context.Context, string, string) (Repository, error)

func (f fetchFunc) FetchRepository(ctx context.Context, owner, name string) (Repository, error) {
	return f(ctx, owner, name)
}

func (f fetchFunc) RefreshRepository(ctx context.Context, owner, name string) (Repository, error) {
	return f(ctx, owner, name)
}

func (fetchFunc) Cooldown(context.Context) (time.Time, error) {
	return time.Time{}, nil
}

func testRepository(owner, name string, stars int) Repository {
	return Repository{Owner: owner, Name: name, FullName: owner + "/" + name, Stars: stars, HTMLURL: "https://github.com/" + owner + "/" + name, FetchedAt: time.Unix(0, 0).UTC()}
}

func newTestGateway(t *testing.T, upstream client.GitHub, store *fakeStore, clock *fakeClock) *Gateway {
	t.Helper()
	gateway := newGateway(upstream, store, config.CacheConfig{TTL: 5 * time.Minute, LockTTL: time.Second, LockWait: 100 * time.Millisecond})
	gateway.now = clock.Now
	gateway.jitter = func(time.Duration) time.Duration { return 0 }
	return gateway
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type fakeStore struct {
	mu       sync.Mutex
	clock    *fakeClock
	values   map[string]fakeValue
	err      error
	onSetNX  func(string, string)
	onDelete func(string)
}

type fakeValue struct {
	value   string
	expires time.Time
}

func newFakeStore(clock *fakeClock) *fakeStore {
	return &fakeStore{clock: clock, values: make(map[string]fakeValue)}
}

func (s *fakeStore) Get(_ context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return "", s.err
	}
	value, ok := s.value(key)
	if !ok {
		return "", goredis.Nil
	}
	return value.value, nil
}

func (s *fakeStore) Set(_ context.Context, key, value string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.values[key] = fakeValue{value: value, expires: s.clock.Now().Add(ttl)}
	return nil
}

func (s *fakeStore) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return false, s.err
	}
	if _, ok := s.value(key); ok {
		s.mu.Unlock()
		return false, nil
	}
	s.values[key] = fakeValue{value: value, expires: s.clock.Now().Add(ttl)}
	hook := s.onSetNX
	s.mu.Unlock()
	if hook != nil {
		hook(key, value)
	}
	return true, nil
}

func (s *fakeStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return s.err
	}
	delete(s.values, key)
	hook := s.onDelete
	s.mu.Unlock()
	if hook != nil {
		hook(key)
	}
	return nil
}

func (s *fakeStore) CompareAndDelete(_ context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if value, ok := s.value(key); ok && value.value == token {
		delete(s.values, key)
	}
	return nil
}

func (s *fakeStore) SetLater(_ context.Context, key string, until, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if value, ok := s.value(key); ok {
		milliseconds, err := strconv.ParseInt(value.value, 10, 64)
		if err == nil && !until.After(time.UnixMilli(milliseconds)) {
			return nil
		}
	}
	s.values[key] = fakeValue{value: strconv.FormatInt(until.UnixMilli(), 10), expires: until}
	return nil
}

func (s *fakeStore) value(key string) (fakeValue, bool) {
	value, ok := s.values[key]
	if ok && !value.expires.After(s.clock.Now()) {
		delete(s.values, key)
		return fakeValue{}, false
	}
	return value, ok
}

func (s *fakeStore) ttl(key string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.value(key)
	if !ok {
		return 0
	}
	return value.expires.Sub(s.clock.Now())
}
