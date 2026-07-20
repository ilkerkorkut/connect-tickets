package github

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type coordinationStore interface {
	Get(context.Context, string) (string, error)
	Set(context.Context, string, string, time.Duration) error
	SetNX(context.Context, string, string, time.Duration) (bool, error)
	Delete(context.Context, string) error
	CompareAndDelete(context.Context, string, string) error
	SetLater(context.Context, string, time.Time, time.Time) error
}

type redisStore struct {
	client *goredis.Client
}

var compareAndDelete = goredis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

var setLater = goredis.NewScript(`
local existing = redis.call('GET', KEYS[1])
if existing and tonumber(existing) >= tonumber(ARGV[1]) then
  return existing
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return ARGV[1]
`)

func (s redisStore) Get(ctx context.Context, key string) (string, error) {
	return s.client.Get(ctx, key).Result()
}

func (s redisStore) Set(
	ctx context.Context,
	key, value string,
	ttl time.Duration,
) error {
	return s.client.Set(ctx, key, value, ttl).Err()
}

func (s redisStore) SetNX(
	ctx context.Context,
	key, value string,
	ttl time.Duration,
) (bool, error) {
	return s.client.SetNX(ctx, key, value, ttl).Result()
}

func (s redisStore) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

func (s redisStore) CompareAndDelete(ctx context.Context, key, token string) error {
	return compareAndDelete.Run(ctx, s.client, []string{key}, token).Err()
}

func (s redisStore) SetLater(
	ctx context.Context,
	key string,
	until, now time.Time,
) error {
	ttl := until.Sub(now)

	if ttl <= 0 {
		return s.Delete(ctx, key)
	}

	return setLater.Run(ctx, s.client, []string{key}, until.UnixMilli(), ttl.Milliseconds()).Err()
}
