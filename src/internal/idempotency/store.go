package idempotency

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Store interface {
	Exists(ctx context.Context, key string) bool
	Mark(ctx context.Context, key string)
	UnMark(ctx context.Context, key string)
}

type RedisStore struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisStore(c *redis.Client, ttl time.Duration) *RedisStore {
	return &RedisStore{
		client: c,
		ttl:    ttl,
	}
}

func (s *RedisStore) Exists(ctx context.Context, key string) bool {
	return s.client.Exists(ctx, key).Val() == 1
}

func (s *RedisStore) Mark(ctx context.Context, key string) {
	s.client.SetNX(ctx, key, true, s.ttl)
}

func (s *RedisStore) UnMark(ctx context.Context, key string) {
	s.client.Del(ctx, key)
}
