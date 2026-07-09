package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/alexgul25/place-svc/internal/cache"
	"github.com/alexgul25/place-svc/internal/domain"
	"github.com/alexgul25/place-svc/internal/domain/models"
)

type PlaceCache struct {
	client     *goredis.Client
	serializer cache.CacheSerializer
	ttl        time.Duration
}

func NewPlaceCache(
	addr, password, username string, db int, dialTimeout, readTimeout, writeTimeout time.Duration,
	serializer cache.CacheSerializer,
	ttl time.Duration,
) (*PlaceCache, error) {
	const op = "redis.NewPlaceCache"

	client := goredis.NewClient(&goredis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		Username:     username,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &PlaceCache{
		client:     client,
		serializer: serializer,
		ttl:        ttl,
	}, nil
}

func (pc *PlaceCache) Close() error {
	return pc.client.Close()
}

func (pc *PlaceCache) Get(ctx context.Context, userID string) ([]models.Place, error) {
	const op = "redis.PlaceCache.Get"

	strPlaces, err := pc.client.Get(ctx, cache.ToKeyUserPlaces(userID)).Result()
	if err == goredis.Nil {
		return nil, domain.ErrCacheMiss
	} else if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	var places []models.Place
	err = pc.serializer.Unmarshal([]byte(strPlaces), &places)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return places, nil
}

func (pc *PlaceCache) Set(ctx context.Context, userID string, places []models.Place) error {
	const op = "redis.PlaceCache.Set"

	data, err := pc.serializer.Marshal(places)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if err := pc.client.Set(ctx, cache.ToKeyUserPlaces(userID), data, pc.ttl).Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (pc *PlaceCache) Invalidate(ctx context.Context, userID string) error {
	const op = "redis.PlaceCache.Invalidate"

	err := pc.client.Del(ctx, cache.ToKeyUserPlaces(userID)).Err()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
