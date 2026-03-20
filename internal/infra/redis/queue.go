package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"movie_processing_service/internal/domain"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(client *redis.Client) *RedisQueue {
	return &RedisQueue{
		client: client,
	}
}

func (r *RedisQueue) AddToQueue(ctx context.Context, movie domain.Movie) error {
	data, err := json.Marshal(movie)
	if err != nil {

		return fmt.Errorf("marshal error: %w", err)
	}
	err = r.client.LPush(ctx, "movies_queue", data).Err()
	if err != nil {

		return fmt.Errorf("push to redis: %w", err)
	}

	return nil
}
func (r *RedisQueue) TakeFromQueue(ctx context.Context) (domain.Movie, error) {
	var final domain.Movie

	res, err := r.client.BLMove(
		ctx,
		"movies_queue",
		"movies_processing",
		"RIGHT",
		"LEFT",
		0,
	).Result()
	if err != nil {
		return domain.Movie{}, fmt.Errorf("blmove from redis: %w", err)
	}

	if err := json.Unmarshal([]byte(res), &final); err != nil {
		return domain.Movie{}, fmt.Errorf("unmarshal err: %w", err)
	}

	return final, nil
}
func (r *RedisQueue) TryLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	res, err := r.client.SetArgs(ctx, key, 1, redis.SetArgs{
		Mode: "NX",
		TTL:  ttl,
	}).Result()
	if err == redis.Nil {

		return false, nil
	}
	if err != nil {

		return false, fmt.Errorf("set lock: %w", err)
	}

	return res == "OK", nil
}
func (r *RedisQueue) RemoveFromProcessing(ctx context.Context, movie domain.Movie) error {
	data, err := json.Marshal(movie)
	if err != nil {
		return fmt.Errorf("marshal to remove error: %w", err)
	}
	err = r.client.LRem(ctx, "movies_processing", 1, data).Err()
	if err != nil {
		return fmt.Errorf("lrem error: %w", err)
	}
	return nil
}
func (r *RedisQueue) Delete(ctx context.Context, key string) error {
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("delete key: %w", err)
	}
	return nil
}
func (r *RedisQueue) Exists(ctx context.Context, key string) (bool, error) {
	res, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("check key exists: %w", err)
	}
	return res == 1, nil
}
func (r *RedisQueue) Set(ctx context.Context, key string, movie domain.Movie) error {
	data, err := json.Marshal(movie)
	if err != nil {
		return fmt.Errorf("marshal movie: %w", err)
	}
	if err := r.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("set key: %w", err)
	}
	return nil
}
