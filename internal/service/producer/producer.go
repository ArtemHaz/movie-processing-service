package producer

import (
	"context"
	"fmt"
	"movie_processing_service/internal/config"
	"movie_processing_service/internal/infra/redis"
	"movie_processing_service/internal/repository"
	"time"

	"go.uber.org/zap"
)

type Producer struct {
	repo  *repository.MovieRepository
	queue *redis.RedisQueue
	cfg   config.ProducerConfig
	log   *zap.Logger
}

func New(repo *repository.MovieRepository, queue *redis.RedisQueue, log *zap.Logger, cfg config.ProducerConfig) *Producer {
	return &Producer{
		repo:  repo,
		queue: queue,
		cfg:   cfg,
		log:   log,
	}
}

func (p *Producer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			p.log.Info("producer stopped")
			return
		default:
		}

		movies, err := p.repo.GetMovies(ctx)
		if err != nil {
			p.log.Error("get movies error", zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}
		p.log.Info("movies fetched", zap.Int("count", len(movies)))

		for _, m := range movies {
			lockKey := fmt.Sprintf("lock:movie:%d", m.ID)
			existKey := fmt.Sprintf("exist:movie:%d", m.ID)

			ok, err := p.queue.TryLock(ctx, lockKey, p.cfg.LockTTL)
			if err != nil {
				p.log.Error("lock error",
					zap.Int("movie_id", m.ID),
					zap.Error(err),
				)
				continue
			}
			if !ok {
				continue
			}

			exist, err := p.queue.Exists(ctx, existKey)
			if err != nil {
				p.log.Error("exist check error",
					zap.Int("movie_id", m.ID),
					zap.Error(err),
				)
				continue
			}
			if exist {
				continue
			}

			if err := p.queue.Set(ctx, existKey, m); err != nil {
				p.log.Error("set dedup key error",
					zap.Int("movie_id", m.ID),
					zap.Error(err),
				)
				continue
			}

			if err := p.queue.AddToQueue(ctx, m); err != nil {
				p.log.Error("enqueue error",
					zap.Int("movie_id", m.ID),
					zap.Error(err),
				)
				continue
			}
		}

		time.Sleep(p.cfg.PollInterval)
	}
}
