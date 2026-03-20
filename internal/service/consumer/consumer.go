package consumer

import (
	"context"
	"fmt"
	"movie_processing_service/internal/config"
	"movie_processing_service/internal/domain"
	red "movie_processing_service/internal/infra/redis"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Consumer struct {
	queue    *red.RedisQueue
	producer KafkaProducer
	cfg      config.ConsumerConfig
	buffer   []domain.Movie
	log      *zap.Logger
	mu       sync.Mutex
}

func NewConsumer(queue *red.RedisQueue, producer KafkaProducer, log *zap.Logger, cfg config.ConsumerConfig) *Consumer {
	return &Consumer{
		queue:    queue,
		producer: producer,
		log:      log,
		cfg:      cfg,
		buffer:   make([]domain.Movie, 0),
	}
}

func (c *Consumer) Start(ctx context.Context) {
	go c.consumeLoop(ctx)
	go c.batchLoop(ctx)
}

func (c *Consumer) consumeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.log.Info("consume loop stopped")
			return
		default:
		}

		movie, err := c.queue.TakeFromQueue(ctx)
		if err != nil {
			c.log.Error("take from queue error", zap.Error(err))
			continue
		}

		c.mu.Lock()
		c.buffer = append(c.buffer, movie)
		c.mu.Unlock()
	}
}

func (c *Consumer) batchLoop(ctx context.Context) {
	c.log.Info("batch loop started")
	for {
		select {
		case <-ctx.Done():
			c.log.Info("batch loop stopped")
			return
		default:
		}

		time.Sleep(c.cfg.BatchInterval)
		c.log.Info("batch tick")

		c.mu.Lock()
		batch := c.buffer
		c.buffer = nil
		c.mu.Unlock()

		if len(batch) == 0 {
			continue
		}

		c.log.Info("sending batch to kafka", zap.Int("size", len(batch)))

		err := c.producer.SendBatch(ctx, batch)
		if err != nil {
			c.log.Error("kafka send error", zap.Error(err))
			continue
		}

		for _, m := range batch {
			if err := c.queue.RemoveFromProcessing(ctx, m); err != nil {
				c.log.Error("remove from processing error",
					zap.Int("movie_id", m.ID),
					zap.Error(err),
				)
				continue
			}

			key := fmt.Sprintf("exist:movie:%d", m.ID)

			if err := c.queue.Delete(ctx, key); err != nil {
				c.log.Error("delete dedup key error",
					zap.Int("movie_id", m.ID),
					zap.Error(err),
				)
			}
		}

		c.log.Info("batch processed successfully", zap.Int("size", len(batch)))
	}
}
