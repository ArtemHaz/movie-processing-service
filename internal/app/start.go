package app

import (
	"context"
	"fmt"

	"movie_processing_service/internal/config"
	"movie_processing_service/internal/infra/kafka"
	"movie_processing_service/internal/infra/postgres"
	red "movie_processing_service/internal/infra/redis"
	"movie_processing_service/internal/logger"
	"movie_processing_service/internal/repository"
	"movie_processing_service/internal/service/consumer"
	"movie_processing_service/internal/service/producer"

	"github.com/redis/go-redis/v9"
)

func Run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	log, err := logger.NewLogger(cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer log.Sync()

	log.Info("starting movie-processing-service")

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.Redis.Addr,
	})
	queue := red.NewRedisQueue(rdb)

	db, err := postgres.NewPostgres(cfg.Postgres, log)
	if err != nil {
		return fmt.Errorf("init postgres: %w", err)
	}
	defer db.Close()

	repo := repository.NewMovieRepository(db)

	prod := producer.New(repo, queue, log,cfg.Producer)
	kafkaProducer := kafka.NewProducer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
	)
	defer kafkaProducer.Close()
	cons := consumer.NewConsumer(queue, kafkaProducer, log,cfg.Consumer)

	// run workers
	go prod.Start(ctx)
	go cons.Start(ctx)

	<-ctx.Done()

	log.Info("shutting down application")

	return nil
}
