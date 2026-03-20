package consumer

import (
	"context"
	"movie_processing_service/internal/domain"
)

type KafkaProducer interface {
	SendBatch(ctx context.Context, movies []domain.Movie) error
}
