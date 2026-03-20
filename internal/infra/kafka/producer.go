package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"movie_processing_service/internal/domain"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
	}

	return &Producer{
		writer: writer,
	}
}
func (p *Producer) SendBatch(ctx context.Context, movies []domain.Movie) error {
	if len(movies) == 0 {
		return nil
	}
	messages := make([]kafka.Message, 0, len(movies))
	for _, m := range movies {
		value, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("marshal movie: %w", err)
		}
		messages = append(messages, kafka.Message{
			Value: value,
		})
	}
	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("write kafka messages: %w", err)
	}
	return nil
}
func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("close kafka writer: %w", err)
	}
	return nil
}
