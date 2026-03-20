package config

import (
	"fmt"
	"time"

	"github.com/joho/godotenv"

	"github.com/caarlos0/env/v10"
)

type Config struct {
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`

	Postgres Postgres
	Kafka    Kafka
	Redis    Redis
	Producer ProducerConfig
	Consumer ConsumerConfig
}
type Postgres struct {
	Host     string        `env:"DB_HOST,required"`
	Port     int           `env:"DB_PORT,required"`
	User     string        `env:"DB_USER,required"`
	Password string        `env:"DB_PASSWORD,required"`
	Name     string        `env:"DB_NAME,required"`
	Tout     time.Duration `env:"DB_TIMEOUT,required"`
}
type Kafka struct {
	Brokers []string `env:"KAFKA_BROKERS,required"`
	Topic   string   `env:"KAFKA_TOPIC,required"`
}
type Redis struct {
	Addr string `env:"REDIS_ADDR,required"`
}
type ProducerConfig struct {
	PollInterval time.Duration `env:"PRODUCER_POLL_INTERVAL" envDefault:"2s"`
	LockTTL      time.Duration `env:"PRODUCER_LOCK_TTL" envDefault:"20s"`
}

type ConsumerConfig struct {
	BatchInterval time.Duration `env:"CONSUMER_BATCH_INTERVAL" envDefault:"10s"`
}

func Load() (*Config, error) {
	_ = godotenv.Load()
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("error parsing config: %w", err)
	}
	return cfg, nil
}
