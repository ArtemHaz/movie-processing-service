package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"movie_processing_service/internal/config"

	"go.uber.org/zap"

	"github.com/pressly/goose/v3"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func NewPostgres(cfg config.Postgres, log *zap.Logger) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Name,
	)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sql error : %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Tout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres database: %w", err)
	}
	if err := goose.SetDialect("postgres"); err != nil {
		return nil, fmt.Errorf("goose set dialect: %w", err)
	}
	if err := goose.Up(db, "migrations"); err != nil {
		return nil, fmt.Errorf("up migrates: %w", err)
	}

	log.Info("postgres connected and migrations applied")

	return db, nil
}
