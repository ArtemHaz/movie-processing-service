package repository

import (
	"context"
	"database/sql"
	"fmt"
	"movie_processing_service/internal/domain"
)

type MovieRepository struct {
	db *sql.DB
}

func NewMovieRepository(db *sql.DB) *MovieRepository {
	return &MovieRepository{
		db: db,
	}
}

func (r *MovieRepository) GetMovies(ctx context.Context) ([]domain.Movie, error) {
	query := `SELECT id, title, year
FROM movies
WHERE year >= 2000;`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query movies: %w", err)
	}
	defer rows.Close()
	var movies []domain.Movie
	for rows.Next() {
		var m domain.Movie
		if err := rows.Scan(&m.ID, &m.Title, &m.Year); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		movies = append(movies, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return movies, nil
}
