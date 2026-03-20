-- +goose Up
CREATE TABLE movies (
    id BIGINT PRIMARY KEY,
    title TEXT NOT NULL,
    year INT NOT NULL
);

-- +goose Down
DROP TABLE movies;