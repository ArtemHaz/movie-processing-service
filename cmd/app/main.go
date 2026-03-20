package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"movie_processing_service/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	if err := app.Run(ctx); err != nil {
		log.Fatalf("application stopped with error: %v", err)
	}
}