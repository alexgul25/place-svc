package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/alexgul25/place-svc/internal/app"
	"github.com/alexgul25/place-svc/internal/config"
	"github.com/alexgul25/place-svc/internal/lib/logger"
)

func main() {
	if err := run(); err != nil {
		slog.Error("service failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	appCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	cfg, err := config.LoadPlaceService()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	log := logger.New(cfg.Env)

	application, err := app.New(appCtx, log, cfg)
	if err != nil {
		return fmt.Errorf("failed to init app: %w", err)
	}

	application.Run()

	<-appCtx.Done()

	application.GracefulShutdown()

	return nil
}
