package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/alexgul25/place-svc/internal/app"
	"github.com/alexgul25/place-svc/internal/config"
	"github.com/alexgul25/place-svc/internal/lib/logger"
)

func main() {
	appCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	cfg, err := config.LoadPlaceService()
	if err != nil {
		slog.Error("failed to load config files", slog.Any("error", err))
		os.Exit(1)
	}

	log := logger.New(cfg.Env)

	application, err := app.New(log, cfg)
	if err != nil {
		slog.Error("failed to init app", slog.Any("error", err))
		os.Exit(1)
	}

	application.Run()

	<-appCtx.Done()

	application.GracefulShutdown()
}
