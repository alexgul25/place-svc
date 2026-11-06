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
	"github.com/alexgul25/place-svc/internal/storage/redis"
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
	// Ошибку запуска main пишет через slog по умолчанию, а go-redis - через свой логгер.
	// Оба направляются в лог сервиса, чтобы в окружении prod все записи были в JSON.
	slog.SetDefault(log)
	redis.SetLogger(log)

	application, err := app.New(appCtx, log, cfg)
	if err != nil {
		// SIGTERM или SIGINT пришёл до того, как сервис запустился: это остановка, а не сбой.
		if appCtx.Err() != nil {
			log.Info("service stopped during startup", slog.Any("reason", err))
			return nil
		}
		return fmt.Errorf("failed to init app: %w", err)
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- application.Run()
	}()

	var runErr error
	select {
	case err := <-serverErr:
		// Сервер не запустился (например, порт занят) или аварийно остановился.
		runErr = fmt.Errorf("grpc server: %w", err)
	case <-appCtx.Done():
	}

	// Outbox-процессор и соединения останавливаются и тогда, когда сервер не запустился.
	stopCtx, cancel := context.WithTimeout(context.Background(), cfg.GRPCServer.GracefulTimeout)
	defer cancel()
	application.GracefulShutdown(stopCtx)

	return runErr
}
