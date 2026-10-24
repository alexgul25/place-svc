package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	grpcapp "github.com/alexgul25/place-svc/internal/app/grpc"
	"github.com/alexgul25/place-svc/internal/config"
	"github.com/alexgul25/place-svc/internal/infrastructure/kafka"
	"github.com/alexgul25/place-svc/internal/infrastructure/serializer"
	"github.com/alexgul25/place-svc/internal/outbox"
	placelogic "github.com/alexgul25/place-svc/internal/service/place"
	"github.com/alexgul25/place-svc/internal/storage/postgresql"
	"github.com/alexgul25/place-svc/internal/storage/redis"
)

type App struct {
	log        *slog.Logger
	grpcServer *grpcapp.ServerApp
	processor  *outbox.ProcessorWithSyncProducer
	storage    io.Closer
	producer   io.Closer
	cache      io.Closer
}

// New собирает приложение и проверяет соединение с БД. Если ctx отменяется
// во время проверки (например, по SIGTERM), New возвращает ошибку.
func New(
	ctx context.Context,
	log *slog.Logger,
	cfg *config.Config,
) (*App, error) {
	storage, err := postgresql.NewStorage(cfg.Database.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to init storage: %w", err)
	}

	if err := storage.Ping(ctx); err != nil {
		storage.Close()
		return nil, fmt.Errorf("failed to ping storage: %w", err)
	}

	outboxStorage := postgresql.NewOutboxStorage(storage.DB(), serializer.JSONSerializer{})

	syncProducer, err := kafka.NewSyncProducer(cfg.KafkaProducer.Brokers, cfg.ServiceName, cfg.KafkaProducer.SendTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to init kafka sync producer: %w", err)
	}
	log.Info("init kafka sync producer", slog.String("brokers", cfg.KafkaProducer.BrokersRaw))

	processor := outbox.NewProcessorWithSyncProducer(
		outboxStorage,
		syncProducer,
		log,
		cfg.OutboxProcessor.OpTimeout,
		cfg.OutboxProcessor.SelectInterval,
		cfg.OutboxProcessor.SelectSize,
	)
	log.Info("init outbox processor with sync producer")

	placeCache, err := redis.NewPlaceCache(
		cfg.RedisCache.Addr,
		cfg.RedisCache.Password,
		cfg.RedisCache.Username,
		cfg.RedisCache.DB,
		cfg.RedisCache.DialTimeout,
		cfg.RedisCache.ReadTimeout,
		cfg.RedisCache.WriteTimeout,
		serializer.JSONSerializer{},
		cfg.RedisCache.TTL,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to init place cache: %w", err)
	}
	log.Info("init place cache")

	placeStorage := postgresql.NewPlaceStorage(storage.DB(), outboxStorage)

	placeLogic := placelogic.New(log, placeStorage, placeCache)

	serverApp := grpcapp.New(log, placeLogic, cfg.GRPCServer.Port)
	log.Info("init server app")

	return &App{
		log:        log,
		grpcServer: serverApp,
		processor:  processor,
		storage:    storage,
		producer:   syncProducer,
		cache:      placeCache,
	}, nil
}

func (a *App) close() error {
	return errors.Join(
		a.cache.Close(),
		a.producer.Close(),
		a.storage.Close(),
	)
}

// Run запускает outbox-процессор и обслуживает gRPC-запросы до остановки сервера.
// Ошибка означает, что сервер не смог запуститься или аварийно остановился
// (например, порт занят). После GracefulShutdown Run возвращает nil.
func (a *App) Run() error {
	const op = "App.Run"

	a.log.Info("starting app", slog.String("source", op))

	go a.processor.Start()

	return a.grpcServer.Run()
}

// GracefulShutdown останавливает gRPC-сервер и outbox-процессор и закрывает соединения
// с Redis, Kafka и БД, но не дольше, чем живёт ctx. Если время вышло (например, процессор
// ждёт ответа зависшей Kafka, а sarama прервать нельзя), GracefulShutdown возвращается,
// не дожидаясь остального: процесс завершится, а неотправленные записи останутся в outbox.
func (a *App) GracefulShutdown(ctx context.Context) {
	const op = "App.GracefulShutdown"

	log := a.log.With(slog.String("source", op))
	log.Info("app is shutting down gracefully")

	done := make(chan struct{})
	go func() {
		defer close(done)

		a.grpcServer.GracefulStop(ctx)
		a.processor.Shutdown()
		if err := a.close(); err != nil {
			log.Error("failed to close app components", slog.Any("error", err))
		}
	}()

	select {
	case <-done:
		log.Info("app has shut down gracefully")
	case <-ctx.Done():
		log.Warn("graceful shutdown timed out, exiting without waiting for the outbox processor and connections")
	}
}
