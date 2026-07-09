package app

import (
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

func New(
	log *slog.Logger,
	cfg *config.Config,
) (*App, error) {
	storage, err := postgresql.NewStorage(
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.Host,
		cfg.Database.DbName,
		cfg.Database.Port)
	if err != nil {
		return nil, fmt.Errorf("failed to init storage: %w", err)
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

func (a *App) Run() {
	const op = "App.Run"

	a.log.Info("starting app", slog.String("source", op))

	go a.processor.Start()
	go a.grpcServer.MustRun()

	a.log.Info("app is running", slog.String("source", op))
}

func (a *App) GracefulShutdown() {
	const op = "App.GracefulShutdown"

	a.log.Info("app is shutting down gracefully", slog.String("source", op))

	a.grpcServer.GracefulStop()
	a.processor.Shutdown()
	if err := a.close(); err != nil {
		a.log.Error("failed to close app components", slog.String("source", op), slog.Any("error", err))
	}

	a.log.Info("app has shut down gracefully")
}
