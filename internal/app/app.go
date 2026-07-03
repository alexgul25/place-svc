package app

import (
	"fmt"
	"log/slog"

	grpcapp "github.com/alexgul25/place-svc/internal/app/grpc"
	placelogic "github.com/alexgul25/place-svc/internal/service/place"
	"github.com/alexgul25/place-svc/internal/storage/postgresql"
)

type StorageCloser interface {
	Close() error
}

type App struct {
	grpcServer    *grpcapp.ServerApp
	storageCloser StorageCloser
}

func New(
	log *slog.Logger,
	grpcPort int,
	dbUser, dbPassword, dbHost, dbName string, dbPort int,
) (*App, error) {
	storage, err := postgresql.NewStorage(dbUser, dbPassword, dbHost, dbName, dbPort)
	if err != nil {
		return nil, fmt.Errorf("failed to init storage: %w", err)
	}

	placeStorage := postgresql.NewPlaceStorage(storage.DB())

	placeLogic := placelogic.New(log, placeStorage)

	serverApp := grpcapp.New(log, placeLogic, grpcPort)

	return &App{
		grpcServer:    serverApp,
		storageCloser: storage,
	}, nil
}

func (a *App) CloseStorage() error {
	return a.storageCloser.Close()
}

func (a *App) RunServer() {
	a.grpcServer.MustRun()
}

func (a *App) GracefulStop() {
	a.grpcServer.GracefulStop()
}
