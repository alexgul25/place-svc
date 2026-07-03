package grpcapp

import (
	"fmt"
	"log/slog"
	"net"

	handlersgrpc "github.com/alexgul25/place-svc/internal/grpc/handlers"
	"github.com/alexgul25/place-svc/internal/grpc/interceptors"
	"google.golang.org/grpc"
)

type ServerApp struct {
	log        *slog.Logger
	gRPCServer *grpc.Server
	port       int
}

func New(log *slog.Logger, placeService handlersgrpc.PlaceService, port int) *ServerApp {
	headersToLog := []string{interceptors.HeaderServiceName}
	headersToEnrich := []string{interceptors.HeaderServiceName, interceptors.HeaderUserID}

	gRPCServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		interceptors.NewRecoveryInterceptor(log),
		interceptors.NewLoggingInterceptor(log, headersToLog),
		interceptors.NewContextEnricherInterceptor(headersToEnrich),
	))

	handlersgrpc.Register(gRPCServer, placeService)

	return &ServerApp{
		log:        log,
		gRPCServer: gRPCServer,
		port:       port,
	}
}

func (sa *ServerApp) Run() error {
	const op = "ServerApp.Run"

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", sa.port))
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	sa.log.Info("place grpc server started", slog.String("addr", l.Addr().String()))

	if err := sa.gRPCServer.Serve(l); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (sa *ServerApp) MustRun() {
	if err := sa.Run(); err != nil {
		panic(err)
	}
}

func (sa *ServerApp) GracefulStop() {
	const op = "ServerApp.GracefulStop"

	sa.log.With(slog.String("source", op)).Info("stopping place grpc server", slog.Int("port", sa.port))

	sa.gRPCServer.GracefulStop()

	sa.log.Info("place grpc server gracefully stopped")
}
