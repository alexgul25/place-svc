package grpcapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	handlersgrpc "github.com/alexgul25/place-svc/internal/grpc/handlers"
	"github.com/alexgul25/place-svc/internal/grpc/interceptors"
)

type ServerApp struct {
	log          *slog.Logger
	gRPCServer   *grpc.Server
	healthServer *health.Server
	port         int
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

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(gRPCServer, healthServer)

	return &ServerApp{
		log:          log,
		gRPCServer:   gRPCServer,
		healthServer: healthServer,
		port:         port,
	}
}

// Run открывает порт из конфига и обслуживает запросы до остановки сервера.
func (sa *ServerApp) Run() error {
	const op = "ServerApp.Run"

	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf(":%d", sa.port))
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return sa.Serve(l)
}

// Serve обслуживает запросы на уже открытом listener. Отделён от Run, чтобы
// в тестах запускать сервер без сети (bufconn) или на случайном порту.
func (sa *ServerApp) Serve(l net.Listener) error {
	const op = "ServerApp.Serve"

	sa.log.Info("place grpc server started", slog.String("addr", l.Addr().String()))

	if err := sa.gRPCServer.Serve(l); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

// GracefulStop останавливает сервер: перестаёт принимать новые запросы и ждёт
// завершения начатых. Если они не завершились, пока не истёк ctx, оставшиеся
// соединения закрываются принудительно. Без ограничения остановка могла бы
// длиться бесконечно, например пока клиент держит открытый поток Watch.
func (sa *ServerApp) GracefulStop(ctx context.Context) {
	const op = "ServerApp.GracefulStop"

	log := sa.log.With(slog.String("source", op))
	log.Info("stopping place grpc server", slog.Int("port", sa.port))

	sa.healthServer.Shutdown()

	stopped := make(chan struct{})
	go func() {
		sa.gRPCServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Info("place grpc server gracefully stopped")
	case <-ctx.Done():
		log.Warn("graceful stop timed out, closing remaining connections")
		// Stop закрывает все соединения, после чего GracefulStop тоже завершается.
		sa.gRPCServer.Stop()
		<-stopped
		log.Info("place grpc server stopped")
	}
}
