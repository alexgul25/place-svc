package grpcapp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	grpcapp "github.com/alexgul25/place-svc/internal/app/grpc"
	"github.com/alexgul25/place-svc/internal/domain/models"
	placev1 "github.com/alexgul25/protos/gen/go/place/v1"
)

const userID = "4f1c2d3e-5a6b-4c7d-8e9f-0a1b2c3d4e5f"

// ----------------------------------------------------------------------------
// Заглушка сервиса
// ----------------------------------------------------------------------------

// serviceStub - бизнес-логика для сервера. Функции вызываются из горутин сервера,
// поэтому тест не должен менять их во время работы.
type serviceStub struct {
	addPlace func(ctx context.Context, userID, name, info string) (models.Place, error)
}

func (s *serviceStub) AddPlace(ctx context.Context, userID, name, info string) (models.Place, error) {
	return s.addPlace(ctx, userID, name, info)
}

func (s *serviceStub) GetUserPlaces(context.Context, string) ([]models.Place, error) {
	return nil, nil
}

// addOK - AddPlace, который возвращает место с переданными полями.
func addOK(_ context.Context, userID, name, info string) (models.Place, error) {
	return models.Place{ID: "6f1c2a8e-3b7d-4c5e-9f10-2a3b4c5d6e7f", UserID: userID, Name: name, Info: info}, nil
}

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// syncBuffer - буфер для логов, безопасный для записи из горутин сервера.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newLogger возвращает JSON-логгер, пишущий в буфер.
func newLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// dialBufconn возвращает клиентское соединение с сервером в памяти.
func dialBufconn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}

// startServer собирает сервер так же, как сервис (все интерцепторы и health-check),
// запускает его в памяти (bufconn) и возвращает соединение с ним.
// После теста сервер останавливается через GracefulStop.
func startServer(t *testing.T, log *slog.Logger, svc *serviceStub) *grpc.ClientConn {
	t.Helper()

	app := grpcapp.New(log, svc, 0)
	lis := bufconn.Listen(1 << 20)

	served := make(chan error, 1)
	go func() { served <- app.Serve(lis) }()
	t.Cleanup(func() {
		// t.Context() к моменту Cleanup уже отменён, поэтому отдельный контекст с таймаутом.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		app.GracefulStop(ctx)
		if err := <-served; err != nil {
			t.Errorf("Serve returned error: %v", err)
		}
	})

	return dialBufconn(t, lis)
}

// asUser - вызов от имени пользователя userID, как его делает Gateway.
func asUser(t *testing.T) context.Context {
	t.Helper()
	return metadata.AppendToOutgoingContext(t.Context(), "x-service-name", "gateway-svc", "x-user-id", userID)
}

// recvStatus читает следующий статус из потока Watch. Если сервер не присылает его
// за 5 секунд, тест падает, а не зависает.
func recvStatus(t *testing.T, stream healthpb.Health_WatchClient) (healthpb.HealthCheckResponse_ServingStatus, error) {
	t.Helper()

	type result struct {
		resp *healthpb.HealthCheckResponse
		err  error
	}
	got := make(chan result, 1)
	go func() {
		resp, err := stream.Recv()
		got <- result{resp, err}
	}()

	select {
	case r := <-got:
		return r.resp.GetStatus(), r.err
	case <-time.After(5 * time.Second):
		t.Fatal("no health status from the server within 5s")
		return 0, nil
	}
}

// freePort находит свободный TCP-порт.
func freePort(t *testing.T) int {
	t.Helper()

	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

// busyPort занимает порт на время теста.
func busyPort(t *testing.T) int {
	t.Helper()

	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", ":0")
	if err != nil {
		t.Fatalf("failed to occupy port: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	return l.Addr().(*net.TCPAddr).Port
}

// ----------------------------------------------------------------------------
// Цепочка интерцепторов
// ----------------------------------------------------------------------------

// Заголовок x-user-id доходит до хэндлера через интерцептор, которым сервер собран в New.
func TestServer_HeadersReachHandlers(t *testing.T) {
	log, _ := newLogger()
	conn := startServer(t, log, &serviceStub{addPlace: addOK})

	resp, err := placev1.NewPlaceServiceClient(conn).AddPlace(asUser(t), &placev1.AddPlaceRequest{Name: "Парк"})
	if err != nil {
		t.Fatalf("AddPlace returned error: %v", err)
	}
	if resp.GetUserId() != userID {
		t.Errorf("user_id = %q, want %q", resp.GetUserId(), userID)
	}
}

func TestServer_RecoversFromPanic(t *testing.T) {
	var calls int
	var mu sync.Mutex
	svc := &serviceStub{addPlace: func(ctx context.Context, userID, name, info string) (models.Place, error) {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			panic("unexpected nil pointer in business logic")
		}
		return addOK(ctx, userID, name, info)
	}}
	log, logs := newLogger()
	client := placev1.NewPlaceServiceClient(startServer(t, log, svc))
	req := &placev1.AddPlaceRequest{Name: "Парк"}

	// Паника в обработчике превращается в Internal без подробностей.
	_, err := client.AddPlace(asUser(t), req)
	if st := status.Convert(err); st.Code() != codes.Internal || st.Message() != "internal error" {
		t.Fatalf("got status (%v, %q), want (Internal, %q)", st.Code(), st.Message(), "internal error")
	}
	if !strings.Contains(logs.String(), "recovered from panic") {
		t.Errorf("panic was not logged, log: %s", logs.String())
	}

	// Сервер продолжает обслуживать запросы.
	if _, err := client.AddPlace(asUser(t), req); err != nil {
		t.Fatalf("second call returned error: %v", err)
	}
}

func TestServer_LogsCallsButNotHealthChecks(t *testing.T) {
	log, logs := newLogger()
	conn := startServer(t, log, &serviceStub{addPlace: addOK})

	if _, err := placev1.NewPlaceServiceClient(conn).AddPlace(asUser(t), &placev1.AddPlaceRequest{Name: "Парк"}); err != nil {
		t.Fatalf("AddPlace returned error: %v", err)
	}
	if _, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check returned error: %v", err)
	}

	out := logs.String()
	if !strings.Contains(out, `"grpc.method":"AddPlace"`) || !strings.Contains(out, `"x-service-name":"gateway-svc"`) {
		t.Errorf("AddPlace call was not logged with service name, log: %s", out)
	}
	// В лог попадает только x-service-name: ID пользователя логировать не нужно.
	if strings.Contains(out, userID) {
		t.Errorf("user id must not be logged by the interceptor, log: %s", out)
	}
	if strings.Contains(out, "grpc.health.v1.Health") {
		t.Errorf("health check must not be logged, log: %s", out)
	}
}

// ----------------------------------------------------------------------------
// Health-check и остановка
// ----------------------------------------------------------------------------

func TestServer_HealthCheck(t *testing.T) {
	log, _ := newLogger()
	conn := startServer(t, log, &serviceStub{})

	resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check returned error: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("status = %v, want SERVING", resp.GetStatus())
	}
}

// startWatching запускает сервер в памяти и открывает поток Watch - вызов, который
// длится, пока клиент его не закроет. Возвращает приложение, поток и функцию закрытия потока.
func startWatching(t *testing.T, log *slog.Logger) (*grpcapp.ServerApp, healthpb.Health_WatchClient, context.CancelFunc) {
	t.Helper()

	app := grpcapp.New(log, &serviceStub{}, 0)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = app.Serve(lis) }()

	conn := dialBufconn(t, lis)

	// Watch - поток, в который сервер присылает каждое изменение статуса.
	watchCtx, cancelWatch := context.WithCancel(t.Context())
	t.Cleanup(cancelWatch)
	stream, err := healthpb.NewHealthClient(conn).Watch(watchCtx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("failed to watch health: %v", err)
	}
	if st, err := recvStatus(t, stream); err != nil || st != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("initial status = (%v, %v), want SERVING", st, err)
	}

	return app, stream, cancelWatch
}

func TestServer_GracefulStopReportsNotServing(t *testing.T) {
	log, logs := newLogger()
	app, stream, cancelWatch := startWatching(t, log)

	// Контекст без таймаута: остановка ждёт, сколько потребуется.
	stopped := make(chan struct{})
	go func() {
		app.GracefulStop(context.Background())
		close(stopped)
	}()

	// При остановке сервер сразу сообщает, что больше не принимает запросы.
	if st, err := recvStatus(t, stream); err != nil || st != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("status after stop = (%v, %v), want NOT_SERVING", st, err)
	}

	// GracefulStop ждёт завершения всех открытых вызовов, включая поток Watch.
	// Пока клиент его не закроет, остановка не завершится.
	select {
	case <-stopped:
		t.Fatal("GracefulStop returned while a watch stream was still open")
	case <-time.After(100 * time.Millisecond):
	}

	cancelWatch()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("GracefulStop did not return after the watch stream was closed")
	}
	if !strings.Contains(logs.String(), "place grpc server gracefully stopped") {
		t.Errorf("graceful stop is not logged, log: %s", logs.String())
	}
}

func TestServer_GracefulStopTimeout(t *testing.T) {
	log, logs := newLogger()
	app, stream, _ := startWatching(t, log)

	// Клиент не закрывает поток Watch. Без таймаута остановка ждала бы бесконечно,
	// с таймаутом оставшиеся соединения закрываются принудительно.
	const timeout = 200 * time.Millisecond
	// Время засекается до создания контекста: срок отсчитывается от момента его создания,
	// и замер, сделанный позже, оказался бы короче таймаута.
	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	stopped := make(chan struct{})
	go func() {
		app.GracefulStop(ctx)
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("GracefulStop did not return after the timeout")
	}
	if elapsed := time.Since(start); elapsed < timeout {
		t.Errorf("GracefulStop returned after %v, before the timeout %v", elapsed, timeout)
	}
	if !strings.Contains(logs.String(), "graceful stop timed out") {
		t.Errorf("forced stop is not logged, log: %s", logs.String())
	}

	// Клиент узнаёт о закрытии соединения: поток завершается ошибкой
	// (перед ней может прийти статус NOT_SERVING).
	for {
		if _, err := recvStatus(t, stream); err != nil {
			break
		}
	}
}

func TestServe_AfterStop(t *testing.T) {
	log, _ := newLogger()
	app := grpcapp.New(log, &serviceStub{}, 0)
	app.GracefulStop(t.Context())

	err := app.Serve(bufconn.Listen(1 << 20))
	if !errors.Is(err, grpc.ErrServerStopped) {
		t.Errorf("got error %v, want %v", err, grpc.ErrServerStopped)
	}
}

// ----------------------------------------------------------------------------
// Run: запуск на настоящем TCP-порту
// ----------------------------------------------------------------------------

func TestRun_ServesOnConfiguredPort(t *testing.T) {
	port := freePort(t)
	log, logs := newLogger()
	app := grpcapp.New(log, &serviceStub{}, port)

	done := make(chan error, 1)
	go func() { done <- app.Run() }()

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer conn.Close()

	// WaitForReady: клиент ждёт, пока сервер начнёт слушать порт.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatalf("health check returned error: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("status = %v, want SERVING", resp.GetStatus())
	}
	if !strings.Contains(logs.String(), fmt.Sprintf(":%d", port)) {
		t.Errorf("started address is not logged, log: %s", logs.String())
	}

	// Run обслуживает запросы до остановки: main ждёт его, чтобы узнать об ошибке сервера.
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while the server was serving", err)
	default:
	}

	app.GracefulStop(t.Context())
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error after GracefulStop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after GracefulStop")
	}
}

// Занятый порт - ошибка запуска, которую вызывающий может обработать, а не паника.
func TestRun_PortIsBusy(t *testing.T) {
	log, _ := newLogger()
	app := grpcapp.New(log, &serviceStub{}, busyPort(t))

	err := app.Run()
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("got error %v, want %v", err, syscall.EADDRINUSE)
	}
}
