package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	grpcapp "github.com/alexgul25/place-svc/internal/app/grpc"
	"github.com/alexgul25/place-svc/internal/domain/models"
	"github.com/alexgul25/place-svc/internal/outbox"
)

// Внутренний тест: App собирается из настоящего gRPC-сервера и outbox-процессора,
// а вместо PostgreSQL, Redis и Kafka - заглушки. Так запуск и остановку приложения
// можно проверить без внешних сервисов; New с ними проверяется в интеграционных тестах.

// ----------------------------------------------------------------------------
// Заглушки
// ----------------------------------------------------------------------------

type placeServiceStub struct{}

func (placeServiceStub) AddPlace(context.Context, string, string, string) (models.Place, error) {
	return models.Place{}, nil
}

func (placeServiceStub) GetUserPlaces(context.Context, string) ([]models.Place, error) {
	return nil, nil
}

// outboxRepoStub отдаёт записи из records один раз, потом пустые списки.
type outboxRepoStub struct {
	mu      sync.Mutex
	records []outbox.Record
}

func (r *outboxRepoStub) SelectPending(context.Context, int) ([]outbox.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	records := r.records
	r.records = nil
	return records, nil
}

func (r *outboxRepoStub) MarkAsPublished(context.Context, string) error { return nil }

// producerStub отправляет сразу или, если задан release, ждёт его закрытия:
// так ведёт себя sarama, когда Kafka не отвечает.
type producerStub struct {
	sending chan struct{} // закрывается, когда началась первая отправка
	release chan struct{}
	once    sync.Once
}

func (p *producerStub) SendMessage(context.Context, string, string, []byte, []byte) error {
	if p.sending != nil {
		p.once.Do(func() { close(p.sending) })
	}
	if p.release != nil {
		<-p.release
	}
	return nil
}

func (p *producerStub) Close() error { return nil }

// closeRecorder запоминает, какие соединения и в каком порядке закрыло приложение.
type closeRecorder struct {
	mu     sync.Mutex
	closed []string
	errs   map[string]error
	all    chan struct{} // закрывается, когда закрыты все три соединения
}

func newCloseRecorder(errs map[string]error) *closeRecorder {
	return &closeRecorder{errs: errs, all: make(chan struct{})}
}

type namedCloser struct {
	name string
	rec  *closeRecorder
}

func (c namedCloser) Close() error {
	c.rec.mu.Lock()
	defer c.rec.mu.Unlock()

	c.rec.closed = append(c.rec.closed, c.name)
	if len(c.rec.closed) == 3 {
		close(c.rec.all)
	}
	return c.rec.errs[c.name]
}

func (r *closeRecorder) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.closed)
}

// syncBuffer - буфер для логов, безопасный для записи из горутин приложения.
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

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// newTestApp собирает приложение на заданном порту с заглушками вместо внешних сервисов.
func newTestApp(t *testing.T, port int, repo outbox.OutboxRepository, producer *producerStub, rec *closeRecorder) (*App, *syncBuffer) {
	t.Helper()

	logs := &syncBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	return &App{
		log:        log,
		grpcServer: grpcapp.New(log, placeServiceStub{}, port),
		processor:  outbox.NewProcessorWithSyncProducer(repo, producer, log, time.Second, 10*time.Millisecond, 100),
		storage:    namedCloser{name: "storage", rec: rec},
		producer:   namedCloser{name: "producer", rec: rec},
		cache:      namedCloser{name: "cache", rec: rec},
	}, logs
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

// waitServing ждёт, пока сервер на порту ответит на health-check.
func waitServing(t *testing.T, port int) {
	t.Helper()

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health check = (%v, %v), want SERVING", resp.GetStatus(), err)
	}
}

// shutdown вызывает GracefulShutdown с таймаутом и возвращает, сколько он длился.
func shutdown(t *testing.T, a *App, timeout time.Duration) time.Duration {
	t.Helper()

	// Время засекается до создания контекста: срок отсчитывается от момента его создания.
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		a.GracefulShutdown(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout + 5*time.Second):
		t.Fatalf("GracefulShutdown did not return within %v after its timeout", 5*time.Second)
	}
	return time.Since(start)
}

// ----------------------------------------------------------------------------
// Run и GracefulShutdown
// ----------------------------------------------------------------------------

func TestApp_RunAndGracefulShutdown(t *testing.T) {
	port := freePort(t)
	rec := newCloseRecorder(nil)
	a, logs := newTestApp(t, port, &outboxRepoStub{}, &producerStub{}, rec)

	runErr := make(chan error, 1)
	go func() { runErr <- a.Run() }()
	waitServing(t, port)

	shutdown(t, a, 5*time.Second)

	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned error after GracefulShutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after GracefulShutdown")
	}

	// Сначала кеш и продюсер, БД - последней.
	if got := rec.names(); !slices.Equal(got, []string{"cache", "producer", "storage"}) {
		t.Errorf("closed %q, want [cache producer storage]", got)
	}
	out := logs.String()
	for _, msg := range []string{"processor with sync producer is shutdowned successfully", "app has shut down gracefully"} {
		if !strings.Contains(out, msg) {
			t.Errorf("no %q in the log: %s", msg, out)
		}
	}
}

// Сервер не запустился: Run сразу возвращает ошибку, а GracefulShutdown всё равно
// останавливает уже запущенный outbox-процессор и закрывает соединения.
func TestApp_RunPortIsBusy(t *testing.T) {
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", ":0")
	if err != nil {
		t.Fatalf("failed to occupy port: %v", err)
	}
	defer l.Close()

	rec := newCloseRecorder(nil)
	a, logs := newTestApp(t, l.Addr().(*net.TCPAddr).Port, &outboxRepoStub{}, &producerStub{}, rec)

	if err := a.Run(); !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("Run error = %v, want %v", err, syscall.EADDRINUSE)
	}

	shutdown(t, a, 5*time.Second)

	if got := rec.names(); len(got) != 3 {
		t.Errorf("closed %q, want all three connections", got)
	}
	if !strings.Contains(logs.String(), "processor with sync producer is shutdowned successfully") {
		t.Errorf("outbox processor was not stopped, log: %s", logs.String())
	}
}

// Outbox-процессор ждёт ответа Kafka, которая не отвечает, а sarama прервать нельзя.
// Остановка не должна ждать его дольше GRACEFUL_TIMEOUT.
func TestApp_GracefulShutdownTimeout(t *testing.T) {
	producer := &producerStub{sending: make(chan struct{}), release: make(chan struct{})}
	repo := &outboxRepoStub{records: []outbox.Record{{ID: "r1", Message: outbox.Message{Topic: outbox.TopicPlaceCreated}}}}
	rec := newCloseRecorder(nil)
	port := freePort(t)
	a, logs := newTestApp(t, port, repo, producer, rec)

	go func() { _ = a.Run() }()
	waitServing(t, port)

	select {
	case <-producer.sending:
	case <-time.After(5 * time.Second):
		t.Fatal("the outbox processor did not start sending")
	}

	const timeout = 200 * time.Millisecond
	elapsed := shutdown(t, a, timeout)

	// Отправка отпускается после проверки: остановка доходит до конца в фоне,
	// и тест дожидается её, чтобы горутины приложения не пережили тест.
	t.Cleanup(func() {
		close(producer.release)
		select {
		case <-rec.all:
		case <-time.After(5 * time.Second):
			t.Error("connections were not closed after the send finished")
		}
	})

	if elapsed < timeout {
		t.Errorf("GracefulShutdown returned after %v, before the timeout %v", elapsed, timeout)
	}
	if !strings.Contains(logs.String(), "graceful shutdown timed out") {
		t.Errorf("timed out shutdown is not logged, log: %s", logs.String())
	}
	if strings.Contains(logs.String(), "app has shut down gracefully") {
		t.Errorf("a timed out shutdown must not be reported as graceful, log: %s", logs.String())
	}
}

func TestApp_GracefulShutdownLogsCloseErrors(t *testing.T) {
	rec := newCloseRecorder(map[string]error{
		"cache":   errors.New("redis: connection reset"),
		"storage": errors.New("sql: database is closed"),
	})
	a, logs := newTestApp(t, freePort(t), &outboxRepoStub{}, &producerStub{}, rec)

	// Run не вызывается: GracefulShutdown должен работать и для незапущенного приложения.
	shutdown(t, a, 5*time.Second)

	out := logs.String()
	if !strings.Contains(out, "failed to close app components") {
		t.Fatalf("close errors are not logged, log: %s", out)
	}
	// Ошибка одного соединения не мешает закрыть остальные, и в лог попадают все ошибки.
	for _, want := range []string{"redis: connection reset", "sql: database is closed"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in the log: %s", want, out)
		}
	}
	if got := rec.names(); len(got) != 3 {
		t.Errorf("closed %q, want all three connections", got)
	}
}
