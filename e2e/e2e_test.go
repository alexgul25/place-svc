//go:build integration

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	placev1 "github.com/alexgul25/protos/gen/go/place/v1"

	"github.com/alexgul25/place-svc/internal/cache"
	"github.com/alexgul25/place-svc/internal/domain/events"
	"github.com/alexgul25/place-svc/internal/outbox"
	"github.com/alexgul25/place-svc/internal/testutil/kafkatest"
)

// connect подключается к сервису и ждёт, пока он начнёт отвечать.
func connect(t *testing.T, e env) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient("127.0.0.1:"+e["GRPCSERVER_PORT"], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("service is not ready: status %v, error %v", resp.GetStatus(), err)
	}
	return conn
}

// requireJSONLogs проверяет, что в окружении prod каждая строка вывода - JSON-запись лога.
func requireJSONLogs(t *testing.T, output string) {
	t.Helper()

	for line := range strings.Lines(strings.TrimSpace(output)) {
		if !json.Valid([]byte(line)) {
			t.Errorf("log line is not JSON: %q", line)
		}
	}
}

// silentBroker принимает соединения и ничего не отвечает, как зависшая Kafka.
func silentBroker(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	conns := make(chan net.Conn, 100)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			conns <- c
		}
	}()
	t.Cleanup(func() {
		l.Close()
		for {
			select {
			case c := <-conns:
				c.Close()
			default:
				return
			}
		}
	})
	return l.Addr().String()
}

// ----------------------------------------------------------------------------
// Мигратор
// ----------------------------------------------------------------------------

func TestMigrator(t *testing.T) {
	// Мигратору хватает настроек БД: Kafka, Redis и ENV ему не нужны.
	e := newEnv(t).database()

	code, out := runToExit(t, migratorBin, e)
	if code != 0 {
		t.Fatalf("migrator exited with code %d:\n%s", code, out)
	}
	if got := strings.Count(out, "migration applied"); got != 3 {
		t.Errorf("applied %d migrations, want 3:\n%s", got, out)
	}

	// Повторный запуск при каждом деплое ничего не меняет.
	code, out = runToExit(t, migratorBin, e)
	if code != 0 || strings.Contains(out, "migration applied") || !strings.Contains(out, "migrations applied successfully") {
		t.Errorf("second run: exit code %d, output:\n%s", code, out)
	}
}

func TestMigrator_Errors(t *testing.T) {
	e := newEnv(t).database()

	tests := []struct {
		name    string
		env     env
		wantOut string
	}{
		{name: "missing config", env: e.with("DB_PASSWORD", ""), wantOut: "DB_PASSWORD"},
		{name: "database unavailable", env: e.with("DB_PORT", "1"), wantOut: "failed to up migrations"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out := runToExit(t, migratorBin, tt.env)
			if code != 1 {
				t.Errorf("exit code = %d, want 1", code)
			}
			if !strings.Contains(out, tt.wantOut) {
				t.Errorf("output does not mention %q:\n%s", tt.wantOut, out)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Сервис
// ----------------------------------------------------------------------------

func TestService_Lifecycle(t *testing.T) {
	// Сервис останавливается по SIGTERM (docker stop, Kubernetes) и по SIGINT (Ctrl+C в терминале).
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			e := newEnv(t)
			migrate(t, e)

			svc := start(t, serviceBin, e)
			client := placev1.NewPlaceServiceClient(connect(t, e))
			userID := uuid.NewString()
			// Сервис кладёт список мест в кеш: ключ удаляется после теста.
			t.Cleanup(func() {
				rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr, DB: redisDB})
				defer rdb.Close()
				rdb.Del(context.Background(), cache.ToKeyUserPlaces(userID))
			})
			ctx := metadata.AppendToOutgoingContext(t.Context(), "x-service-name", "gateway-svc", "x-user-id", userID)

			// Короткий сценарий: настройки из окружения, PostgreSQL, Redis и Kafka работают вместе.
			place, err := client.AddPlace(ctx, &placev1.AddPlaceRequest{Name: "Парк Горького", Info: "Прогулка у реки"})
			if err != nil {
				t.Fatalf("AddPlace returned error: %v", err)
			}
			list, err := client.GetUserPlaces(ctx, &placev1.GetUserPlacesRequest{UserId: userID})
			if err != nil || len(list.GetPlaces()) != 1 || list.GetPlaces()[0].GetName() != "Парк Горького" {
				t.Fatalf("GetUserPlaces = (%v, %v), want the added place", list, err)
			}

			// Событие о новом месте доходит до Kafka.
			consumeCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			msgs, err := kafkatest.Consume(consumeCtx, brokers, outbox.TopicPlaceCreated, 1, func(m kafkatest.Message) bool {
				return m.Key == userID
			})
			if err != nil {
				t.Fatal(err)
			}
			var event events.PlaceCreated
			if err := json.Unmarshal(msgs[0].Value, &event); err != nil || event.PlaceID != place.GetId() {
				t.Errorf("event = %q (%v), want place.created for %s", msgs[0].Value, err, place.GetId())
			}

			svc.signal(t, sig)
			if code := svc.wait(t, 15*time.Second); code != 0 {
				t.Errorf("exit code = %d, want 0", code)
			}

			out := svc.output.String()
			for _, msg := range []string{"place grpc server started", "app has shut down gracefully"} {
				if !strings.Contains(out, msg) {
					t.Errorf("output does not contain %q", msg)
				}
			}
			requireJSONLogs(t, out)
		})
	}
}

func TestService_GracefulTimeout(t *testing.T) {
	e := newEnv(t).with("GRACEFUL_TIMEOUT", "1s")
	migrate(t, e)

	svc := start(t, serviceBin, e)
	conn := connect(t, e)

	// Поток Watch длится, пока его не закроет клиент: без таймаута остановка ждала бы бесконечно.
	stream, err := healthpb.NewHealthClient(conn).Watch(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("failed to watch health: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("failed to receive health status: %v", err)
	}

	started := time.Now()
	svc.signal(t, syscall.SIGTERM)
	if code := svc.wait(t, 15*time.Second); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}

	// Сервис ждёт GRACEFUL_TIMEOUT и затем закрывает соединения принудительно.
	if elapsed := time.Since(started); elapsed < time.Second || elapsed > 5*time.Second {
		t.Errorf("service stopped in %v, want about GRACEFUL_TIMEOUT (1s)", elapsed)
	}
	// Тот же срок ограничивает и всю остановку приложения: какое из двух сообщений
	// успеет попасть в лог до выхода процесса, зависит от планировщика.
	out := svc.output.String()
	if !strings.Contains(out, "graceful stop timed out") && !strings.Contains(out, "graceful shutdown timed out") {
		t.Errorf("forced stop is not logged:\n%s", out)
	}
}

func TestService_StartupErrors(t *testing.T) {
	e := newEnv(t)
	migrate(t, e)

	// Порт, занятый другим процессом.
	var lc net.ListenConfig
	busy, err := lc.Listen(t.Context(), "tcp", ":0")
	if err != nil {
		t.Fatalf("failed to occupy port: %v", err)
	}
	defer busy.Close()
	busyPort := fmt.Sprint(busy.Addr().(*net.TCPAddr).Port)

	tests := []struct {
		name    string
		env     env
		wantOut string
		// Ошибки конфига печатаются текстом: формат лога задаёт ENV, а он ещё не прочитан.
		json bool
	}{
		{name: "missing Redis TTL", env: e.with("REDIS_CACHE_TTL", ""), wantOut: "REDIS_CACHE_TTL"},
		{name: "invalid port", env: e.with("GRPCSERVER_PORT", "port"), wantOut: "GRPCSERVER_PORT"},
		{name: "database unavailable", env: e.with("DB_PORT", "1"), wantOut: "failed to ping storage", json: true},
		{name: "Kafka unavailable", env: e.with("KAFKA_PRODUCER_BROKERS", "127.0.0.1:1"), wantOut: "failed to init kafka sync producer", json: true},
		{name: "Redis unavailable", env: e.with("REDIS_CACHE_ADDR", "127.0.0.1:1"), wantOut: "failed to init place cache", json: true},
		{name: "port is busy", env: e.with("GRPCSERVER_PORT", busyPort), wantOut: "address already in use", json: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Сервис сразу завершается с кодом 1 и понятной ошибкой, а не падает с паникой
			// и не висит без возможности обслуживать запросы.
			code, out := runToExit(t, serviceBin, tt.env)
			if code != 1 {
				t.Errorf("exit code = %d, want 1", code)
			}
			if !strings.Contains(out, tt.wantOut) {
				t.Errorf("output does not mention %q:\n%s", tt.wantOut, out)
			}
			if strings.Contains(out, "panic") {
				t.Errorf("service panicked:\n%s", out)
			}
			// Сборщик логов в prod разбирает JSON: строка с причиной ошибки не должна выпадать.
			if tt.json {
				requireJSONLogs(t, out)
			}
		})
	}
}

// Kafka принимает соединения, но не отвечает. Пока сервис ждёт её при запуске,
// SIGTERM (например, docker stop) должен останавливать его сразу, а не через 24 с.
func TestService_StopDuringStartup(t *testing.T) {
	e := newEnv(t).with("KAFKA_PRODUCER_BROKERS", silentBroker(t))
	migrate(t, e)

	svc := start(t, serviceBin, e)
	// За это время сервис проверяет БД и доходит до подключения к Kafka.
	time.Sleep(time.Second)

	started := time.Now()
	svc.signal(t, syscall.SIGTERM)
	if code := svc.wait(t, 5*time.Second); code != 0 {
		t.Errorf("exit code = %d, want 0: a stop before startup is not a failure", code)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Errorf("service stopped %v after SIGTERM, want at once", elapsed)
	}

	out := svc.output.String()
	if !strings.Contains(out, "service stopped during startup") {
		t.Errorf("stop during startup is not logged:\n%s", out)
	}
	requireJSONLogs(t, out)
}
