//go:build integration

package app_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	placev1 "github.com/alexgul25/protos/gen/go/place/v1"

	"github.com/alexgul25/place-svc/internal/app"
	"github.com/alexgul25/place-svc/internal/cache"
	"github.com/alexgul25/place-svc/internal/config"
	"github.com/alexgul25/place-svc/internal/domain/events"
	"github.com/alexgul25/place-svc/internal/outbox"
	"github.com/alexgul25/place-svc/internal/storage/postgresql"
	"github.com/alexgul25/place-svc/internal/testutil/kafkatest"
	"github.com/alexgul25/place-svc/internal/testutil/pgtest"
	"github.com/alexgul25/place-svc/migrations"
)

// Приложение собирается так же, как в main: настоящие PostgreSQL, Redis и Kafka,
// gRPC-сервер на TCP-порту и outbox-процессор.

// testRedisDB - номер базы Redis для тестов пакета: тесты кеша используют базу 1.
const testRedisDB = 2

// Временная база пакета с миграциями, соединение с ней и адреса Redis и Kafka.
var (
	testDBConfig config.DatabaseConfig
	testDB       *sql.DB
	testRedis    string
	testBrokers  []string
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run отделён от TestMain, чтобы отложенные вызовы выполнились до os.Exit.
func run(m *testing.M) int {
	ctx := context.Background()

	if testRedis = os.Getenv("TEST_REDIS_ADDR"); testRedis == "" {
		fmt.Fprintln(os.Stderr, "TEST_REDIS_ADDR is not set, start a test Redis with `make test-infra-up`")
		return 1
	}
	var err error
	if testBrokers, err = kafkatest.Brokers(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	// Топик для событий при деплое создаёт kafka-init, здесь - тест.
	topicCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := kafkatest.CreateTopic(topicCtx, testBrokers, outbox.TopicPlaceCreated); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	dsn, drop, err := pgtest.NewDatabase(ctx, "place_svc_app")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() {
		if err := drop(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()

	storage, err := postgresql.NewStorage(dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer storage.Close()

	if _, err := migrations.Up(ctx, storage.DB()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	if testDBConfig, err = databaseConfig(dsn); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	testDB = storage.DB()

	return m.Run()
}

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// databaseConfig раскладывает строку подключения на поля конфига, как их задают переменные окружения.
func databaseConfig(dsn string) (config.DatabaseConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return config.DatabaseConfig{}, err
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		return config.DatabaseConfig{}, fmt.Errorf("invalid port in %s: %w", pgtest.EnvDSN, err)
	}
	password, _ := u.User.Password()

	return config.DatabaseConfig{
		User:     u.User.Username(),
		Password: password,
		DbName:   strings.TrimPrefix(u.Path, "/"),
		Host:     u.Hostname(),
		Port:     port,
	}, nil
}

// newConfig возвращает настройки сервиса для тестовых серверов. Процессор проверяет
// outbox часто, чтобы тест не ждал событий долго.
func newConfig(t *testing.T) *config.Config {
	t.Helper()

	return &config.Config{
		Env:         "local",
		ServiceName: "place-svc",
		Database:    testDBConfig,
		GRPCServer:  config.GRPCServerConfig{Port: freePort(t), GracefulTimeout: 5 * time.Second},
		OutboxProcessor: config.OutboxProcessorConfig{
			OpTimeout:      5 * time.Second,
			SelectInterval: 100 * time.Millisecond,
			SelectSize:     10,
		},
		KafkaProducer: config.KafkaProducerConfig{
			BrokersRaw:  strings.Join(testBrokers, ","),
			Brokers:     testBrokers,
			SendTimeout: 10 * time.Second,
		},
		RedisCache: config.RedisCacheConfig{
			Addr:         testRedis,
			DB:           testRedisDB,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			TTL:          time.Hour,
		},
	}
}

// newDatabase создаёт отдельную пустую базу, к которой подключается только проверяемый код,
// и возвращает её имя и настройки подключения.
func newDatabase(t *testing.T) (string, config.DatabaseConfig) {
	t.Helper()

	dsn, drop, err := pgtest.NewDatabase(t.Context(), "place_svc_app_new")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := drop(); err != nil {
			t.Error(err)
		}
	})

	cfg, err := databaseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return cfg.DbName, cfg
}

// requireNoConnections ждёт, пока в базе name не останется сессий: закрытые соединения
// PostgreSQL убирает из pg_stat_activity не мгновенно.
func requireNoConnections(t *testing.T, name string) {
	t.Helper()

	var n int
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := testDB.QueryRowContext(t.Context(), "SELECT count(*) FROM pg_stat_activity WHERE datname = $1", name).Scan(&n); err != nil {
			t.Fatalf("failed to count connections: %v", err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connections to database %s are still open", n, name)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// requireNoNewGoroutines ждёт, пока число горутин не вернётся к before: горутины
// закрытых клиентов завершаются не мгновенно.
func requireNoNewGoroutines(t *testing.T, before int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines are running after New failed, %d before", runtime.NumGoroutine(), before)
		}
		time.Sleep(10 * time.Millisecond)
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

// publishedRecords возвращает ID отправленных outbox-записей пользователя от старых к новым.
func publishedRecords(t *testing.T, userID string) []string {
	t.Helper()

	rows, err := testDB.QueryContext(t.Context(),
		"SELECT id FROM outbox WHERE msg_key = $1 AND send_status = $2 ORDER BY created_at", userID, outbox.StatusPublished)
	if err != nil {
		t.Fatalf("failed to select outbox: %v", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("failed to scan outbox id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to select outbox: %v", err)
	}
	return ids
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
	// После закрытия соединений подключение sarama в фоне быстро завершается ошибкой.
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
// Сценарий через все слои
// ----------------------------------------------------------------------------

// placeClient - клиент сервиса от имени пользователя, как его вызывает Gateway.
type placeClient struct {
	t      *testing.T
	client placev1.PlaceServiceClient
	userID string
}

func (c *placeClient) ctx() context.Context {
	return metadata.AppendToOutgoingContext(c.t.Context(), "x-service-name", "gateway-svc", "x-user-id", c.userID)
}

func (c *placeClient) add(name, info string) *placev1.AddPlaceResponse {
	c.t.Helper()

	resp, err := c.client.AddPlace(c.ctx(), &placev1.AddPlaceRequest{Name: name, Info: info})
	if err != nil {
		c.t.Fatalf("AddPlace(%q) returned error: %v", name, err)
	}
	return resp
}

func (c *placeClient) names() []string {
	c.t.Helper()

	resp, err := c.client.GetUserPlaces(c.ctx(), &placev1.GetUserPlacesRequest{UserId: c.userID})
	if err != nil {
		c.t.Fatalf("GetUserPlaces returned error: %v", err)
	}
	var names []string
	for _, p := range resp.GetPlaces() {
		names = append(names, p.GetName())
	}
	return names
}

func TestApp_PlaceScenario(t *testing.T) {
	cfg := newConfig(t)
	// Список брокеров с пробелами и пустым элементом: в лог должен попасть разобранный список.
	cfg.KafkaProducer.BrokersRaw = " " + strings.Join(testBrokers, " , ") + ", "
	logs := &syncBuffer{}
	log := slog.New(slog.NewJSONHandler(logs, nil))

	application, err := app.New(t.Context(), log, cfg)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- application.Run() }()

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", cfg.GRPCServer.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer conn.Close()

	waitCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	health, err := healthpb.NewHealthClient(conn).Check(waitCtx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil || health.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health = (%v, %v), want SERVING", health.GetStatus(), err)
	}

	user := &placeClient{t: t, client: placev1.NewPlaceServiceClient(conn), userID: uuid.NewString()}
	key := cache.ToKeyUserPlaces(user.userID)
	rdb := goredis.NewClient(&goredis.Options{Addr: testRedis, DB: testRedisDB})
	// Ключ удаляется после теста. Cleanup выполняется после defer, поэтому и Close здесь.
	t.Cleanup(func() {
		rdb.Del(context.Background(), key)
		rdb.Close()
	})

	// Места сохраняются, список приходит от старых к новым.
	park := user.add("Парк Горького", "Прогулка у реки")
	cafe := user.add("Кафе", "")
	if got := user.names(); !slices.Equal(got, []string{"Парк Горького", "Кафе"}) {
		t.Fatalf("places = %q, want [Парк Горького Кафе]", got)
	}

	// Список попал в кеш в базе Redis из настроек.
	if n, err := rdb.Exists(t.Context(), key).Result(); err != nil || n != 1 {
		t.Fatalf("cache key %s exists = %d (%v), want the list cached", key, n, err)
	}

	// Новое место очищает кеш, и следующий список его уже содержит.
	museum := user.add("Музей", "Импрессионисты")
	if n, err := rdb.Exists(t.Context(), key).Result(); err != nil || n != 0 {
		t.Errorf("cache key %s exists = %d (%v) after AddPlace, want it invalidated", key, n, err)
	}
	if got := user.names(); !slices.Equal(got, []string{"Парк Горького", "Кафе", "Музей"}) {
		t.Errorf("places after AddPlace = %q, want [Парк Горького Кафе Музей]", got)
	}

	// Outbox-процессор отправляет в Kafka событие о каждом месте, в порядке добавления.
	consumeCtx, cancelConsume := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelConsume()
	msgs, err := kafkatest.Consume(consumeCtx, testBrokers, outbox.TopicPlaceCreated, 3, func(m kafkatest.Message) bool {
		return m.Key == user.userID
	})
	if err != nil {
		t.Fatal(err)
	}
	var messageIDs []string
	for i, want := range []*placev1.AddPlaceResponse{park, cafe, museum} {
		var e events.PlaceCreated
		if err := json.Unmarshal(msgs[i].Value, &e); err != nil {
			t.Fatalf("event %d is not JSON: %q", i+1, msgs[i].Value)
		}
		if e.PlaceID != want.GetId() || e.UserID != user.userID || e.Name != want.GetName() || e.Info != want.GetInfo() ||
			!e.CreatedAt.Equal(want.GetCreatedAt().AsTime()) {
			t.Errorf("event %d = %+v, want the place from AddPlace %v", i+1, e, want)
		}
		if msgs[i].Headers["source"] != "place-svc" {
			t.Errorf("event %d source = %q, want place-svc", i+1, msgs[i].Headers["source"])
		}
		messageIDs = append(messageIDs, msgs[i].Headers["message_id"])
	}

	// Отправленные записи отмечены в outbox, а message_id - их ID: по нему получатель
	// отбрасывает повторы.
	deadline := time.Now().Add(10 * time.Second)
	for {
		published := publishedRecords(t, user.userID)
		if slices.Equal(published, messageIDs) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("published outbox records = %q, want %q", published, messageIDs)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Остановка, как в main: Run возвращает nil, процессор и соединения закрываются.
	stopCtx, cancelStop := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelStop()
	application.GracefulShutdown(stopCtx)
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after GracefulShutdown")
	}

	out := logs.String()
	if !strings.Contains(out, "app has shut down gracefully") {
		t.Errorf("no graceful shutdown in the log:\n%s", out)
	}
	for line := range strings.Lines(out) {
		var record struct {
			Msg     string
			Brokers []string
		}
		if json.Unmarshal([]byte(line), &record) == nil && record.Msg == "init kafka sync producer" {
			if !slices.Equal(record.Brokers, testBrokers) {
				t.Errorf("logged brokers = %q, want %q", record.Brokers, testBrokers)
			}
			return
		}
	}
	t.Errorf("no init kafka sync producer record with a broker list in the log:\n%s", out)
}

// ----------------------------------------------------------------------------
// Ошибки запуска
// ----------------------------------------------------------------------------

// Если запуск не удался, New закрывает то, что успел открыть: иначе соединение с БД
// и продюсер Kafka оставались бы открытыми, пока жив процесс.
func TestNew_ClosesConnectionsOnError(t *testing.T) {
	tests := []struct {
		name    string
		change  func(cfg *config.Config)
		wantErr string
	}{
		{
			name:    "Kafka unavailable",
			change:  func(cfg *config.Config) { cfg.KafkaProducer.Brokers = []string{"127.0.0.1:1"} },
			wantErr: "failed to init kafka sync producer",
		},
		{
			// К этому моменту открыты и БД, и продюсер Kafka.
			name:    "Redis unavailable",
			change:  func(cfg *config.Config) { cfg.RedisCache.Addr = "127.0.0.1:1" },
			wantErr: "failed to init place cache",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, db := newDatabase(t)
			cfg := newConfig(t)
			cfg.Database = db
			tt.change(cfg)
			before := runtime.NumGoroutine()

			application, err := app.New(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("New returned error %v, want %q", err, tt.wantErr)
			}
			if application != nil {
				t.Errorf("New returned %+v with an error, want nil", application)
			}
			requireNoConnections(t, name)
			// Продюсер Kafka держит соединение и фоновые горутины: после закрытия их не остаётся.
			requireNoNewGoroutines(t, before)
		})
	}
}

// Kafka принимает соединения, но не отвечает: без отмены запуск ждал бы до трёх
// KAFKA_PRODUCER_SEND_TIMEOUT. SIGTERM в это время отменяет ctx, и New должен вернуться сразу.
func TestNew_CanceledWhileWaitingForKafka(t *testing.T) {
	name, db := newDatabase(t)
	cfg := newConfig(t)
	cfg.Database = db
	cfg.KafkaProducer.Brokers = []string{silentBroker(t)}

	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(300*time.Millisecond, cancel)

	start := time.Now()
	application, err := app.New(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("New succeeded with an unresponsive Kafka: %+v", application)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got error %v, want %v", err, context.Canceled)
	}
	if elapsed > 2*time.Second {
		t.Errorf("New returned %v after start, want right after ctx is canceled", elapsed)
	}
	requireNoConnections(t, name)
}
