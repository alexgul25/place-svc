package config_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/alexgul25/place-svc/internal/config"
)

// requiredEnv - обязательные переменные с корректными значениями.
// Порядок важен для читаемых имён подтестов, поэтому это срез, а не map.
var requiredEnv = []struct{ key, value string }{
	{"ENV", "local"},
	{"DB_USER", "user"},
	{"DB_PASSWORD", "secret"},
	{"DB_NAME", "places"},
	{"DB_HOST", "localhost"},
	{"DB_PORT", "5432"},
	{"KAFKA_PRODUCER_BROKERS", "kafka:9092"},
	{"REDIS_CACHE_ADDR", "redis:6379"},
	{"REDIS_CACHE_TTL", "45s"},
}

// optionalEnv - необязательные переменные: у них есть значения по умолчанию или пустое значение допустимо.
var optionalEnv = []string{
	"SERVICE_NAME",
	"GRPCSERVER_PORT",
	"GRACEFUL_TIMEOUT",
	"OUTBOX_PROCESSOR_OP_TIMEOUT",
	"OUTBOX_PROCESSOR_SELECT_INTERVAL",
	"OUTBOX_PROCESSOR_SELECT_SIZE",
	"KAFKA_PRODUCER_SEND_TIMEOUT",
	"REDIS_CACHE_PASSWORD",
	"REDIS_CACHE_USERNAME",
	"REDIS_CACHE_DB",
	"REDIS_CACHE_DIAL_TIMEOUT",
	"REDIS_CACHE_READ_TIMEOUT",
	"REDIS_CACHE_WRITE_TIMEOUT",
}

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// isolate готовит чистое окружение для теста:
//   - убирает все переменные конфига. Пустое значение не подходит: envconfig и godotenv
//     считают такую переменную заданной. t.Setenv запоминает исходное значение
//     и вернёт его после теста, а os.Unsetenv удаляет переменную на время теста;
//   - переходит во временную папку, чтобы godotenv не нашёл посторонний .env.
//
// t.Setenv и t.Chdir несовместимы с t.Parallel: тесты пакета идут последовательно.
func isolate(t *testing.T) {
	t.Helper()

	for _, e := range requiredEnv {
		t.Setenv(e.key, "")
		os.Unsetenv(e.key)
	}
	for _, key := range optionalEnv {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}

	t.Chdir(t.TempDir())
}

// setRequiredEnv задаёт все обязательные переменные корректными значениями.
func setRequiredEnv(t *testing.T) {
	t.Helper()

	for _, e := range requiredEnv {
		t.Setenv(e.key, e.value)
	}
}

// writeDotEnv создаёт .env в текущей (временной) папке.
func writeDotEnv(t *testing.T, content string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(".", ".env"), []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write .env: %v", err)
	}
}

// mustLoad загружает конфиг и роняет тест при ошибке.
func mustLoad(t *testing.T) *config.Config {
	t.Helper()

	cfg, err := config.LoadPlaceService()
	if err != nil {
		t.Fatalf("LoadPlaceService returned error: %v", err)
	}
	return cfg
}

// mustFail загружает конфиг и проверяет, что загрузка упала с ошибкой, которая называет переменную key.
func mustFail(t *testing.T, key string) {
	t.Helper()

	cfg, err := config.LoadPlaceService()
	if err == nil {
		t.Fatalf("expected error for %s, got config %+v", key, cfg)
	}
	// Ошибка должна называть переменную, иначе по логу не понять, что исправлять.
	if !strings.Contains(err.Error(), key) {
		t.Errorf("error %q does not mention %s", err, key)
	}
	if cfg != nil {
		t.Errorf("expected nil config on error, got %+v", cfg)
	}
}

// ----------------------------------------------------------------------------
// LoadPlaceService
// ----------------------------------------------------------------------------

func TestLoadPlaceService_Defaults(t *testing.T) {
	isolate(t)
	setRequiredEnv(t)

	cfg := mustLoad(t)

	if cfg.Env != "local" {
		t.Errorf("Env = %q, want %q", cfg.Env, "local")
	}
	if cfg.ServiceName != "place-svc" {
		t.Errorf("ServiceName = %q, want %q", cfg.ServiceName, "place-svc")
	}

	wantDB := config.DatabaseConfig{User: "user", Password: "secret", DbName: "places", Host: "localhost", Port: 5432}
	if cfg.Database != wantDB {
		t.Errorf("Database = %+v, want %+v", cfg.Database, wantDB)
	}

	wantServer := config.GRPCServerConfig{Port: 50052, GracefulTimeout: 10 * time.Second}
	if cfg.GRPCServer != wantServer {
		t.Errorf("GRPCServer = %+v, want %+v", cfg.GRPCServer, wantServer)
	}

	wantOutbox := config.OutboxProcessorConfig{OpTimeout: 5 * time.Second, SelectInterval: 10 * time.Second, SelectSize: 100}
	if cfg.OutboxProcessor != wantOutbox {
		t.Errorf("OutboxProcessor = %+v, want %+v", cfg.OutboxProcessor, wantOutbox)
	}

	if !slices.Equal(cfg.KafkaProducer.Brokers, []string{"kafka:9092"}) {
		t.Errorf("KafkaProducer.Brokers = %q, want [kafka:9092]", cfg.KafkaProducer.Brokers)
	}
	if cfg.KafkaProducer.SendTimeout != 8*time.Second {
		t.Errorf("KafkaProducer.SendTimeout = %v, want 8s", cfg.KafkaProducer.SendTimeout)
	}

	wantRedis := config.RedisCacheConfig{
		Addr:         "redis:6379",
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		TTL:          45 * time.Second,
	}
	if cfg.RedisCache != wantRedis {
		t.Errorf("RedisCache = %+v, want %+v", cfg.RedisCache, wantRedis)
	}
}

func TestLoadPlaceService_OptionalOverrides(t *testing.T) {
	isolate(t)
	setRequiredEnv(t)
	overrides := map[string]string{
		"SERVICE_NAME":                     "place-svc-2",
		"GRPCSERVER_PORT":                  "6000",
		"GRACEFUL_TIMEOUT":                 "3s",
		"OUTBOX_PROCESSOR_OP_TIMEOUT":      "6s",
		"OUTBOX_PROCESSOR_SELECT_INTERVAL": "500ms",
		"OUTBOX_PROCESSOR_SELECT_SIZE":     "150",
		"KAFKA_PRODUCER_SEND_TIMEOUT":      "7s",
		"REDIS_CACHE_PASSWORD":             "redis-secret",
		"REDIS_CACHE_USERNAME":             "place",
		"REDIS_CACHE_DB":                   "2",
		"REDIS_CACHE_DIAL_TIMEOUT":         "6s",
		"REDIS_CACHE_READ_TIMEOUT":         "4s",
		"REDIS_CACHE_WRITE_TIMEOUT":        "2s",
	}
	for k, v := range overrides {
		t.Setenv(k, v)
	}

	cfg := mustLoad(t)

	if cfg.ServiceName != "place-svc-2" {
		t.Errorf("ServiceName = %q, want %q", cfg.ServiceName, "place-svc-2")
	}

	wantServer := config.GRPCServerConfig{Port: 6000, GracefulTimeout: 3 * time.Second}
	if cfg.GRPCServer != wantServer {
		t.Errorf("GRPCServer = %+v, want %+v", cfg.GRPCServer, wantServer)
	}

	wantOutbox := config.OutboxProcessorConfig{OpTimeout: 6 * time.Second, SelectInterval: 500 * time.Millisecond, SelectSize: 150}
	if cfg.OutboxProcessor != wantOutbox {
		t.Errorf("OutboxProcessor = %+v, want %+v", cfg.OutboxProcessor, wantOutbox)
	}

	if cfg.KafkaProducer.SendTimeout != 7*time.Second {
		t.Errorf("KafkaProducer.SendTimeout = %v, want 7s", cfg.KafkaProducer.SendTimeout)
	}

	wantRedis := config.RedisCacheConfig{
		Addr:         "redis:6379",
		Password:     "redis-secret",
		Username:     "place",
		DB:           2,
		DialTimeout:  6 * time.Second,
		ReadTimeout:  4 * time.Second,
		WriteTimeout: 2 * time.Second,
		TTL:          45 * time.Second,
	}
	if cfg.RedisCache != wantRedis {
		t.Errorf("RedisCache = %+v, want %+v", cfg.RedisCache, wantRedis)
	}
}

func TestLoadPlaceService_MissingRequired(t *testing.T) {
	for _, missing := range requiredEnv {
		t.Run(missing.key, func(t *testing.T) {
			isolate(t)
			setRequiredEnv(t)
			os.Unsetenv(missing.key)

			mustFail(t, missing.key)
		})
	}
}

func TestLoadPlaceService_InvalidValues(t *testing.T) {
	tests := []struct{ key, value string }{
		{"DB_PORT", "not-a-number"},
		{"GRPCSERVER_PORT", "not-a-number"},
		{"GRACEFUL_TIMEOUT", "soon"},
		{"OUTBOX_PROCESSOR_OP_TIMEOUT", "5"},
		{"OUTBOX_PROCESSOR_SELECT_INTERVAL", "often"},
		{"OUTBOX_PROCESSOR_SELECT_SIZE", "many"},
		{"KAFKA_PRODUCER_SEND_TIMEOUT", "8 seconds"},
		{"REDIS_CACHE_DB", "first"},
		{"REDIS_CACHE_DIAL_TIMEOUT", "5"},
		{"REDIS_CACHE_READ_TIMEOUT", "3"},
		{"REDIS_CACHE_WRITE_TIMEOUT", "3"},
		{"REDIS_CACHE_TTL", "1day"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			isolate(t)
			setRequiredEnv(t)
			t.Setenv(tt.key, tt.value)

			mustFail(t, tt.key)
		})
	}
}

// Значения, которые разбираются без ошибок, но с которыми сервис упал бы при запуске
// или работал бы неправильно.
func TestLoadPlaceService_OutOfRange(t *testing.T) {
	tests := []struct {
		key, value string
		// Что случилось бы без проверки.
		why string
	}{
		{"DB_PORT", "70000", "connection string with invalid port"},
		{"DB_PORT", "-1", "connection string with invalid port"},
		{"GRPCSERVER_PORT", "0", "server listens on a random port"},
		{"GRPCSERVER_PORT", "70000", "panic in MustRun"},
		{"GRPCSERVER_PORT", "-1", "panic in MustRun"},
		{"GRACEFUL_TIMEOUT", "0s", "no time to finish requests on shutdown"},
		{"GRACEFUL_TIMEOUT", "-1s", "no time to finish requests on shutdown"},
		{"OUTBOX_PROCESSOR_OP_TIMEOUT", "0s", "every outbox query fails with deadline exceeded"},
		{"OUTBOX_PROCESSOR_SELECT_INTERVAL", "0s", "panic: non-positive interval for NewTicker"},
		{"OUTBOX_PROCESSOR_SELECT_INTERVAL", "-1s", "panic: non-positive interval for NewTicker"},
		{"OUTBOX_PROCESSOR_SELECT_SIZE", "0", "LIMIT 0: outbox never sends anything"},
		{"OUTBOX_PROCESSOR_SELECT_SIZE", "-1", "LIMIT -1: every outbox query fails"},
		{"KAFKA_PRODUCER_SEND_TIMEOUT", "0s", "sarama rejects the producer config"},
		{"REDIS_CACHE_DIAL_TIMEOUT", "0s", "Redis ping fails with deadline exceeded"},
		{"REDIS_CACHE_DB", "-1", "go-redis silently uses DB 0"},
		{"REDIS_CACHE_TTL", "-1s", "cache entries need a positive TTL"},
	}

	for _, tt := range tests {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			isolate(t)
			setRequiredEnv(t)
			t.Setenv(tt.key, tt.value)

			cfg, err := config.LoadPlaceService()
			if err == nil {
				t.Fatalf("expected error (otherwise: %s), got config", tt.why)
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Errorf("error %q does not mention %s", err, tt.key)
			}
			if cfg != nil {
				t.Errorf("expected nil config on error, got %+v", cfg)
			}
		})
	}
}

// Граничные значения допустимы: проверки не должны отсекать лишнее.
func TestLoadPlaceService_BoundaryValues(t *testing.T) {
	tests := []struct{ key, value string }{
		{"DB_PORT", "1"},
		{"DB_PORT", "65535"},
		{"GRPCSERVER_PORT", "1"},
		{"GRPCSERVER_PORT", "65535"},
		{"GRACEFUL_TIMEOUT", "1ns"},
		{"OUTBOX_PROCESSOR_OP_TIMEOUT", "1ns"},
		{"OUTBOX_PROCESSOR_SELECT_INTERVAL", "1ns"},
		{"OUTBOX_PROCESSOR_SELECT_SIZE", "1"},
		{"KAFKA_PRODUCER_SEND_TIMEOUT", "1ns"},
		{"REDIS_CACHE_DB", "0"},
		{"REDIS_CACHE_DIAL_TIMEOUT", "1ns"},
		{"REDIS_CACHE_TTL", "1ns"},
		// go-redis понимает 0 (значение по умолчанию), -1 (без таймаута) и -2 (без дедлайнов),
		// поэтому таймауты чтения и записи не проверяются.
		{"REDIS_CACHE_READ_TIMEOUT", "-1ns"},
		{"REDIS_CACHE_WRITE_TIMEOUT", "0s"},
	}

	for _, tt := range tests {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			isolate(t)
			setRequiredEnv(t)
			t.Setenv(tt.key, tt.value)

			mustLoad(t)
		})
	}
}

func TestLoadPlaceService_KafkaBrokers(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{name: "one broker", raw: "kafka:9092", want: []string{"kafka:9092"}},
		{name: "several brokers", raw: "kafka-1:9092,kafka-2:9092", want: []string{"kafka-1:9092", "kafka-2:9092"}},
		{
			// С пробелом адрес " kafka-2:9092" не резолвится: sarama пишет
			// "lookup  kafka-2: no such host", а с единственным брокером сервис не стартует.
			name: "spaces around addresses",
			raw:  "kafka-1:9092, kafka-2:9092",
			want: []string{"kafka-1:9092", "kafka-2:9092"},
		},
		{name: "empty items", raw: " kafka-1:9092 ,, kafka-2:9092 , ", want: []string{"kafka-1:9092", "kafka-2:9092"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolate(t)
			setRequiredEnv(t)
			t.Setenv("KAFKA_PRODUCER_BROKERS", tt.raw)

			cfg := mustLoad(t)
			if !slices.Equal(cfg.KafkaProducer.Brokers, tt.want) {
				t.Errorf("KafkaProducer.Brokers = %q, want %q", cfg.KafkaProducer.Brokers, tt.want)
			}
			if cfg.KafkaProducer.BrokersRaw != tt.raw {
				t.Errorf("KafkaProducer.BrokersRaw = %q, want %q", cfg.KafkaProducer.BrokersRaw, tt.raw)
			}
		})
	}
}

// Список без единого адреса равносилен незаданной переменной.
func TestLoadPlaceService_KafkaBrokersOnlySeparators(t *testing.T) {
	isolate(t)
	setRequiredEnv(t)
	t.Setenv("KAFKA_PRODUCER_BROKERS", " , ,")

	mustFail(t, "KAFKA_PRODUCER_BROKERS")
}

// ----------------------------------------------------------------------------
// Файл .env
// ----------------------------------------------------------------------------

func TestLoadPlaceService_DotEnv(t *testing.T) {
	isolate(t)
	writeDotEnv(t, strings.Join([]string{
		"ENV=dev",
		"DB_USER=file-user",
		"DB_PASSWORD=file-secret",
		"DB_NAME=file-db",
		"DB_HOST=file-host",
		"DB_PORT=6432",
		"KAFKA_PRODUCER_BROKERS=file-kafka-1:9092,file-kafka-2:9092",
		"REDIS_CACHE_ADDR=file-redis:6379",
		"REDIS_CACHE_TTL=1m",
		"OUTBOX_PROCESSOR_SELECT_SIZE=50",
	}, "\n"))

	cfg := mustLoad(t)

	wantDB := config.DatabaseConfig{User: "file-user", Password: "file-secret", DbName: "file-db", Host: "file-host", Port: 6432}
	if cfg.Env != "dev" || cfg.Database != wantDB {
		t.Errorf("config is not loaded from .env: %+v", cfg)
	}
	if !slices.Equal(cfg.KafkaProducer.Brokers, []string{"file-kafka-1:9092", "file-kafka-2:9092"}) {
		t.Errorf("KafkaProducer.Brokers = %q, want brokers from .env", cfg.KafkaProducer.Brokers)
	}
	if cfg.RedisCache.Addr != "file-redis:6379" || cfg.RedisCache.TTL != time.Minute {
		t.Errorf("RedisCache = %+v, want values from .env", cfg.RedisCache)
	}
	if cfg.OutboxProcessor.SelectSize != 50 {
		t.Errorf("OutboxProcessor.SelectSize = %d, want 50", cfg.OutboxProcessor.SelectSize)
	}
}

func TestLoadPlaceService_EnvOverridesDotEnv(t *testing.T) {
	isolate(t)
	setRequiredEnv(t)
	t.Setenv("DB_HOST", "from-env")
	writeDotEnv(t, "DB_HOST=from-file\n")

	if got := mustLoad(t).Database.Host; got != "from-env" {
		t.Errorf("Database.Host = %q, want value from environment %q", got, "from-env")
	}
}

func TestLoadPlaceService_InvalidDotEnv(t *testing.T) {
	isolate(t)
	setRequiredEnv(t)
	// Незакрытая кавычка: godotenv не может разобрать файл.
	writeDotEnv(t, "DB_HOST=\"unterminated\n")

	cfg, err := config.LoadPlaceService()
	if err == nil {
		t.Fatal("expected error for broken .env, got nil")
	}
	if cfg != nil {
		t.Errorf("expected nil config on error, got %+v", cfg)
	}
}

// ----------------------------------------------------------------------------
// DSN
// ----------------------------------------------------------------------------

func TestDatabaseConfig_DSN(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.DatabaseConfig
	}{
		{
			name: "simple values",
			cfg:  config.DatabaseConfig{User: "user", Password: "secret", DbName: "places", Host: "localhost", Port: 5432},
		},
		{
			// Спецсимволы URL в пароле должны экранироваться, а не ломать строку подключения.
			name: "special characters in password",
			cfg:  config.DatabaseConfig{User: "user", Password: "p@ss:w/rd?#%", DbName: "places", Host: "localhost", Port: 5432},
		},
		{
			name: "special characters in user and database",
			cfg:  config.DatabaseConfig{User: "us er@corp", Password: "secret", DbName: "my db", Host: "db.example.com", Port: 6432},
		},
		{
			name: "IPv6 host",
			cfg:  config.DatabaseConfig{User: "user", Password: "secret", DbName: "places", Host: "::1", Port: 5432},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := tt.cfg.DSN()

			// Строку разбирает тот же драйвер, что и сервис: значения должны вернуться без искажений.
			parsed, err := pgconn.ParseConfig(dsn)
			if err != nil {
				t.Fatalf("pgconn.ParseConfig(%q) returned error: %v", dsn, err)
			}

			if parsed.User != tt.cfg.User {
				t.Errorf("user = %q, want %q", parsed.User, tt.cfg.User)
			}
			if parsed.Password != tt.cfg.Password {
				t.Errorf("password = %q, want %q", parsed.Password, tt.cfg.Password)
			}
			if parsed.Database != tt.cfg.DbName {
				t.Errorf("database = %q, want %q", parsed.Database, tt.cfg.DbName)
			}
			if parsed.Host != tt.cfg.Host {
				t.Errorf("host = %q, want %q", parsed.Host, tt.cfg.Host)
			}
			if int(parsed.Port) != tt.cfg.Port {
				t.Errorf("port = %d, want %d", parsed.Port, tt.cfg.Port)
			}
		})
	}
}

func TestDatabaseConfig_DSN_Format(t *testing.T) {
	cfg := config.DatabaseConfig{User: "user", Password: "secret", DbName: "places", Host: "localhost", Port: 5432}

	const want = "postgres://user:secret@localhost:5432/places"
	if got := cfg.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}
