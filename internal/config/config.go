package config

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Env             string `envconfig:"ENV"`
	ServiceName     string `envconfig:"SERVICE_NAME" default:"place-svc"`
	Database        DatabaseConfig
	GRPCServer      GRPCServerConfig
	OutboxProcessor OutboxProcessorConfig
	KafkaProducer   KafkaProducerConfig
	RedisCache      RedisCacheConfig
}

type DatabaseConfig struct {
	User     string `envconfig:"DB_USER"`
	Password string `envconfig:"DB_PASSWORD"`
	DbName   string `envconfig:"DB_NAME"`
	Host     string `envconfig:"DB_HOST"`
	Port     int    `envconfig:"DB_PORT"`
}

func (c *DatabaseConfig) DSN() string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password),
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		Path:   c.DbName,
	}
	return u.String()
}

type GRPCServerConfig struct {
	Port            int           `envconfig:"GRPCSERVER_PORT" default:"50052"`
	GracefulTimeout time.Duration `envconfig:"GRACEFUL_TIMEOUT" default:"10s"`
}

type OutboxProcessorConfig struct {
	OpTimeout      time.Duration `envconfig:"OUTBOX_PROCESSOR_OP_TIMEOUT" default:"5s"`
	SelectInterval time.Duration `envconfig:"OUTBOX_PROCESSOR_SELECT_INTERVAL" default:"10s"`
	SelectSize     int           `envconfig:"OUTBOX_PROCESSOR_SELECT_SIZE" default:"100"`
}

type KafkaProducerConfig struct {
	BrokersRaw  string `envconfig:"KAFKA_PRODUCER_BROKERS"`
	Brokers     []string
	SendTimeout time.Duration `envconfig:"KAFKA_PRODUCER_SEND_TIMEOUT" default:"8s"`
}

type RedisCacheConfig struct {
	Addr         string        `envconfig:"REDIS_CACHE_ADDR"`
	Password     string        `envconfig:"REDIS_CACHE_PASSWORD"`
	Username     string        `envconfig:"REDIS_CACHE_USERNAME"`
	DB           int           `envconfig:"REDIS_CACHE_DB" default:"0"`
	DialTimeout  time.Duration `envconfig:"REDIS_CACHE_DIAL_TIMEOUT" default:"5s"`
	ReadTimeout  time.Duration `envconfig:"REDIS_CACHE_READ_TIMEOUT" default:"3s"`
	WriteTimeout time.Duration `envconfig:"REDIS_CACHE_WRITE_TIMEOUT" default:"3s"`
	TTL          time.Duration `envconfig:"REDIS_CACHE_TTL"`
}

// load читает .env (если он есть) и переменные окружения в spec.
func load(spec any) error {
	const op = "load"

	err := godotenv.Load()
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%s: %w", op, err)
	}

	if err := envconfig.Process("", spec); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func LoadPlaceService() (*Config, error) {
	const op = "LoadPlaceService"

	var cfg Config
	if err := load(&cfg); err != nil {
		return nil, err
	}

	if cfg.Env == "" {
		return nil, fmt.Errorf("%s: env variable ENV not set", op)
	}

	if err := validateDatabase(&cfg.Database); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	cfg.KafkaProducer.Brokers = splitList(cfg.KafkaProducer.BrokersRaw)
	if len(cfg.KafkaProducer.Brokers) == 0 {
		return nil, fmt.Errorf("%s: env variable KAFKA_PRODUCER_BROKERS not set", op)
	}

	if cfg.RedisCache.Addr == "" {
		return nil, fmt.Errorf("%s: env variable REDIS_CACHE_ADDR not set", op)
	}
	if cfg.RedisCache.TTL == 0 {
		return nil, fmt.Errorf("%s: env variable REDIS_CACHE_TTL not set", op)
	}

	if err := validateRanges(&cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &cfg, nil
}

// LoadMigrator загружает настройки мигратора. Ему нужна только БД, поэтому
// остальные переменные сервиса (Kafka, Redis, gRPC-сервер) не читаются и не проверяются.
func LoadMigrator() (*DatabaseConfig, error) {
	const op = "LoadMigrator"

	var db DatabaseConfig
	if err := load(&db); err != nil {
		return nil, err
	}

	if err := validateDatabase(&db); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &db, nil
}

// validateDatabase проверяет, что заданы все настройки БД и порт допустим.
func validateDatabase(db *DatabaseConfig) error {
	required := []struct {
		name string
		set  bool
	}{
		{"DB_USER", db.User != ""},
		{"DB_PASSWORD", db.Password != ""},
		{"DB_NAME", db.DbName != ""},
		{"DB_HOST", db.Host != ""},
		{"DB_PORT", db.Port != 0},
	}
	for _, r := range required {
		if !r.set {
			return fmt.Errorf("env variable %s not set", r.name)
		}
	}

	if db.Port < 1 || db.Port > 65535 {
		return fmt.Errorf("env variable DB_PORT must be between 1 and 65535, got %d", db.Port)
	}
	return nil
}

// validateRanges проверяет значения, которые разбираются без ошибок, но с которыми
// сервис упал бы при запуске или работал бы неправильно: например, нулевой интервал
// outbox-процессора вызывал панику в time.NewTicker, а GRPCSERVER_PORT=0 открывал
// сервер на случайном порту.
//
// Таймауты чтения и записи Redis не проверяются: go-redis понимает 0 (значение
// по умолчанию), -1 (без таймаута) и -2 (без дедлайнов).
func validateRanges(cfg *Config) error {
	ports := []struct {
		name  string
		value int
	}{
		{"GRPCSERVER_PORT", cfg.GRPCServer.Port},
	}
	for _, p := range ports {
		if p.value < 1 || p.value > 65535 {
			return fmt.Errorf("env variable %s must be between 1 and 65535, got %d", p.name, p.value)
		}
	}

	durations := []struct {
		name  string
		value time.Duration
	}{
		{"GRACEFUL_TIMEOUT", cfg.GRPCServer.GracefulTimeout},
		{"OUTBOX_PROCESSOR_OP_TIMEOUT", cfg.OutboxProcessor.OpTimeout},
		{"OUTBOX_PROCESSOR_SELECT_INTERVAL", cfg.OutboxProcessor.SelectInterval},
		{"KAFKA_PRODUCER_SEND_TIMEOUT", cfg.KafkaProducer.SendTimeout},
		{"REDIS_CACHE_DIAL_TIMEOUT", cfg.RedisCache.DialTimeout},
		{"REDIS_CACHE_TTL", cfg.RedisCache.TTL},
	}
	for _, d := range durations {
		if d.value <= 0 {
			return fmt.Errorf("env variable %s must be positive, got %s", d.name, d.value)
		}
	}

	if cfg.OutboxProcessor.SelectSize <= 0 {
		return fmt.Errorf("env variable OUTBOX_PROCESSOR_SELECT_SIZE must be positive, got %d", cfg.OutboxProcessor.SelectSize)
	}
	if cfg.RedisCache.DB < 0 {
		return fmt.Errorf("env variable REDIS_CACHE_DB must not be negative, got %d", cfg.RedisCache.DB)
	}

	return nil
}

// splitList разбирает список через запятую: обрезает пробелы вокруг элементов
// и пропускает пустые элементы
// "a, b," ---> [a b]
func splitList(raw string) []string {
	var items []string
	for item := range strings.SplitSeq(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}
