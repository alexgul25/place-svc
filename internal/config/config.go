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

func load() (*Config, error) {
	const op = "load"

	err := godotenv.Load()
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	var cfg Config
	err = envconfig.Process("", &cfg)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return &cfg, nil
}

func LoadPlaceService() (*Config, error) {
	const op = "LoadPlaceService"

	cfg, err := load()
	if err != nil {
		return nil, err
	}

	if cfg.Env == "" {
		return nil, fmt.Errorf("%s: env variable ENV not set", op)
	}

	if cfg.Database.User == "" {
		return nil, fmt.Errorf("%s: env variable DB_USER not set", op)
	}
	if cfg.Database.Password == "" {
		return nil, fmt.Errorf("%s: env variable DB_PASSWORD not set", op)
	}
	if cfg.Database.DbName == "" {
		return nil, fmt.Errorf("%s: env variable DB_NAME not set", op)
	}
	if cfg.Database.Host == "" {
		return nil, fmt.Errorf("%s: env variable DB_HOST not set", op)
	}
	if cfg.Database.Port == 0 {
		return nil, fmt.Errorf("%s: env variable DB_PORT not set", op)
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

	if err := validateRanges(cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return cfg, nil
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
		{"DB_PORT", cfg.Database.Port},
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
