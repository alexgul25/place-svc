package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Env             string `envconfig:"ENV"`
	ServiceName     string `envconfig:"SERVICE_NAME" env-default:"place-svc"`
	Database        DatabaseConfig
	GRPCServer      GRPCServerConfig
	OutboxProcessor OutboxProcessorConfig
	KafkaProducer   KafkaProducerConfig
}

type DatabaseConfig struct {
	User     string `envconfig:"DB_USER"`
	Password string `envconfig:"DB_PASSWORD"`
	DbName   string `envconfig:"DB_NAME"`
	Host     string `envconfig:"DB_HOST"`
	Port     int    `envconfig:"DB_PORT"`
}

type GRPCServerConfig struct {
	Port            int           `envconfig:"GRPCSERVER_PORT" env-default:"50051"`
	GracefulTimeout time.Duration `envconfig:"GRACEFUL_TIMEOUT" env-default:"10s"`
}

type OutboxProcessorConfig struct {
	OpTimeout      time.Duration `envconfig:"OUTBOX_PROCESSOR_OP_TIMEOUT" env-default:"5s"`
	SelectInterval time.Duration `envconfig:"OUTBOX_PROCESSOR_SELECT_INTERVAL" env-default:"10s"`
	SelectSize     int           `envconfig:"OUTBOX_PROCESSOR_SELECT_SIZE" env-default:"100"`
}

type KafkaProducerConfig struct {
	BrokersRaw  string `envconfig:"KAFKA_PRODUCER_BROKERS"`
	Brokers     []string
	SendTimeout time.Duration `envconfig:"KAFKA_PRODUCER_SEND_TIMEOUT" env-default:"8s"`
}

func load() (*Config, error) {
	const op = "load"

	err := godotenv.Load()
	if err != nil {
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
	if cfg.KafkaProducer.BrokersRaw == "" {
		return nil, fmt.Errorf("%s: env variable KAFKA_PRODUCER_BROKERS not set", op)
	}

	cfg.KafkaProducer.Brokers = strings.Split(cfg.KafkaProducer.BrokersRaw, ",")

	return cfg, nil
}
