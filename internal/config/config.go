package config

import (
	"fmt"
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Env        string `envconfig:"ENV"`
	Database   DatabaseConfig
	GRPCServer GRPCServerConfig
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
	APIKey          string        `envconfig:"API_KEY"`
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

func LoadWishlistService() (*Config, error) {
	const op = "LoadWishlistService"

	cfg, err := load()
	if err != nil {
		return nil, err
	}

	if cfg.Env == "" {
		return nil, fmt.Errorf("%s env variable not set: ENV", op)
	}
	if cfg.GRPCServer.APIKey == "" {
		return nil, fmt.Errorf("%s env variable not set: API_KEY", op)
	}
	if cfg.Database.User == "" {
		return nil, fmt.Errorf("%s env variable not set: DB_USER", op)
	}
	if cfg.Database.Password == "" {
		return nil, fmt.Errorf("%s env variable not set: DB_PASSWORD", op)
	}
	if cfg.Database.DbName == "" {
		return nil, fmt.Errorf("%s env variable not set: DB_NAME", op)
	}
	if cfg.Database.Host == "" {
		return nil, fmt.Errorf("%s env variable not set: DB_HOST", op)
	}
	if cfg.Database.Port == 0 {
		return nil, fmt.Errorf("%s env variable not set: DB_PORT", op)
	}

	return cfg, nil
}
