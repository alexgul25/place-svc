package main

import (
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/alexgul25/place-svc/internal/config"
)

func main() {
	if err := run(); err != nil {
		slog.Error("migrator failed", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("migrations applied successfully")
}

func run() error {
	cfg, err := config.LoadPlaceService()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	db, err := goose.OpenDBWithDriver("pgx", cfg.Database.DSN())
	if err != nil {
		return fmt.Errorf("failed to open DB: %w", err)
	}
	defer db.Close()

	pathToMigrations := "./migrations"
	if err := goose.Up(db, pathToMigrations); err != nil {
		return fmt.Errorf("failed to up migrations: %w", err)
	}

	return nil
}
