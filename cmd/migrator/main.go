package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/alexgul25/place-svc/internal/config"
	"github.com/alexgul25/place-svc/migrations"
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

	db, err := sql.Open("pgx", cfg.Database.DSN())
	if err != nil {
		return fmt.Errorf("failed to open DB: %w", err)
	}
	defer db.Close()

	// Миграции встроены в бинарник: мигратор не зависит от текущей папки.
	results, err := migrations.Up(context.Background(), db)
	for _, r := range results {
		slog.Info("migration applied", slog.String("file", r.Source.Path), slog.Duration("duration", r.Duration))
	}
	if err != nil {
		return fmt.Errorf("failed to up migrations: %w", err)
	}

	return nil
}
