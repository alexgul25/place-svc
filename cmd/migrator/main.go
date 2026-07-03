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
	cfg, err := config.LoadPlaceService()
	if err != nil {
		slog.Error("failed to load config files", slog.Any("error", err))
		os.Exit(1)
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.DbName,
	)

	db, err := goose.OpenDBWithDriver("pgx", connStr)
	if err != nil {
		slog.Error("failed to open DB", slog.Any("error", err))
		os.Exit(1)
	}
	defer db.Close()

	pathToMigrations := "./migrations"
	if err := goose.Up(db, pathToMigrations); err != nil {
		slog.Error("failed to up migrations", slog.Any("error", err))
		os.Exit(1)
	}

	slog.Info("migrations applied successfully")
}
