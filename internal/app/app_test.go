package app_test

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/alexgul25/place-svc/internal/app"
	"github.com/alexgul25/place-svc/internal/config"
)

// Этот тест не требует внешних серверов и запускается вместе с unit-тестами.

func TestNew_DatabaseUnavailable(t *testing.T) {
	cfg := &config.Config{
		Env: "local",
		// Порт 1 на localhost закрыт: соединение отклоняется сразу.
		Database: config.DatabaseConfig{User: "user", Password: "pass", DbName: "db", Host: "127.0.0.1", Port: 1},
	}

	// Сервис не запускается без БД: ошибка видна сразу при старте, а не на первом запросе.
	application, err := app.New(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	if err == nil || !strings.Contains(err.Error(), "failed to ping storage") {
		t.Fatalf("New returned error %v, want failed to ping storage", err)
	}
	if application != nil {
		t.Errorf("New returned %+v with an error, want nil", application)
	}
}
