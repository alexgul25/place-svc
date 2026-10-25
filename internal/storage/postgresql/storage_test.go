package postgresql_test

import (
	"testing"

	"github.com/alexgul25/place-svc/internal/storage/postgresql"
)

// Эти тесты не требуют базы данных и запускаются вместе с unit-тестами.

func TestStorage_InvalidDSN(t *testing.T) {
	// Драйвер pgx не разбирает строку подключения в sql.Open: ошибка появляется
	// при первом обращении к базе. Поэтому сервис проверяет соединение через Ping при старте.
	storage, err := postgresql.NewStorage("postgres://user:pass@localhost:not-a-port/db")
	if err != nil {
		t.Fatalf("NewStorage returned error: %v", err)
	}
	defer storage.Close()

	if err := storage.Ping(t.Context()); err == nil {
		t.Error("Ping with invalid DSN succeeded, want error")
	}
}

func TestStorage_PingUnreachable(t *testing.T) {
	// Порт 1 на localhost закрыт: соединение отклоняется сразу.
	storage, err := postgresql.NewStorage("postgres://user:pass@127.0.0.1:1/db?connect_timeout=2")
	if err != nil {
		t.Fatalf("NewStorage returned error: %v", err)
	}
	defer storage.Close()

	if err := storage.Ping(t.Context()); err == nil {
		t.Error("Ping to unreachable database succeeded, want error")
	}
}
