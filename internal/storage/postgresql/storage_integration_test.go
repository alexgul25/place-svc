//go:build integration

package postgresql_test

import (
	"testing"

	"github.com/alexgul25/place-svc/internal/storage/postgresql"
)

func TestStorage_PingAndClose(t *testing.T) {
	storage, err := postgresql.NewStorage(testDSN)
	if err != nil {
		t.Fatalf("NewStorage returned error: %v", err)
	}

	if err := storage.Ping(t.Context()); err != nil {
		t.Fatalf("Ping returned error: %v", err)
	}

	// После Close соединение больше не работает: сервис закрывает его при остановке.
	if err := storage.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := storage.Ping(t.Context()); err == nil {
		t.Error("Ping after Close succeeded, want error")
	}
}
