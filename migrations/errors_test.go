package migrations_test

import (
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/alexgul25/place-svc/migrations"
)

// Эти тесты не требуют базы данных и запускаются вместе с unit-тестами.

func TestNewProvider_NilDB(t *testing.T) {
	if _, err := migrations.NewProvider(nil); err == nil {
		t.Error("NewProvider(nil) succeeded, want error")
	}
}

func TestUp_Errors(t *testing.T) {
	t.Run("nil db", func(t *testing.T) {
		if _, err := migrations.Up(t.Context(), nil); err == nil {
			t.Error("Up(nil) succeeded, want error")
		}
	})

	t.Run("closed db", func(t *testing.T) {
		// Мигратор должен завершиться с ошибкой, а не молча пропустить миграции.
		db, err := sql.Open("pgx", "postgres://user:pass@127.0.0.1:1/db")
		if err != nil {
			t.Fatalf("sql.Open returned error: %v", err)
		}
		db.Close()

		if _, err := migrations.Up(t.Context(), db); err == nil {
			t.Error("Up on closed db succeeded, want error")
		}
	})
}
