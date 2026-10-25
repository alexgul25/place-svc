package pgtest

import (
	"strings"
	"testing"
)

// Тесты не требуют базы данных. Ошибка в withDatabase незаметно направила бы
// тесты в общую базу из TEST_DATABASE_URL вместо временной.

func TestWithDatabase(t *testing.T) {
	tests := []struct {
		name, dsn, want string
	}{
		{
			name: "replaces database, keeps credentials and parameters",
			dsn:  "postgres://postgres:secret@localhost:55433/place_svc_test?sslmode=disable",
			want: "postgres://postgres:secret@localhost:55433/tmp_db?sslmode=disable",
		},
		{
			name: "postgresql scheme",
			dsn:  "postgresql://u@db.example.com/base",
			want: "postgresql://u@db.example.com/tmp_db",
		},
		{
			name: "no database in DSN",
			dsn:  "postgres://u@localhost:5432",
			want: "postgres://u@localhost:5432/tmp_db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := withDatabase(tt.dsn, "tmp_db")
			if err != nil {
				t.Fatalf("withDatabase returned error: %v", err)
			}
			if got != tt.want {
				t.Errorf("withDatabase(%q) = %q, want %q", tt.dsn, got, tt.want)
			}
		})
	}
}

func TestWithDatabase_RejectsNonURL(t *testing.T) {
	// Формат "ключ=значение" pgx тоже понимает, но заменить в нём базу так просто нельзя.
	for _, dsn := range []string{"host=localhost dbname=place_svc_test", "mysql://u@localhost/db", "://bad"} {
		if _, err := withDatabase(dsn, "tmp_db"); err == nil {
			t.Errorf("withDatabase(%q) succeeded, want error", dsn)
		}
	}
}

func TestNewDatabase_Errors(t *testing.T) {
	t.Run("TEST_DATABASE_URL is not set", func(t *testing.T) {
		t.Setenv(EnvDSN, "")

		_, _, err := NewDatabase(t.Context(), "x")
		if err == nil || !strings.Contains(err.Error(), "make test-infra-up") {
			t.Errorf("got error %v, want hint about make test-infra-up", err)
		}
	})

	t.Run("DSN is not a URL", func(t *testing.T) {
		// Формат проверяется до подключения к серверу. Если проверить его после
		// CREATE DATABASE, при ошибке созданная база остаётся на сервере.
		t.Setenv(EnvDSN, "host=127.0.0.1 port=1 user=postgres dbname=place_svc_test connect_timeout=2")

		_, _, err := NewDatabase(t.Context(), "x")
		if err == nil || !strings.Contains(err.Error(), "must be a URL") {
			t.Errorf("got error %v, want DSN format error before connecting", err)
		}
	})

	t.Run("server is unreachable", func(t *testing.T) {
		t.Setenv(EnvDSN, "postgres://u:p@127.0.0.1:1/db?connect_timeout=2")

		if _, _, err := NewDatabase(t.Context(), "x"); err == nil {
			t.Error("NewDatabase succeeded, want error")
		}
	})
}
