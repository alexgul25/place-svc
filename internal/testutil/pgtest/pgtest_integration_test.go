//go:build integration

package pgtest

import (
	"database/sql"
	"os"
	"testing"
)

// База, которую не удалили, осталась бы на сервере после каждого запуска тестов.
func TestNewDatabase_CreatesAndDrops(t *testing.T) {
	dsn, drop, err := NewDatabase(t.Context(), "place_svc_pgtest")
	if err != nil {
		t.Fatalf("NewDatabase returned error: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to open the database: %v", err)
	}
	defer db.Close()

	var name string
	if err := db.QueryRowContext(t.Context(), "SELECT current_database()").Scan(&name); err != nil {
		t.Fatalf("failed to connect to the new database: %v", err)
	}

	// Соединение db остаётся открытым: WITH (FORCE) удаляет базу и в этом случае.
	if err := drop(); err != nil {
		t.Fatalf("drop returned error: %v", err)
	}

	admin, err := sql.Open("pgx", os.Getenv(EnvDSN))
	if err != nil {
		t.Fatalf("failed to open %s: %v", EnvDSN, err)
	}
	defer admin.Close()

	var exists bool
	if err := admin.QueryRowContext(t.Context(), "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name).Scan(&exists); err != nil {
		t.Fatalf("failed to look for database %s: %v", name, err)
	}
	if exists {
		t.Errorf("database %s still exists after drop", name)
	}
}
