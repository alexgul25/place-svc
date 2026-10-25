//go:build integration

package migrations_test

import (
	"database/sql"
	"slices"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/alexgul25/place-svc/internal/testutil/pgtest"
	"github.com/alexgul25/place-svc/migrations"
)

// latestVersion - номер последней миграции. При добавлении миграции его нужно увеличить.
const latestVersion = 3

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// newDB создаёт для теста отдельную пустую базу и возвращает соединение с ней.
func newDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn, drop, err := pgtest.NewDatabase(t.Context(), "place_svc_migrations")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := drop(); err != nil {
			t.Error(err)
		}
	})

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	return db
}

// newProvider возвращает goose-провайдер со встроенными миграциями.
func newProvider(t *testing.T, db *sql.DB) *goose.Provider {
	t.Helper()

	provider, err := migrations.NewProvider(db)
	if err != nil {
		t.Fatalf("NewProvider returned error: %v", err)
	}
	return provider
}

// names выполняет запрос, возвращающий один текстовый столбец, и собирает значения.
func names(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()

	rows, err := db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatalf("failed to execute %q: %v", query, err)
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan %q: %v", query, err)
		}
		result = append(result, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to execute %q: %v", query, err)
	}
	return result
}

// tables возвращает таблицы схемы public.
func tables(t *testing.T, db *sql.DB) []string {
	t.Helper()
	return names(t, db, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
}

// ----------------------------------------------------------------------------
// Тесты
// ----------------------------------------------------------------------------

func TestUp_AppliesAllMigrations(t *testing.T) {
	db := newDB(t)

	results, err := migrations.Up(t.Context(), db)
	if err != nil {
		t.Fatalf("Up returned error: %v", err)
	}
	if len(results) != latestVersion {
		t.Errorf("applied %d migrations, want %d", len(results), latestVersion)
	}

	// Повторный запуск ничего не делает: мигратор можно безопасно запускать при каждом деплое.
	results, err = migrations.Up(t.Context(), db)
	if err != nil {
		t.Fatalf("second Up returned error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("second Up applied %d migrations, want 0", len(results))
	}

	version, err := newProvider(t, db).GetDBVersion(t.Context())
	if err != nil {
		t.Fatalf("GetDBVersion returned error: %v", err)
	}
	if version != latestVersion {
		t.Errorf("database version = %d, want %d", version, latestVersion)
	}
}

func TestDownAndUpAgain(t *testing.T) {
	db := newDB(t)
	provider := newProvider(t, db)

	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("Up returned error: %v", err)
	}

	// Откат последней миграции убирает её индекс, таблицы остаются.
	if _, err := provider.Down(t.Context()); err != nil {
		t.Fatalf("Down returned error: %v", err)
	}
	indexes := names(t, db, "SELECT indexname FROM pg_indexes WHERE tablename = 'places' ORDER BY indexname")
	if !slices.Equal(indexes, []string{"places_pkey"}) {
		t.Errorf("indexes of places after rolling back 003 = %q, want only places_pkey", indexes)
	}

	// Каждая миграция откатывается: после отката всех остаётся только служебная таблица goose.
	if _, err := provider.DownTo(t.Context(), 0); err != nil {
		t.Fatalf("DownTo(0) returned error: %v", err)
	}
	if got := tables(t, db); !slices.Equal(got, []string{"goose_db_version"}) {
		t.Errorf("tables after rollback = %q, want only goose_db_version", got)
	}

	// После отката миграции снова применяются: Down-части не оставляют мусора.
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("Up after rollback returned error: %v", err)
	}
	if got := tables(t, db); !slices.Equal(got, []string{"goose_db_version", "outbox", "places"}) {
		t.Errorf("tables after second Up = %q", got)
	}
}

func TestIndexes(t *testing.T) {
	db := newDB(t)
	if _, err := migrations.Up(t.Context(), db); err != nil {
		t.Fatalf("Up returned error: %v", err)
	}

	got := names(t, db,
		"SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename IN ('places', 'outbox') ORDER BY indexname")
	want := []string{
		// Список мест пользователя: поиск по user_id, сортировка по created_at.
		"CREATE INDEX idx_places_user_id_created_at ON public.places USING btree (user_id, created_at)",
		"CREATE UNIQUE INDEX outbox_pkey ON public.outbox USING btree (id)",
		"CREATE UNIQUE INDEX places_pkey ON public.places USING btree (id)",
	}
	if !slices.Equal(got, want) {
		t.Errorf("indexes = %q, want %q", got, want)
	}
}

// На большой таблице индекс можно построить заранее без блокировки записи,
// и тогда миграция 003 его не трогает.
func TestMigration003_SkipsIndexBuiltConcurrently(t *testing.T) {
	db := newDB(t)
	provider := newProvider(t, db)
	if _, err := provider.UpTo(t.Context(), 2); err != nil {
		t.Fatalf("UpTo(2) returned error: %v", err)
	}

	// CREATE INDEX CONCURRENTLY нельзя выполнить в транзакции, поэтому так его строят вручную.
	if _, err := db.ExecContext(t.Context(),
		"CREATE INDEX CONCURRENTLY idx_places_user_id_created_at ON places (user_id, created_at)"); err != nil {
		t.Fatalf("failed to build the index concurrently: %v", err)
	}

	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("Up after building the index by hand returned error: %v", err)
	}
	if version, err := provider.GetDBVersion(t.Context()); err != nil || version != 3 {
		t.Errorf("database version = %d (%v), want 3", version, err)
	}
}
