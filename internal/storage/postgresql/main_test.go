//go:build integration

package postgresql_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alexgul25/place-svc/internal/infrastructure/serializer"
	"github.com/alexgul25/place-svc/internal/outbox"
	"github.com/alexgul25/place-svc/internal/storage/postgresql"
	"github.com/alexgul25/place-svc/internal/testutil/pgtest"
	"github.com/alexgul25/place-svc/migrations"
)

// Отдельная база, созданная для тестов этого пакета: строка подключения и соединение.
var (
	testDSN string
	testDB  *sql.DB
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run отделён от TestMain, чтобы отложенные вызовы выполнились до os.Exit.
func run(m *testing.M) int {
	ctx := context.Background()

	dsn, drop, err := pgtest.NewDatabase(ctx, "place_svc_storage")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() {
		if err := drop(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()

	storage, err := postgresql.NewStorage(dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer storage.Close()

	// Схему создают те же миграции, что применяются в проде.
	if _, err := migrations.Up(ctx, storage.DB()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	testDSN, testDB = dsn, storage.DB()
	return m.Run()
}

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// newStorages очищает таблицы и возвращает хранилища мест и outbox с JSON-сериализатором,
// как в сервисе: каждый тест начинает с пустой базы.
// Тесты пакета работают с одной базой, поэтому не используют t.Parallel.
func newStorages(t *testing.T) (*postgresql.PlaceStorage, *postgresql.OutboxStorage) {
	t.Helper()
	return newStoragesWithSerializer(t, serializer.JSONSerializer{})
}

func newStoragesWithSerializer(t *testing.T, s outbox.EventSerializer) (*postgresql.PlaceStorage, *postgresql.OutboxStorage) {
	t.Helper()

	if _, err := testDB.ExecContext(t.Context(), "TRUNCATE places, outbox"); err != nil {
		t.Fatalf("failed to truncate tables: %v", err)
	}
	outboxStorage := postgresql.NewOutboxStorage(testDB, s)
	return postgresql.NewPlaceStorage(testDB, outboxStorage), outboxStorage
}

// countRows возвращает число строк в таблице.
func countRows(t *testing.T, table string) int {
	t.Helper()

	var n int
	// Имя таблицы задаёт сам тест, а не внешний ввод.
	if err := testDB.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("failed to count rows in %s: %v", table, err)
	}
	return n
}

// outboxRow - запись outbox так, как она лежит в таблице.
type outboxRow struct {
	ID          string
	Topic       string
	Key         string
	Payload     string
	Status      string
	CreatedAt   time.Time
	PublishedAt sql.NullTime
}

// outboxRows возвращает все записи outbox от старых к новым.
func outboxRows(t *testing.T) []outboxRow {
	t.Helper()

	rows, err := testDB.QueryContext(t.Context(), `
		SELECT id, msg_topic, msg_key, msg_payload, send_status, created_at, published_at
		FROM outbox
		ORDER BY created_at, id
	`)
	if err != nil {
		t.Fatalf("failed to select outbox: %v", err)
	}
	defer rows.Close()

	var records []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.ID, &r.Topic, &r.Key, &r.Payload, &r.Status, &r.CreatedAt, &r.PublishedAt); err != nil {
			t.Fatalf("failed to scan outbox row: %v", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to select outbox: %v", err)
	}
	return records
}
