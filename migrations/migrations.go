// Package migrations содержит SQL-миграции базы данных, встроенные в бинарник.
// Мигратор и тесты применяют одни и те же файлы и не зависят от текущей папки.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed *.sql
var files embed.FS

// NewProvider возвращает goose-провайдер для встроенных миграций.
func NewProvider(db *sql.DB) (*goose.Provider, error) {
	provider, err := goose.NewProvider(goose.DialectPostgres, db, files)
	if err != nil {
		return nil, fmt.Errorf("migrations.NewProvider: %w", err)
	}
	return provider, nil
}

// Up применяет все ещё не применённые миграции и возвращает результат по каждой из них.
func Up(ctx context.Context, db *sql.DB) ([]*goose.MigrationResult, error) {
	const op = "migrations.Up"

	provider, err := NewProvider(db)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	results, err := provider.Up(ctx)
	if err != nil {
		return results, fmt.Errorf("%s: %w", op, err)
	}

	return results, nil
}
