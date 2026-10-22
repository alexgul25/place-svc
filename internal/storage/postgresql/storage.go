package postgresql

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Storage struct {
	db *sql.DB
}

func NewStorage(dsn string) (*Storage, error) {
	const op = "Storage.NewStorage"

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db}, nil
}

func (s *Storage) DB() *sql.DB {
	return s.db
}

// Ping проверяет соединение с БД. Проверка прерывается, когда отменяется ctx.
func (s *Storage) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Storage) Close() error {
	return s.db.Close()
}
