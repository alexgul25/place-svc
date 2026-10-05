package postgresql

import (
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
		return nil, fmt.Errorf("%s: %w\n", op, err)
	}

	return &Storage{db}, nil
}

func (s *Storage) DB() *sql.DB {
	return s.db
}

func (s *Storage) Ping() error {
	return s.db.Ping()
}

func (s *Storage) Close() error {
	return s.db.Close()
}
