package postgresql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/alexgul25/place-svc/internal/outbox"
)

type OutboxStorage struct {
	db         *sql.DB
	serializer outbox.EventSerializer
}

func NewOutboxStorage(db *sql.DB, serializer outbox.EventSerializer) *OutboxStorage {
	return &OutboxStorage{db: db, serializer: serializer}
}

func (os *OutboxStorage) InsertRecord(ctx context.Context, tx *sql.Tx, topic, key string, event any) error {
	const op = "postgresql.OutboxStorage.InsertRecord"

	payload, err := os.serializer.Marshal(event)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	query := `
		INSERT INTO outbox (msg_topic, msg_key, msg_payload, send_status, created_at)
		VALUES ($1, $2, $3, $4, now())
	`

	_, err = tx.ExecContext(ctx, query, topic, key, payload, outbox.StatusPending)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (os *OutboxStorage) SelectPending(ctx context.Context, limit int) ([]outbox.Record, error) {
	const op = "postgresql.OutboxStorage.SelectPending"

	query := `
		SELECT id, msg_topic, msg_key, msg_payload, created_at
		FROM outbox
		WHERE send_status = $1
		ORDER BY created_at
		LIMIT $2
	`

	rows, err := os.db.QueryContext(ctx, query, outbox.StatusPending, limit)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	var records []outbox.Record
	for rows.Next() {
		record := outbox.Record{SendStatus: outbox.StatusPending}
		err := rows.Scan(
			&record.ID,
			&record.Message.Topic,
			&record.Message.Key,
			&record.Message.Payload,
			&record.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return records, nil
}

func (os *OutboxStorage) MarkAsPublished(ctx context.Context, id string) error {
	const op = "postgresql.OutboxStorage.MarkAsPublished"

	query := `
		UPDATE outbox
		SET send_status = $1, published_at = NOW()
		WHERE id = $2
	`

	result, err := os.db.ExecContext(ctx, query, outbox.StatusPublished, id)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("record %s not found", id)
	}

	return nil
}
