package postgresql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/alexgul25/place-svc/internal/domain/events"
	"github.com/alexgul25/place-svc/internal/domain/models"
	"github.com/alexgul25/place-svc/internal/outbox"
)

type PlaceStorage struct {
	db            *sql.DB
	outboxStorage *OutboxStorage
}

func NewPlaceStorage(db *sql.DB, outboxStorage *OutboxStorage) *PlaceStorage {
	return &PlaceStorage{
		db:            db,
		outboxStorage: outboxStorage,
	}
}

func (ps *PlaceStorage) InsertPlaceWithOutbox(ctx context.Context, place models.Place, event events.PlaceCreated) error {
	const op = "postgresql.PlaceStorage.InsertPlaceWithOutbox"

	tx, err := ps.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	// После Commit откат ничего не делает и возвращает sql.ErrTxDone, а при ошибке
	// до Commit наружу уходит исходная ошибка, поэтому ошибка отката не нужна.
	defer func() { _ = tx.Rollback() }()

	query := `
		INSERT INTO places (id, user_id, place_name, info, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err = tx.ExecContext(ctx, query, place.ID, place.UserID, place.Name, place.Info, place.CreatedAt)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err = ps.outboxStorage.InsertRecord(ctx, tx, outbox.TopicPlaceCreated, place.UserID, event)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (ps *PlaceStorage) GetPlacesByUserID(ctx context.Context, userID string) ([]models.Place, error) {
	const op = "postgresql.PlaceStorage.GetPlacesByUserID"

	// Места идут от старых к новым, а id упорядочивает места,
	// добавленные в одну и ту же микросекунду.
	query := `
		SELECT id, place_name, info, created_at
		FROM places
		WHERE user_id = $1
		ORDER BY created_at, id
	`

	rows, err := ps.db.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	places := []models.Place{}
	for rows.Next() {
		place := models.Place{UserID: userID}

		if err := rows.Scan(&place.ID, &place.Name, &place.Info, &place.CreatedAt); err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}

		places = append(places, place)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return places, nil
}
