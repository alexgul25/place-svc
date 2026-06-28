package postgresql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/alexgul25/wishlist-svc/internal/domain/models"
)

type PlaceStorage struct {
	db *sql.DB
}

func NewPlaceStorage(db *sql.DB) *PlaceStorage {
	return &PlaceStorage{db: db}
}

func (ps *PlaceStorage) CreatePlace(ctx context.Context, userID, name, info string) (models.Place, error) {
	const op = "postgresql.PlaceStorage.CreatePlace"

	query := `
		INSERT INTO places (user_id, name, info, created_at)
		VALUES ($1, $2, $3, NOW())
		RETURNING id, created_at
	`

	place := models.Place{UserID: userID, Name: name, Info: info}
	row := ps.db.QueryRowContext(ctx, query, place.UserID, place.Name, place.Info)
	err := row.Scan(&place.ID, &place.CreatedAt)
	if err != nil {
		return models.Place{}, fmt.Errorf("%s: %w", op, err)
	}

	return place, nil
}

func (ps *PlaceStorage) GetPlacesByUserID(ctx context.Context, userID string) ([]models.Place, error) {
	const op = "postgresql.PlaceStorage.GetPlaceByUserID"

	query := `
		SELECT id, name, info, created_at
		FROM places
		WHERE user_id = $1
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
