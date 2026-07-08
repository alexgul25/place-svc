package placelogic

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/alexgul25/place-svc/internal/domain/events"
	"github.com/alexgul25/place-svc/internal/domain/models"
)

type PlaceRepository interface {
	InsertPlaceWithOutbox(ctx context.Context, place models.Place, event events.PlaceCreated) error
	GetPlacesByUserID(ctx context.Context, userID string) ([]models.Place, error)
}

type PlaceLogic struct {
	log       *slog.Logger
	placeRepo PlaceRepository
}

func New(log *slog.Logger, placeRepo PlaceRepository) *PlaceLogic {
	return &PlaceLogic{log: log, placeRepo: placeRepo}
}

func (pl *PlaceLogic) AddPlace(ctx context.Context, userID, name, info string) (models.Place, error) {
	const op = "PlaceLogic.AddPlace"

	log := pl.log.With(
		slog.String("op", op),
		slog.String("user_id", userID),
	)

	log.Info("attemptong to add place")

	place := models.Place{
		ID:        uuid.NewString(),
		UserID:    userID,
		Name:      name,
		Info:      info,
		CreatedAt: time.Now().UTC(),
	}

	event := events.PlaceCreated{
		PlaceID:   place.ID,
		UserID:    place.UserID,
		Name:      place.Name,
		Info:      place.Info,
		CreatedAt: place.CreatedAt,
	}

	err := pl.placeRepo.InsertPlaceWithOutbox(ctx, place, event)
	if err != nil {
		log.Error("failed to add place", slog.Any("error", err))

		return models.Place{}, fmt.Errorf("%s: %w", op, err)
	}

	log.Info("place added successfully")

	return place, nil
}

func (pl *PlaceLogic) GetUserPlaces(ctx context.Context, userID string) ([]models.Place, error) {
	const op = "PlaceLogic.GetUserPlaces"

	log := pl.log.With(
		slog.String("op", op),
		slog.String("user_id", userID),
	)

	log.Info("attempting to get user places")

	places, err := pl.placeRepo.GetPlacesByUserID(ctx, userID)
	if err != nil {
		log.Error("failed to get user places", slog.Any("error", err))

		return nil, fmt.Errorf("%s: %w", op, err)
	}

	log.Info("get user places successfully")

	return places, nil
}
