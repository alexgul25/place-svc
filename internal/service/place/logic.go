package placelogic

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/alexgul25/place-svc/internal/domain"
	"github.com/alexgul25/place-svc/internal/domain/events"
	"github.com/alexgul25/place-svc/internal/domain/models"
)

type PlaceRepository interface {
	InsertPlaceWithOutbox(ctx context.Context, place models.Place, event events.PlaceCreated) error
	GetPlacesByUserID(ctx context.Context, userID string) ([]models.Place, error)
}

type PlaceCache interface {
	Get(ctx context.Context, userID string) ([]models.Place, error)
	Set(ctx context.Context, userID string, places []models.Place) error
	Invalidate(ctx context.Context, userID string) error
}

type PlaceLogic struct {
	log   *slog.Logger
	repo  PlaceRepository
	cache PlaceCache
}

func New(log *slog.Logger, repo PlaceRepository, cache PlaceCache) *PlaceLogic {
	return &PlaceLogic{log: log, repo: repo, cache: cache}
}

func (pl *PlaceLogic) AddPlace(ctx context.Context, userID, name, info string) (models.Place, error) {
	const op = "PlaceLogic.AddPlace"

	log := pl.log.With(
		slog.String("op", op),
		slog.String("user_id", userID),
	)

	log.Info("attempting to add place to storage")

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

	err := pl.repo.InsertPlaceWithOutbox(ctx, place, event)
	if err != nil {
		log.Error("failed to add place to storage", slog.Any("error", err))

		return models.Place{}, fmt.Errorf("%s: %w", op, err)
	}

	log.Info("place added to storage successfully")

	log.Info("attempting to invalidate cache")

	err = pl.cache.Invalidate(ctx, place.UserID)
	if err != nil {
		log.Error("failed to invalidate cache", slog.Any("error", err))
	}

	log.Info("cache invalidated successfully")

	return place, nil
}

func (pl *PlaceLogic) GetUserPlaces(ctx context.Context, userID string) ([]models.Place, error) {
	const op = "PlaceLogic.GetUserPlaces"

	log := pl.log.With(
		slog.String("op", op),
		slog.String("user_id", userID),
	)

	log.Info("attempting to get user places from cache")

	places, err := pl.cache.Get(ctx, userID)
	if err == domain.ErrCacheMiss {
		log.Info("cache miss")
	} else if err != nil {
		log.Error("failed to get user places from cache", slog.Any("error", err))
	} else {
		log.Info("get user places from cache successfully")

		return places, nil
	}

	log.Info("attempting to get user places from storage")

	places, err = pl.repo.GetPlacesByUserID(ctx, userID)
	if err != nil {
		log.Error("failed to get user places from storage", slog.Any("error", err))

		return nil, fmt.Errorf("%s: %w", op, err)
	}

	log.Info("get user places from storage successfully")

	log.Info("attempting to update cache")

	err = pl.cache.Set(ctx, userID, places)
	if err != nil {
		log.Error("failed to update cache")
	}

	log.Info("cache updated successfully")

	return places, nil
}
