package placelogic_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alexgul25/place-svc/internal/domain"
	"github.com/alexgul25/place-svc/internal/domain/events"
	"github.com/alexgul25/place-svc/internal/domain/models"
	placelogic "github.com/alexgul25/place-svc/internal/service/place"
)

const userID = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"

// ----------------------------------------------------------------------------
// Заглушки
// ----------------------------------------------------------------------------

// Поведение заглушек задаётся функциями в полях. Если функция не задана,
// а метод вызван, тест падает: так проверяется, что вызова не было.

type stubRepo struct {
	t      *testing.T
	insert func(ctx context.Context, place models.Place, event events.PlaceCreated) error
	get    func(ctx context.Context, userID string) ([]models.Place, error)
}

func (r *stubRepo) InsertPlaceWithOutbox(ctx context.Context, place models.Place, event events.PlaceCreated) error {
	if r.insert == nil {
		r.t.Errorf("unexpected InsertPlaceWithOutbox(%+v)", place)
		return errors.New("unexpected call")
	}
	return r.insert(ctx, place, event)
}

func (r *stubRepo) GetPlacesByUserID(ctx context.Context, userID string) ([]models.Place, error) {
	if r.get == nil {
		r.t.Errorf("unexpected GetPlacesByUserID(%q)", userID)
		return nil, errors.New("unexpected call")
	}
	return r.get(ctx, userID)
}

type stubCache struct {
	t          *testing.T
	get        func(ctx context.Context, userID string) ([]models.Place, error)
	set        func(ctx context.Context, userID string, places []models.Place) error
	invalidate func(ctx context.Context, userID string) error
}

func (c *stubCache) Get(ctx context.Context, userID string) ([]models.Place, error) {
	if c.get == nil {
		c.t.Errorf("unexpected cache Get(%q)", userID)
		return nil, errors.New("unexpected call")
	}
	return c.get(ctx, userID)
}

func (c *stubCache) Set(ctx context.Context, userID string, places []models.Place) error {
	if c.set == nil {
		c.t.Errorf("unexpected cache Set(%q)", userID)
		return errors.New("unexpected call")
	}
	return c.set(ctx, userID, places)
}

func (c *stubCache) Invalidate(ctx context.Context, userID string) error {
	if c.invalidate == nil {
		c.t.Errorf("unexpected cache Invalidate(%q)", userID)
		return errors.New("unexpected call")
	}
	return c.invalidate(ctx, userID)
}

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// newLogic собирает бизнес-логику с заглушками и логгером, который пишет JSON-записи в буфер.
func newLogic(repo *stubRepo, cache *stubCache) (*placelogic.PlaceLogic, *bytes.Buffer) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return placelogic.New(log, repo, cache), &buf
}

// logRecords разбирает JSON-записи лога из буфера.
func logRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()

	var result []map[string]any
	dec := json.NewDecoder(buf)
	for dec.More() {
		var rec map[string]any
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("failed to decode log record: %v", err)
		}
		result = append(result, rec)
	}
	return result
}

// findRecords возвращает записи лога с сообщением msg.
func findRecords(recs []map[string]any, msg string) []map[string]any {
	var result []map[string]any
	for _, rec := range recs {
		if rec["msg"] == msg {
			result = append(result, rec)
		}
	}
	return result
}

// checkErrorLogged проверяет, что ошибка записана в лог один раз, на уровне ERROR и с текстом ошибки,
// а сообщения об успехе нет.
func checkErrorLogged(t *testing.T, recs []map[string]any, failMsg, wantErr, successMsg string) {
	t.Helper()

	failed := findRecords(recs, failMsg)
	if len(failed) != 1 {
		t.Fatalf("got %d %q records, want 1; log: %v", len(failed), failMsg, recs)
	}
	if failed[0]["level"] != "ERROR" {
		t.Errorf("%q: level = %v, want ERROR", failMsg, failed[0]["level"])
	}
	if failed[0]["error"] != wantErr {
		t.Errorf("%q: error = %v, want %q", failMsg, failed[0]["error"], wantErr)
	}
	if got := findRecords(recs, successMsg); len(got) != 0 {
		t.Errorf("%q must not be logged after a failure, got %v", successMsg, got)
	}
}

func samplePlaces() []models.Place {
	createdAt := time.Date(2026, 10, 22, 10, 30, 0, 123456000, time.UTC)
	return []models.Place{
		{ID: "6f1c2a8e-3b7d-4c5e-9f10-2a3b4c5d6e7f", UserID: userID, Name: "Кафе у дома", Info: "столик у окна", CreatedAt: createdAt},
		{ID: "0b8e3c1d-2f4a-4b6c-8d9e-1f2a3b4c5d6e", UserID: userID, Name: "Парк", Info: "", CreatedAt: createdAt.Add(time.Hour)},
	}
}

// ----------------------------------------------------------------------------
// AddPlace
// ----------------------------------------------------------------------------

func TestAddPlace_Success(t *testing.T) {
	var (
		saved       models.Place
		savedEvent  events.PlaceCreated
		invalidated []string
	)
	repo := &stubRepo{t: t, insert: func(_ context.Context, place models.Place, event events.PlaceCreated) error {
		saved, savedEvent = place, event
		return nil
	}}
	cache := &stubCache{t: t, invalidate: func(_ context.Context, userID string) error {
		invalidated = append(invalidated, userID)
		return nil
	}}
	logic, _ := newLogic(repo, cache)

	before := time.Now()
	place, err := logic.AddPlace(t.Context(), userID, "Кафе у дома", "столик у окна")
	after := time.Now()
	if err != nil {
		t.Fatalf("AddPlace returned error: %v", err)
	}

	// Возвращается то, что сохранено.
	if place != saved {
		t.Errorf("returned place %+v differs from saved %+v", place, saved)
	}

	if err := uuid.Validate(place.ID); err != nil {
		t.Errorf("ID %q is not a UUID: %v", place.ID, err)
	}
	if place.UserID != userID || place.Name != "Кафе у дома" || place.Info != "столик у окна" {
		t.Errorf("place = %+v, want fields from the request", place)
	}
	if place.CreatedAt.Location() != time.UTC {
		t.Errorf("CreatedAt location = %v, want UTC", place.CreatedAt.Location())
	}
	// Время обрезается до микросекунд, поэтому может оказаться чуть раньше before.
	if place.CreatedAt.Before(before.Add(-time.Microsecond)) || place.CreatedAt.After(after) {
		t.Errorf("CreatedAt = %v, want between %v and %v", place.CreatedAt, before, after)
	}

	// Событие для outbox описывает то же место.
	wantEvent := events.PlaceCreated{
		PlaceID:   place.ID,
		UserID:    place.UserID,
		Name:      place.Name,
		Info:      place.Info,
		CreatedAt: place.CreatedAt,
	}
	if savedEvent != wantEvent {
		t.Errorf("event = %+v, want %+v", savedEvent, wantEvent)
	}

	// Кеш списка мест пользователя сброшен, чтобы новое место сразу появилось в списке.
	if !slices.Equal(invalidated, []string{userID}) {
		t.Errorf("invalidated cache for %q, want [%s]", invalidated, userID)
	}
}

// PostgreSQL хранит время с точностью до микросекунд (pgx отбрасывает остаток).
// Ответ AddPlace, событие в Kafka и список мест должны показывать одно и то же время.
func TestAddPlace_CreatedAtHasMicrosecondPrecision(t *testing.T) {
	repo := &stubRepo{t: t, insert: func(context.Context, models.Place, events.PlaceCreated) error { return nil }}
	cache := &stubCache{t: t, invalidate: func(context.Context, string) error { return nil }}
	logic, _ := newLogic(repo, cache)

	// Одно значение может случайно оказаться кратным микросекунде, несколько подряд - нет.
	for range 5 {
		place, err := logic.AddPlace(t.Context(), userID, "Парк", "")
		if err != nil {
			t.Fatalf("AddPlace returned error: %v", err)
		}
		if ns := place.CreatedAt.Nanosecond() % 1000; ns != 0 {
			t.Fatalf("CreatedAt = %s has %d ns below a microsecond, PostgreSQL would store a different time",
				place.CreatedAt.Format(time.RFC3339Nano), ns)
		}
	}
}

func TestAddPlace_NewIDForEachPlace(t *testing.T) {
	repo := &stubRepo{t: t, insert: func(context.Context, models.Place, events.PlaceCreated) error { return nil }}
	cache := &stubCache{t: t, invalidate: func(context.Context, string) error { return nil }}
	logic, _ := newLogic(repo, cache)

	first, err := logic.AddPlace(t.Context(), userID, "Парк", "")
	if err != nil {
		t.Fatalf("AddPlace returned error: %v", err)
	}
	second, err := logic.AddPlace(t.Context(), userID, "Парк", "")
	if err != nil {
		t.Fatalf("AddPlace returned error: %v", err)
	}

	if first.ID == second.ID {
		t.Errorf("two places got the same ID %q", first.ID)
	}
}

func TestAddPlace_StorageError(t *testing.T) {
	storageErr := errors.New("connection refused")
	repo := &stubRepo{t: t, insert: func(context.Context, models.Place, events.PlaceCreated) error { return storageErr }}
	// Invalidate не задан: если место не сохранено, кеш трогать незачем.
	cache := &stubCache{t: t}
	logic, buf := newLogic(repo, cache)

	place, err := logic.AddPlace(t.Context(), userID, "Парк", "")
	if !errors.Is(err, storageErr) {
		t.Fatalf("error = %v, want wrapped %v", err, storageErr)
	}
	if place != (models.Place{}) {
		t.Errorf("place = %+v, want zero value on error", place)
	}

	checkErrorLogged(t, logRecords(t, buf), "failed to add place to storage", "connection refused", "place added to storage successfully")
}

// Место уже сохранено, и ошибка Redis не должна превращать успешный запрос в ошибку:
// устаревший список исчезнет из кеша по TTL.
func TestAddPlace_InvalidateError(t *testing.T) {
	repo := &stubRepo{t: t, insert: func(context.Context, models.Place, events.PlaceCreated) error { return nil }}
	cache := &stubCache{t: t, invalidate: func(context.Context, string) error { return errors.New("redis is down") }}
	logic, buf := newLogic(repo, cache)

	place, err := logic.AddPlace(t.Context(), userID, "Парк", "")
	if err != nil {
		t.Fatalf("AddPlace returned error: %v", err)
	}
	if place.Name != "Парк" {
		t.Errorf("place = %+v, want the saved place", place)
	}

	checkErrorLogged(t, logRecords(t, buf), "failed to invalidate cache", "redis is down", "cache invalidated successfully")
}

func TestAddPlace_LogsUserID(t *testing.T) {
	repo := &stubRepo{t: t, insert: func(context.Context, models.Place, events.PlaceCreated) error { return nil }}
	cache := &stubCache{t: t, invalidate: func(context.Context, string) error { return nil }}
	logic, buf := newLogic(repo, cache)

	if _, err := logic.AddPlace(t.Context(), userID, "Парк", ""); err != nil {
		t.Fatalf("AddPlace returned error: %v", err)
	}

	recs := logRecords(t, buf)
	for _, msg := range []string{"place added to storage successfully", "cache invalidated successfully"} {
		got := findRecords(recs, msg)
		if len(got) != 1 {
			t.Errorf("got %d %q records, want 1", len(got), msg)
			continue
		}
		if got[0]["user_id"] != userID || got[0]["op"] != "PlaceLogic.AddPlace" {
			t.Errorf("%q: user_id = %v, op = %v", msg, got[0]["user_id"], got[0]["op"])
		}
	}
}

// ----------------------------------------------------------------------------
// GetUserPlaces
// ----------------------------------------------------------------------------

func TestGetUserPlaces_CacheHit(t *testing.T) {
	tests := []struct {
		name   string
		cached []models.Place
	}{
		{name: "places", cached: samplePlaces()},
		// Пустой список - тоже результат: у пользователя нет мест, в БД идти незачем.
		{name: "empty list", cached: []models.Place{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Хранилище не задано: при попадании в кеш в БД идти нельзя.
			repo := &stubRepo{t: t}
			cache := &stubCache{t: t, get: func(_ context.Context, id string) ([]models.Place, error) {
				if id != userID {
					t.Errorf("cache Get(%q), want %q", id, userID)
				}
				return tt.cached, nil
			}}
			logic, _ := newLogic(repo, cache)

			got, err := logic.GetUserPlaces(t.Context(), userID)
			if err != nil {
				t.Fatalf("GetUserPlaces returned error: %v", err)
			}
			if !slices.Equal(got, tt.cached) {
				t.Errorf("got %+v, want cached %+v", got, tt.cached)
			}
		})
	}
}

func TestGetUserPlaces_CacheMiss(t *testing.T) {
	places := samplePlaces()
	var setUser string
	var setPlaces []models.Place

	repo := &stubRepo{t: t, get: func(_ context.Context, id string) ([]models.Place, error) {
		if id != userID {
			t.Errorf("GetPlacesByUserID(%q), want %q", id, userID)
		}
		return places, nil
	}}
	cache := &stubCache{
		t:   t,
		get: func(context.Context, string) ([]models.Place, error) { return nil, domain.ErrCacheMiss },
		set: func(_ context.Context, id string, p []models.Place) error {
			setUser, setPlaces = id, p
			return nil
		},
	}
	logic, buf := newLogic(repo, cache)

	got, err := logic.GetUserPlaces(t.Context(), userID)
	if err != nil {
		t.Fatalf("GetUserPlaces returned error: %v", err)
	}
	if !slices.Equal(got, places) {
		t.Errorf("got %+v, want places from storage %+v", got, places)
	}

	// Список из БД попадает в кеш для следующих запросов.
	if setUser != userID || !slices.Equal(setPlaces, places) {
		t.Errorf("cache Set(%q, %+v), want (%q, places from storage)", setUser, setPlaces, userID)
	}

	recs := logRecords(t, buf)
	if len(findRecords(recs, "cache miss")) != 1 {
		t.Errorf("no %q record; log: %v", "cache miss", recs)
	}
	// Промах кеша - обычная ситуация, а не ошибка.
	for _, rec := range recs {
		if rec["level"] == "ERROR" {
			t.Errorf("unexpected error record: %v", rec)
		}
	}
}

// Если Redis недоступен, список берётся из БД: кеш только ускоряет ответ.
func TestGetUserPlaces_CacheGetError(t *testing.T) {
	places := samplePlaces()
	setCalled := false

	repo := &stubRepo{t: t, get: func(context.Context, string) ([]models.Place, error) { return places, nil }}
	cache := &stubCache{
		t:   t,
		get: func(context.Context, string) ([]models.Place, error) { return nil, errors.New("redis is down") },
		set: func(context.Context, string, []models.Place) error {
			setCalled = true
			return nil
		},
	}
	logic, buf := newLogic(repo, cache)

	got, err := logic.GetUserPlaces(t.Context(), userID)
	if err != nil {
		t.Fatalf("GetUserPlaces returned error: %v", err)
	}
	if !slices.Equal(got, places) {
		t.Errorf("got %+v, want places from storage", got)
	}
	if !setCalled {
		t.Error("cache was not updated after reading from storage")
	}

	checkErrorLogged(t, logRecords(t, buf), "failed to get user places from cache", "redis is down", "get user places from cache successfully")
}

func TestGetUserPlaces_StorageError(t *testing.T) {
	storageErr := errors.New("connection refused")
	repo := &stubRepo{t: t, get: func(context.Context, string) ([]models.Place, error) { return nil, storageErr }}
	// Set не задан: без данных из БД кеш обновлять нечем.
	cache := &stubCache{t: t, get: func(context.Context, string) ([]models.Place, error) { return nil, domain.ErrCacheMiss }}
	logic, buf := newLogic(repo, cache)

	got, err := logic.GetUserPlaces(t.Context(), userID)
	if !errors.Is(err, storageErr) {
		t.Fatalf("error = %v, want wrapped %v", err, storageErr)
	}
	if got != nil {
		t.Errorf("got %+v, want nil on error", got)
	}

	checkErrorLogged(t, logRecords(t, buf), "failed to get user places from storage", "connection refused", "get user places from storage successfully")
}

// Список уже получен из БД, и ошибка Redis не должна превращать успешный запрос в ошибку.
func TestGetUserPlaces_CacheSetError(t *testing.T) {
	places := samplePlaces()
	repo := &stubRepo{t: t, get: func(context.Context, string) ([]models.Place, error) { return places, nil }}
	cache := &stubCache{
		t:   t,
		get: func(context.Context, string) ([]models.Place, error) { return nil, domain.ErrCacheMiss },
		set: func(context.Context, string, []models.Place) error { return errors.New("redis is down") },
	}
	logic, buf := newLogic(repo, cache)

	got, err := logic.GetUserPlaces(t.Context(), userID)
	if err != nil {
		t.Fatalf("GetUserPlaces returned error: %v", err)
	}
	if !slices.Equal(got, places) {
		t.Errorf("got %+v, want places from storage", got)
	}

	checkErrorLogged(t, logRecords(t, buf), "failed to update cache", "redis is down", "cache updated successfully")
}
