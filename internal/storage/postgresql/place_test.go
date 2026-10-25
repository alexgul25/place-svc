//go:build integration

package postgresql_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/alexgul25/place-svc/internal/domain/events"
	"github.com/alexgul25/place-svc/internal/domain/models"
	"github.com/alexgul25/place-svc/internal/infrastructure/serializer"
	"github.com/alexgul25/place-svc/internal/outbox"
	"github.com/alexgul25/place-svc/internal/storage/postgresql"
)

const (
	aliceID = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
	bobID   = "9b2e6f1a-7c4d-4e8f-a1b2-c3d4e5f60718"
)

// baseTime - время с микросекундами: PostgreSQL хранит время с такой точностью,
// и сервис обрезает его до микросекунд перед сохранением.
var baseTime = time.Date(2026, 5, 1, 10, 30, 0, 123456000, time.UTC)

// ----------------------------------------------------------------------------
// Заглушки
// ----------------------------------------------------------------------------

// serializerStub возвращает то, что вернёт функция marshal.
type serializerStub struct {
	marshal func(event any) ([]byte, error)
}

func (s serializerStub) Marshal(event any) ([]byte, error) {
	return s.marshal(event)
}

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// newPlace возвращает место так, как его собирает сервис: со своим ID.
func newPlace(userID, name string, createdAt time.Time) models.Place {
	return models.Place{
		ID:        uuid.NewString(),
		UserID:    userID,
		Name:      name,
		Info:      "Описание: " + name,
		CreatedAt: createdAt,
	}
}

// eventFor возвращает событие о создании места, как его собирает сервис.
func eventFor(p models.Place) events.PlaceCreated {
	return events.PlaceCreated{PlaceID: p.ID, UserID: p.UserID, Name: p.Name, Info: p.Info, CreatedAt: p.CreatedAt}
}

// insertPlaces добавляет места в заданном порядке.
func insertPlaces(t *testing.T, ps *postgresql.PlaceStorage, places ...models.Place) {
	t.Helper()

	for _, p := range places {
		if err := ps.InsertPlaceWithOutbox(t.Context(), p, eventFor(p)); err != nil {
			t.Fatalf("failed to insert place %q: %v", p.Name, err)
		}
	}
}

// checkPlaces сравнивает списки мест. Время сравнивается через Equal:
// pgx возвращает его в часовом поясе time.Local, а не в том, в котором оно записано.
func checkPlaces(t *testing.T, got, want []models.Place) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d places %+v, want %d %+v", len(got), got, len(want), want)
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.ID != w.ID || g.UserID != w.UserID || g.Name != w.Name || g.Info != w.Info || !g.CreatedAt.Equal(w.CreatedAt) {
			t.Errorf("place %d = %+v, want %+v", i, g, w)
		}
	}
}

// pgCode возвращает код ошибки PostgreSQL или пустую строку.
func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// ----------------------------------------------------------------------------
// InsertPlaceWithOutbox
// ----------------------------------------------------------------------------

func TestInsertPlaceWithOutbox_SavesPlaceAndEvent(t *testing.T) {
	ps, _ := newStorages(t)

	place := newPlace(aliceID, "Парк Горького", baseTime)
	place.Info = "Прогулка у реки 🌳"
	event := eventFor(place)

	if err := ps.InsertPlaceWithOutbox(t.Context(), place, event); err != nil {
		t.Fatalf("InsertPlaceWithOutbox returned error: %v", err)
	}

	got, err := ps.GetPlacesByUserID(t.Context(), aliceID)
	if err != nil {
		t.Fatalf("GetPlacesByUserID returned error: %v", err)
	}
	checkPlaces(t, got, []models.Place{place})

	records := outboxRows(t)
	if len(records) != 1 {
		t.Fatalf("got %d outbox records, want 1", len(records))
	}
	r := records[0]
	// Ключ сообщения - ID пользователя: события одного пользователя попадают в одну партицию Kafka.
	if r.Topic != outbox.TopicPlaceCreated || r.Key != aliceID {
		t.Errorf("record topic and key = (%q, %q), want (%q, %q)", r.Topic, r.Key, outbox.TopicPlaceCreated, aliceID)
	}
	if r.Status != outbox.StatusPending || r.PublishedAt.Valid {
		t.Errorf("record status = %q, published_at = %v; want pending and not published", r.Status, r.PublishedAt)
	}

	// PostgreSQL хранит payload как JSONB и меняет форматирование,
	// поэтому сравнивается разобранное событие, а не байты.
	var payload events.PlaceCreated
	if err := json.Unmarshal([]byte(r.Payload), &payload); err != nil {
		t.Fatalf("payload %q is not a place.created event: %v", r.Payload, err)
	}
	if payload.PlaceID != event.PlaceID || payload.UserID != event.UserID || payload.Name != event.Name ||
		payload.Info != event.Info || !payload.CreatedAt.Equal(event.CreatedAt) {
		t.Errorf("payload = %+v, want %+v", payload, event)
	}
}

// Место и событие о нём сохраняются вместе или не сохраняются вовсе: место без события
// не увидят другие сервисы, а событие без места сообщит о том, чего нет.
func TestInsertPlaceWithOutbox_SavesNothingOnError(t *testing.T) {
	errMarshal := errors.New("marshal failed")
	existing := newPlace(aliceID, "Парк", baseTime)

	tests := []struct {
		name       string
		serializer outbox.EventSerializer
		existing   bool // в базе уже есть место existing со своим событием
		cancel     bool // контекст отменён до вызова
		place      models.Place
		wantErr    error
		wantCode   string
	}{
		{
			name:       "serializer fails",
			serializer: serializerStub{marshal: func(any) ([]byte, error) { return nil, errMarshal }},
			place:      newPlace(aliceID, "Сад", baseTime),
			wantErr:    errMarshal,
		},
		{
			// Место уже вставлено в транзакции, когда PostgreSQL отклоняет payload.
			name:       "payload is not JSON",
			serializer: serializerStub{marshal: func(any) ([]byte, error) { return []byte("not json"), nil }},
			place:      newPlace(aliceID, "Сад", baseTime),
			wantCode:   "22P02", // invalid_text_representation
		},
		{
			name:       "place ID already exists",
			serializer: serializer.JSONSerializer{},
			existing:   true,
			place:      models.Place{ID: existing.ID, UserID: aliceID, Name: "Другое место", CreatedAt: baseTime},
			wantCode:   "23505", // unique_violation
		},
		{
			name:       "context canceled",
			serializer: serializer.JSONSerializer{},
			cancel:     true,
			place:      newPlace(aliceID, "Сад", baseTime),
			wantErr:    context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps, _ := newStoragesWithSerializer(t, tt.serializer)

			wantRows := 0
			if tt.existing {
				// Существующее место добавляется с обычным сериализатором.
				good, _ := newStorages(t)
				insertPlaces(t, good, existing)
				wantRows = 1
			}

			ctx := t.Context()
			if tt.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			err := ps.InsertPlaceWithOutbox(ctx, tt.place, eventFor(tt.place))
			if err == nil {
				t.Fatal("InsertPlaceWithOutbox succeeded, want error")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("got error %v, want %v", err, tt.wantErr)
			}
			if code := pgCode(err); code != tt.wantCode {
				t.Errorf("got error %v (code %q), want code %q", err, code, tt.wantCode)
			}

			if n := countRows(t, "places"); n != wantRows {
				t.Errorf("places has %d rows, want %d", n, wantRows)
			}
			if n := countRows(t, "outbox"); n != wantRows {
				t.Errorf("outbox has %d rows, want %d", n, wantRows)
			}
			// Транзакция откатывается и возвращает соединение в пул:
			// иначе каждая ошибка навсегда занимала бы одно соединение.
			if inUse := testDB.Stats().InUse; inUse != 0 {
				t.Errorf("%d connections are still in use after the error", inUse)
			}
		})
	}
}

// Ошибка Commit возвращается вызывающему: иначе клиент получил бы ответ об успехе,
// а места в базе не было бы. Отложенное ограничение проверяется только при Commit,
// так тест получает ошибку именно на этом шаге.
func TestInsertPlaceWithOutbox_CommitError(t *testing.T) {
	ps, _ := newStorages(t)

	if _, err := testDB.ExecContext(t.Context(),
		"ALTER TABLE places ADD CONSTRAINT test_unique_name UNIQUE (place_name) DEFERRABLE INITIALLY DEFERRED"); err != nil {
		t.Fatalf("failed to add a deferred constraint: %v", err)
	}
	t.Cleanup(func() {
		// t.Context() к этому моменту уже отменён.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := testDB.ExecContext(ctx, "ALTER TABLE places DROP CONSTRAINT test_unique_name"); err != nil {
			t.Errorf("failed to drop the deferred constraint: %v", err)
		}
	})

	insertPlaces(t, ps, newPlace(aliceID, "Парк", baseTime))

	place := newPlace(bobID, "Парк", baseTime)
	err := ps.InsertPlaceWithOutbox(t.Context(), place, eventFor(place))
	if code := pgCode(err); code != "23505" {
		t.Fatalf("got error %v (code %q), want unique violation on commit", err, code)
	}
	if n := countRows(t, "places"); n != 1 {
		t.Errorf("places has %d rows, want 1", n)
	}
	if n := countRows(t, "outbox"); n != 1 {
		t.Errorf("outbox has %d rows, want 1", n)
	}
}

// ----------------------------------------------------------------------------
// GetPlacesByUserID
// ----------------------------------------------------------------------------

func TestGetPlacesByUserID_OldestFirst(t *testing.T) {
	ps, _ := newStorages(t)

	older := newPlace(aliceID, "Старое место", baseTime)
	newer := newPlace(aliceID, "Новое место", baseTime.Add(time.Hour))
	// Места, добавленные в одну микросекунду, упорядочиваются по ID.
	sameTimeA := newPlace(aliceID, "Место A", baseTime.Add(time.Minute))
	sameTimeA.ID = "00000000-0000-4000-8000-00000000000a"
	sameTimeB := newPlace(aliceID, "Место B", baseTime.Add(time.Minute))
	sameTimeB.ID = "00000000-0000-4000-8000-00000000000b"

	// Места вставляются в обратном порядке: так бывает, когда из двух запросов
	// позже начавшийся сохраняет место первым.
	insertPlaces(t, ps, newer, sameTimeB, sameTimeA, older)

	got, err := ps.GetPlacesByUserID(t.Context(), aliceID)
	if err != nil {
		t.Fatalf("GetPlacesByUserID returned error: %v", err)
	}
	checkPlaces(t, got, []models.Place{older, sameTimeA, sameTimeB, newer})
}

func TestGetPlacesByUserID_OnlyUserPlaces(t *testing.T) {
	ps, _ := newStorages(t)

	alicePark := newPlace(aliceID, "Парк", baseTime)
	aliceCafe := newPlace(aliceID, "Кафе", baseTime.Add(time.Minute))
	aliceCafe.Info = "" // описание необязательно
	bobMuseum := newPlace(bobID, "Музей", baseTime)
	insertPlaces(t, ps, alicePark, bobMuseum, aliceCafe)

	tests := []struct {
		name   string
		userID string
		want   []models.Place
	}{
		{name: "alice", userID: aliceID, want: []models.Place{alicePark, aliceCafe}},
		{name: "bob", userID: bobID, want: []models.Place{bobMuseum}},
		{name: "user without places", userID: uuid.NewString(), want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ps.GetPlacesByUserID(t.Context(), tt.userID)
			if err != nil {
				t.Fatalf("GetPlacesByUserID returned error: %v", err)
			}
			checkPlaces(t, got, tt.want)
		})
	}
}

func TestGetPlacesByUserID_Errors(t *testing.T) {
	ps, _ := newStorages(t)

	t.Run("user ID is not a UUID", func(t *testing.T) {
		_, err := ps.GetPlacesByUserID(t.Context(), "not-a-uuid")
		if err == nil {
			t.Fatal("GetPlacesByUserID succeeded, want error")
		}
		// По префиксу ошибку находят в логах и в коде.
		if want := "postgresql.PlaceStorage.GetPlacesByUserID: "; !strings.HasPrefix(err.Error(), want) {
			t.Errorf("got error %q, want prefix %q", err, want)
		}
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		if _, err := ps.GetPlacesByUserID(ctx, aliceID); !errors.Is(err, context.Canceled) {
			t.Errorf("got error %v, want %v", err, context.Canceled)
		}
	})
}
