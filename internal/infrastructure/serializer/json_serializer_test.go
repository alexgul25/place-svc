package serializer_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alexgul25/place-svc/internal/cache"
	"github.com/alexgul25/place-svc/internal/domain/events"
	"github.com/alexgul25/place-svc/internal/domain/models"
	"github.com/alexgul25/place-svc/internal/infrastructure/serializer"
	"github.com/alexgul25/place-svc/internal/outbox"
)

// Один сериализатор пишет и события outbox, и списки мест в кеш.
var (
	_ outbox.EventSerializer = serializer.JSONSerializer{}
	_ cache.CacheSerializer  = serializer.JSONSerializer{}
)

var createdAt = time.Date(2026, 10, 22, 10, 30, 0, 123456789, time.UTC)

// Полезная нагрузка события - контракт с потребителями из Kafka:
// имена полей и формат времени не должны меняться незаметно.
func TestJSONSerializer_PlaceCreatedPayload(t *testing.T) {
	event := events.PlaceCreated{
		PlaceID:   "6f1c2a8e-3b7d-4c5e-9f10-2a3b4c5d6e7f",
		UserID:    "3f2504e0-4f89-41d3-9a0c-0305e82c3301",
		Name:      "Кафе у дома",
		Info:      "столик у окна",
		CreatedAt: createdAt,
	}

	data, err := serializer.JSONSerializer{}.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("payload is not a JSON object: %v (%s)", err, data)
	}

	want := map[string]any{
		"place_id":   event.PlaceID,
		"user_id":    event.UserID,
		"name":       event.Name,
		"info":       event.Info,
		"created_at": "2026-10-22T10:30:00.123456789Z",
	}
	if len(got) != len(want) {
		t.Errorf("payload has fields %v, want exactly %v", keys(got), keys(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
}

// Список мест проходит через кеш без потерь: сохранённый и прочитанный списки совпадают.
func TestJSONSerializer_PlacesRoundTrip(t *testing.T) {
	places := []models.Place{
		{ID: "6f1c2a8e-3b7d-4c5e-9f10-2a3b4c5d6e7f", UserID: "3f2504e0-4f89-41d3-9a0c-0305e82c3301", Name: "Кафе у дома", Info: "столик у окна", CreatedAt: createdAt},
		{ID: "0b8e3c1d-2f4a-4b6c-8d9e-1f2a3b4c5d6e", UserID: "3f2504e0-4f89-41d3-9a0c-0305e82c3301", Name: `"Кавычки" и \ слэш`, Info: "", CreatedAt: createdAt.Add(time.Hour)},
	}

	s := serializer.JSONSerializer{}
	data, err := s.Marshal(places)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var got []models.Place
	if err := s.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	if len(got) != len(places) {
		t.Fatalf("got %d places, want %d", len(got), len(places))
	}
	for i := range places {
		want := places[i]
		if got[i].ID != want.ID || got[i].UserID != want.UserID || got[i].Name != want.Name || got[i].Info != want.Info {
			t.Errorf("place %d = %+v, want %+v", i, got[i], want)
		}
		if !got[i].CreatedAt.Equal(want.CreatedAt) {
			t.Errorf("place %d CreatedAt = %v, want %v", i, got[i].CreatedAt, want.CreatedAt)
		}
	}
}

// Пустой список кешируется как пустой массив: при чтении это пустой список, а не отсутствие данных.
func TestJSONSerializer_EmptyPlaces(t *testing.T) {
	s := serializer.JSONSerializer{}

	data, err := s.Marshal([]models.Place{})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if string(data) != "[]" {
		t.Errorf("Marshal(empty list) = %s, want []", data)
	}

	got := []models.Place{{Name: "stale"}}
	if err := s.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("Unmarshal([]) = %#v, want empty non-nil list", got)
	}
}

func TestJSONSerializer_MarshalError(t *testing.T) {
	if _, err := (serializer.JSONSerializer{}).Marshal(make(chan int)); err == nil {
		t.Error("expected error for a value that cannot be encoded, got nil")
	}
}

func TestJSONSerializer_UnmarshalError(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "broken JSON", data: `[{"ID":`},
		{name: "wrong type", data: `{"ID":"1"}`},
		{name: "empty input", data: ``},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []models.Place
			if err := (serializer.JSONSerializer{}).Unmarshal([]byte(tt.data), &got); err == nil {
				t.Errorf("expected error, got nil and %+v", got)
			}
		})
	}
}

func keys(m map[string]any) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}
