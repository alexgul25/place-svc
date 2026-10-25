//go:build integration

package postgresql_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alexgul25/place-svc/internal/domain/events"
	"github.com/alexgul25/place-svc/internal/outbox"
)

// insertOutboxRecord добавляет запись напрямую через SQL с заданными статусом
// и временем создания и возвращает её ID.
func insertOutboxRecord(t *testing.T, key, payload, status string, createdAt time.Time) string {
	t.Helper()

	var id string
	err := testDB.QueryRowContext(t.Context(), `
		INSERT INTO outbox (msg_topic, msg_key, msg_payload, send_status, created_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, outbox.TopicPlaceCreated, key, payload, status, createdAt).Scan(&id)
	if err != nil {
		t.Fatalf("failed to insert outbox record %q: %v", key, err)
	}
	return id
}

// recordIDs возвращает ID записей в том порядке, в котором они пришли.
func recordIDs(records []outbox.Record) []string {
	ids := make([]string, 0, len(records))
	for _, r := range records {
		ids = append(ids, r.ID)
	}
	return ids
}

// ----------------------------------------------------------------------------
// InsertRecord
// ----------------------------------------------------------------------------

// Запись попадает в outbox только вместе с транзакцией, в которой она добавлена.
func TestInsertRecord_InTransaction(t *testing.T) {
	_, outboxStorage := newStorages(t)
	event := events.PlaceCreated{PlaceID: uuid.NewString(), UserID: aliceID, Name: "Парк", CreatedAt: baseTime}

	insert := func(commit bool) {
		t.Helper()

		tx, err := testDB.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("failed to begin transaction: %v", err)
		}
		if err := outboxStorage.InsertRecord(t.Context(), tx, outbox.TopicPlaceCreated, aliceID, event); err != nil {
			_ = tx.Rollback()
			t.Fatalf("InsertRecord returned error: %v", err)
		}
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatalf("failed to finish transaction: %v", err)
		}
	}

	insert(false)
	if n := countRows(t, "outbox"); n != 0 {
		t.Fatalf("outbox has %d rows after rollback, want 0", n)
	}

	insert(true)
	records, err := outboxStorage.SelectPending(t.Context(), 10)
	if err != nil {
		t.Fatalf("SelectPending returned error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d pending records, want 1", len(records))
	}
	r := records[0]
	if r.Message.Topic != outbox.TopicPlaceCreated || r.Message.Key != aliceID || r.SendStatus != outbox.StatusPending {
		t.Errorf("record = %+v, want pending %q with key %q", r, outbox.TopicPlaceCreated, aliceID)
	}
	var payload events.PlaceCreated
	if err := json.Unmarshal(r.Message.Payload, &payload); err != nil || payload.PlaceID != event.PlaceID {
		t.Errorf("payload %q = (%+v, %v), want the event of place %s", r.Message.Payload, payload, err, event.PlaceID)
	}
}

// ----------------------------------------------------------------------------
// SelectPending
// ----------------------------------------------------------------------------

func TestSelectPending_OldestPendingFirst(t *testing.T) {
	_, outboxStorage := newStorages(t)

	newest := insertOutboxRecord(t, "k3", `{"n": 3}`, outbox.StatusPending, baseTime.Add(2*time.Minute))
	oldest := insertOutboxRecord(t, "k1", `{"n": 1}`, outbox.StatusPending, baseTime)
	// Отправленная запись старше всех, но выбираться не должна.
	insertOutboxRecord(t, "k0", `{"n": 0}`, outbox.StatusPublished, baseTime.Add(-time.Minute))
	middle := insertOutboxRecord(t, "k2", `{"n": 2}`, outbox.StatusPending, baseTime.Add(time.Minute))

	t.Run("limit", func(t *testing.T) {
		records, err := outboxStorage.SelectPending(t.Context(), 2)
		if err != nil {
			t.Fatalf("SelectPending returned error: %v", err)
		}
		if got, want := recordIDs(records), []string{oldest, middle}; !slices.Equal(got, want) {
			t.Fatalf("got records %q, want %q", got, want)
		}

		r := records[0]
		if r.Message.Topic != outbox.TopicPlaceCreated || r.Message.Key != "k1" || string(r.Message.Payload) != `{"n": 1}` {
			t.Errorf("message = %+v, want topic %q, key k1, payload {\"n\": 1}", r.Message, outbox.TopicPlaceCreated)
		}
		if r.SendStatus != outbox.StatusPending || !r.CreatedAt.Equal(baseTime) {
			t.Errorf("status and created_at = (%q, %v), want (pending, %v)", r.SendStatus, r.CreatedAt, baseTime)
		}
	})

	t.Run("all pending", func(t *testing.T) {
		records, err := outboxStorage.SelectPending(t.Context(), 10)
		if err != nil {
			t.Fatalf("SelectPending returned error: %v", err)
		}
		if got, want := recordIDs(records), []string{oldest, middle, newest}; !slices.Equal(got, want) {
			t.Errorf("got records %q, want %q", got, want)
		}
	})
}

func TestSelectPending_NoRecords(t *testing.T) {
	_, outboxStorage := newStorages(t)
	insertOutboxRecord(t, "k", `{}`, outbox.StatusPublished, baseTime)

	records, err := outboxStorage.SelectPending(t.Context(), 10)
	if err != nil {
		t.Fatalf("SelectPending returned error: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("got %d records, want none", len(records))
	}
}

func TestSelectPending_ContextCanceled(t *testing.T) {
	_, outboxStorage := newStorages(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := outboxStorage.SelectPending(ctx, 10); !errors.Is(err, context.Canceled) {
		t.Errorf("got error %v, want %v", err, context.Canceled)
	}
}

// ----------------------------------------------------------------------------
// MarkAsPublished
// ----------------------------------------------------------------------------

func TestMarkAsPublished(t *testing.T) {
	_, outboxStorage := newStorages(t)

	sent := insertOutboxRecord(t, "k1", `{}`, outbox.StatusPending, baseTime)
	waiting := insertOutboxRecord(t, "k2", `{}`, outbox.StatusPending, baseTime.Add(time.Minute))

	if err := outboxStorage.MarkAsPublished(t.Context(), sent); err != nil {
		t.Fatalf("MarkAsPublished returned error: %v", err)
	}

	// Меняется только отмеченная запись.
	rows := outboxRows(t)
	if len(rows) != 2 {
		t.Fatalf("got %d outbox rows, want 2", len(rows))
	}
	if r := rows[0]; r.ID != sent || r.Status != outbox.StatusPublished || !r.PublishedAt.Valid {
		t.Errorf("sent record = %+v, want published with published_at", r)
	}
	if r := rows[1]; r.ID != waiting || r.Status != outbox.StatusPending || r.PublishedAt.Valid {
		t.Errorf("waiting record = %+v, want pending without published_at", r)
	}

	// Отправленная запись больше не выбирается: процессор не пошлёт её повторно.
	records, err := outboxStorage.SelectPending(t.Context(), 10)
	if err != nil {
		t.Fatalf("SelectPending returned error: %v", err)
	}
	if got := recordIDs(records); !slices.Equal(got, []string{waiting}) {
		t.Errorf("pending records = %q, want only %q", got, waiting)
	}
}

func TestMarkAsPublished_Errors(t *testing.T) {
	_, outboxStorage := newStorages(t)

	t.Run("record does not exist", func(t *testing.T) {
		id := uuid.NewString()

		err := outboxStorage.MarkAsPublished(t.Context(), id)
		if err == nil || !strings.Contains(err.Error(), id) {
			t.Errorf("got error %v, want error with record ID %s", err, id)
		}
	})

	t.Run("ID is not a UUID", func(t *testing.T) {
		if err := outboxStorage.MarkAsPublished(t.Context(), "not-a-uuid"); err == nil {
			t.Error("MarkAsPublished succeeded, want error")
		}
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		if err := outboxStorage.MarkAsPublished(ctx, uuid.NewString()); !errors.Is(err, context.Canceled) {
			t.Errorf("got error %v, want %v", err, context.Canceled)
		}
	})
}
