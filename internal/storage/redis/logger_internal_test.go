package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
)

// Внутренний тест: logAdapter не экспортируется. Что SetLogger подключает его к go-redis,
// проверяют E2E-тесты: в тесте глобальный логгер go-redis нельзя заменить безопасно,
// пока работают клиенты из других тестов.
func TestLogAdapter(t *testing.T) {
	var buf bytes.Buffer
	adapter := logAdapter{log: slog.New(slog.NewJSONHandler(&buf, nil))}

	adapter.Printf(context.Background(), "connection pool: failed to dial after %d attempts: %v", 5, errors.New("connection refused"))

	var record struct {
		Level, Msg, Source string
	}
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("log record is not JSON: %q", buf.String())
	}
	want := "connection pool: failed to dial after 5 attempts: connection refused"
	if record.Level != "WARN" || record.Msg != want || record.Source != "go-redis" {
		t.Errorf("record = %+v, want WARN %q from go-redis", record, want)
	}
}
