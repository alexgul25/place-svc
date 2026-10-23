package interceptors

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
)

// Внутренний тест: interceptorLogger не экспортируется, а уровни Debug и неизвестный
// через публичные интерцепторы не достижимы (middleware использует только Info/Warn/Error).
func TestInterceptorLogger_Levels(t *testing.T) {
	tests := []struct {
		name  string
		level logging.Level
		want  string
	}{
		{name: "debug", level: logging.LevelDebug, want: "DEBUG"},
		{name: "info", level: logging.LevelInfo, want: "INFO"},
		{name: "warn", level: logging.LevelWarn, want: "WARN"},
		{name: "error", level: logging.LevelError, want: "ERROR"},
		{name: "unknown level falls back to info", level: logging.Level(42), want: "INFO"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			interceptorLogger(log).Log(context.Background(), tt.level, "message", "key", "value")

			var rec map[string]any
			if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
				t.Fatalf("failed to decode log record %q: %v", buf.String(), err)
			}
			if rec["level"] != tt.want {
				t.Errorf("level = %v, want %s", rec["level"], tt.want)
			}
			// Поля из middleware передаются в slog как пары ключ-значение.
			if rec["msg"] != "message" || rec["key"] != "value" {
				t.Errorf("unexpected record: %v", rec)
			}
		})
	}
}
