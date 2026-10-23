package logger_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/alexgul25/place-svc/internal/lib/logger"
)

func TestNew(t *testing.T) {
	tests := []struct {
		env string
		// Пишутся ли сообщения уровня Debug.
		wantDebug bool
		// Пишутся ли логи в JSON (для сбора логов) или в цветном текстовом виде (для чтения в терминале).
		wantJSON bool
	}{
		{env: "local", wantDebug: true, wantJSON: false},
		{env: "dev", wantDebug: true, wantJSON: true},
		{env: "prod", wantDebug: false, wantJSON: true},
		// Неизвестное или пустое окружение - безопасный вариант, как в prod.
		{env: "staging", wantDebug: false, wantJSON: true},
		{env: "", wantDebug: false, wantJSON: true},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run("env="+tt.env, func(t *testing.T) {
			handler := logger.New(tt.env).Handler()

			if got := handler.Enabled(ctx, slog.LevelDebug); got != tt.wantDebug {
				t.Errorf("Debug enabled = %v, want %v", got, tt.wantDebug)
			}
			if !handler.Enabled(ctx, slog.LevelInfo) {
				t.Error("Info must be enabled in every environment")
			}

			_, isJSON := handler.(*slog.JSONHandler)
			if isJSON != tt.wantJSON {
				t.Errorf("JSON handler = %v, want %v (handler type %T)", isJSON, tt.wantJSON, handler)
			}
		})
	}
}
