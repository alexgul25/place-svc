package interceptors_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/alexgul25/place-svc/internal/grpc/interceptors"
)

const (
	placeMethod  = "/place.v1.PlaceService/AddPlace"
	healthMethod = "/grpc.health.v1.Health/Check"
)

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// newBufferLogger возвращает логгер, который пишет JSON-записи в буфер, начиная с уровня Debug.
func newBufferLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

// records разбирает JSON-записи лога из буфера.
func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
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

// findRecord ищет запись лога по сообщению.
func findRecord(recs []map[string]any, msg string) (map[string]any, bool) {
	for _, rec := range recs {
		if rec["msg"] == msg {
			return rec, true
		}
	}
	return nil, false
}

// incomingContext имитирует входящий запрос с заголовками (пары ключ-значение).
func incomingContext(kv ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(kv...))
}

// call вызывает интерцептор для метода с заданным обработчиком.
func call(ctx context.Context, interceptor grpc.UnaryServerInterceptor, method string, handler grpc.UnaryHandler) (any, error) {
	return interceptor(ctx, "request", &grpc.UnaryServerInfo{FullMethod: method}, handler)
}

// okHandler - обработчик, который просто отвечает "response".
func okHandler(context.Context, any) (any, error) { return "response", nil }

// ----------------------------------------------------------------------------
// ContextEnricher и геттеры
// ----------------------------------------------------------------------------

func TestContextEnricher(t *testing.T) {
	allHeaders := []string{interceptors.HeaderServiceName, interceptors.HeaderUserID}

	tests := []struct {
		name    string
		ctx     context.Context
		headers []string

		wantService, wantUser     string
		wantServiceOK, wantUserOK bool
	}{
		{
			name:          "both headers are copied to context",
			ctx:           incomingContext("x-service-name", "gateway-svc", "x-user-id", "user-1"),
			headers:       allHeaders,
			wantService:   "gateway-svc",
			wantServiceOK: true,
			wantUser:      "user-1",
			wantUserOK:    true,
		},
		{
			// Пустой заголовок равносилен отсутствующему: хэндлер должен вернуть Unauthenticated.
			name:          "empty value is ignored",
			ctx:           incomingContext("x-service-name", "gateway-svc", "x-user-id", ""),
			headers:       allHeaders,
			wantService:   "gateway-svc",
			wantServiceOK: true,
		},
		{
			name:       "first of several values is used",
			ctx:        incomingContext("x-user-id", "user-1", "x-user-id", "user-2"),
			headers:    allHeaders,
			wantUser:   "user-1",
			wantUserOK: true,
		},
		{
			name:       "header not in the list is ignored",
			ctx:        incomingContext("x-service-name", "gateway-svc", "x-user-id", "user-1"),
			headers:    []string{interceptors.HeaderUserID},
			wantUser:   "user-1",
			wantUserOK: true,
		},
		{
			name:    "no incoming metadata",
			ctx:     context.Background(),
			headers: allHeaders,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var handlerCtx context.Context
			handler := func(ctx context.Context, _ any) (any, error) {
				handlerCtx = ctx
				return "response", nil
			}

			resp, err := call(tt.ctx, interceptors.NewContextEnricherInterceptor(tt.headers), placeMethod, handler)
			if err != nil || resp != "response" {
				t.Fatalf("got (%v, %v), want (response, nil)", resp, err)
			}
			if handlerCtx == nil {
				t.Fatal("handler was not called")
			}

			service, ok := interceptors.GetServiceNameFromContext(handlerCtx)
			if service != tt.wantService || ok != tt.wantServiceOK {
				t.Errorf("service name = (%q, %v), want (%q, %v)", service, ok, tt.wantService, tt.wantServiceOK)
			}
			user, ok := interceptors.GetUserIDFromContext(handlerCtx)
			if user != tt.wantUser || ok != tt.wantUserOK {
				t.Errorf("user id = (%q, %v), want (%q, %v)", user, ok, tt.wantUser, tt.wantUserOK)
			}
		})
	}
}

func TestContextEnricher_PassesHandlerError(t *testing.T) {
	wantErr := status.Error(codes.NotFound, "not found")
	handler := func(context.Context, any) (any, error) { return nil, wantErr }

	_, err := call(context.Background(), interceptors.NewContextEnricherInterceptor(nil), placeMethod, handler)
	if !errors.Is(err, wantErr) {
		t.Errorf("got error %v, want %v", err, wantErr)
	}
}

func TestGetters_EmptyContext(t *testing.T) {
	if v, ok := interceptors.GetServiceNameFromContext(context.Background()); ok || v != "" {
		t.Errorf("GetServiceNameFromContext = (%q, %v), want (\"\", false)", v, ok)
	}
	if v, ok := interceptors.GetUserIDFromContext(context.Background()); ok || v != "" {
		t.Errorf("GetUserIDFromContext = (%q, %v), want (\"\", false)", v, ok)
	}
}

// ----------------------------------------------------------------------------
// Recovery
// ----------------------------------------------------------------------------

func TestRecovery_NoPanic(t *testing.T) {
	log, buf := newBufferLogger()

	resp, err := call(context.Background(), interceptors.NewRecoveryInterceptor(log), placeMethod, okHandler)
	if err != nil || resp != "response" {
		t.Fatalf("got (%v, %v), want (response, nil)", resp, err)
	}
	if buf.Len() != 0 {
		t.Errorf("unexpected log output: %s", buf.String())
	}
}

func TestRecovery_Panic(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "panic with string", value: "boom"},
		{name: "panic with error", value: errors.New("secret database password in message")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, buf := newBufferLogger()
			handler := func(context.Context, any) (any, error) { panic(tt.value) }

			resp, err := call(context.Background(), interceptors.NewRecoveryInterceptor(log), placeMethod, handler)
			if resp != nil {
				t.Errorf("expected nil response, got %v", resp)
			}

			// Клиент получает Internal без подробностей: текст паники может содержать внутренние данные.
			st, _ := status.FromError(err)
			if st.Code() != codes.Internal || st.Message() != "internal error" {
				t.Errorf("got status (%v, %q), want (Internal, %q)", st.Code(), st.Message(), "internal error")
			}

			rec, ok := findRecord(records(t, buf), "recovered from panic")
			if !ok {
				t.Fatalf("panic was not logged, log: %s", buf.String())
			}
			if rec["level"] != "ERROR" {
				t.Errorf("log level = %v, want ERROR", rec["level"])
			}
			if _, ok := rec["panic"]; !ok {
				t.Errorf("log record has no panic value: %v", rec)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Logging
// ----------------------------------------------------------------------------

func TestLogging_LogsCallWithServiceName(t *testing.T) {
	log, buf := newBufferLogger()
	interceptor := interceptors.NewLoggingInterceptor(log, []string{interceptors.HeaderServiceName})
	ctx := incomingContext("x-service-name", "gateway-svc", "x-user-id", "user-1")

	resp, err := call(ctx, interceptor, placeMethod, okHandler)
	if err != nil || resp != "response" {
		t.Fatalf("got (%v, %v), want (response, nil)", resp, err)
	}

	recs := records(t, buf)
	for _, msg := range []string{"started call", "finished call"} {
		rec, ok := findRecord(recs, msg)
		if !ok {
			t.Errorf("no %q record, log: %v", msg, recs)
			continue
		}
		if rec["x-service-name"] != "gateway-svc" {
			t.Errorf("%q: x-service-name = %v, want gateway-svc", msg, rec["x-service-name"])
		}
		// В лог попадают только перечисленные заголовки: ID пользователя логировать не нужно.
		if _, ok := rec["x-user-id"]; ok {
			t.Errorf("%q: x-user-id must not be logged", msg)
		}
	}
}

func TestLogging_NoMetadata(t *testing.T) {
	log, buf := newBufferLogger()
	interceptor := interceptors.NewLoggingInterceptor(log, []string{interceptors.HeaderServiceName})

	if _, err := call(context.Background(), interceptor, placeMethod, okHandler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rec, ok := findRecord(records(t, buf), "finished call")
	if !ok {
		t.Fatal("call was not logged")
	}
	if _, ok := rec["x-service-name"]; ok {
		t.Errorf("x-service-name must be absent without metadata: %v", rec)
	}
}

func TestLogging_SkipsHealthCheck(t *testing.T) {
	log, buf := newBufferLogger()
	interceptor := interceptors.NewLoggingInterceptor(log, []string{interceptors.HeaderServiceName})

	called := false
	handler := func(context.Context, any) (any, error) {
		called = true
		return "response", nil
	}

	if _, err := call(context.Background(), interceptor, healthMethod, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler was not called for health check")
	}
	// Проверки здоровья идут каждые несколько секунд и засорили бы лог.
	if buf.Len() != 0 {
		t.Errorf("health check must not be logged, got: %s", buf.String())
	}
}

func TestLogging_LevelByCode(t *testing.T) {
	tests := []struct {
		code      codes.Code
		wantLevel string
	}{
		{code: codes.OK, wantLevel: "INFO"},
		{code: codes.InvalidArgument, wantLevel: "INFO"},
		{code: codes.DeadlineExceeded, wantLevel: "WARN"},
		{code: codes.Internal, wantLevel: "ERROR"},
	}

	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			log, buf := newBufferLogger()
			interceptor := interceptors.NewLoggingInterceptor(log, nil)
			handler := func(context.Context, any) (any, error) {
				if tt.code == codes.OK {
					return "response", nil
				}
				return nil, status.Error(tt.code, "error")
			}

			_, err := call(context.Background(), interceptor, placeMethod, handler)
			if status.Code(err) != tt.code {
				t.Fatalf("got code %v, want %v", status.Code(err), tt.code)
			}

			rec, ok := findRecord(records(t, buf), "finished call")
			if !ok {
				t.Fatal("call was not logged")
			}
			if rec["level"] != tt.wantLevel {
				t.Errorf("level = %v, want %s", rec["level"], tt.wantLevel)
			}
		})
	}
}
