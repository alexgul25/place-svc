package handlersgrpc_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/alexgul25/place-svc/internal/domain/models"
	handlersgrpc "github.com/alexgul25/place-svc/internal/grpc/handlers"
	"github.com/alexgul25/place-svc/internal/grpc/interceptors"
	placev1 "github.com/alexgul25/protos/gen/go/place/v1"
)

// ID в UUID содержат буквы: на них проверяется приведение регистра.
const (
	userID  = "4f1c2d3e-5a6b-4c7d-8e9f-0a1b2c3d4e5f"
	otherID = "9b8a7c6d-5e4f-4a3b-9c2d-1e0f2a3b4c5d"
	placeID = "6f1c2a8e-3b7d-4c5e-9f10-2a3b4c5d6e7f"
)

// errStorage имитирует сбой хранилища. Детали в тексте не должны попасть к клиенту.
var errStorage = errors.New("storage failure: connection to 10.0.0.5:5432 refused")

// errUnexpectedCall возвращает заглушка, если хэндлер вызвал сервис, хотя не должен был.
var errUnexpectedCall = errors.New("unexpected call")

// wrap оборачивает ошибку так же, как это делает бизнес-логика: "op: err".
func wrap(err error) error { return fmt.Errorf("PlaceLogic: %w", err) }

// ----------------------------------------------------------------------------
// Заглушка сервиса
//
// Поведение задаётся функцией в поле. Если функция не задана, а хэндлер всё же
// вызвал метод, тест падает: так проверяется, что при ошибке валидации запрос
// не доходит до бизнес-логики.
//
// Хэндлер выполняется в горутине gRPC-сервера, а t.Fatal можно вызывать только
// из горутины теста. Поэтому заглушка сообщает об ошибке через t.Errorf.
// ----------------------------------------------------------------------------

type serviceStub struct {
	t *testing.T

	addPlace      func(ctx context.Context, userID, name, info string) (models.Place, error)
	getUserPlaces func(ctx context.Context, userID string) ([]models.Place, error)
}

func (s *serviceStub) unexpected(method string) error {
	s.t.Errorf("unexpected call to %s", method)
	return errUnexpectedCall
}

func (s *serviceStub) AddPlace(ctx context.Context, userID, name, info string) (models.Place, error) {
	if s.addPlace == nil {
		return models.Place{}, s.unexpected("AddPlace")
	}
	return s.addPlace(ctx, userID, name, info)
}

func (s *serviceStub) GetUserPlaces(ctx context.Context, userID string) ([]models.Place, error) {
	if s.getUserPlaces == nil {
		return nil, s.unexpected("GetUserPlaces")
	}
	return s.getUserPlaces(ctx, userID)
}

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// newClient поднимает gRPC-сервер с хэндлерами в памяти (bufconn) и возвращает клиент к нему.
// Заголовки в контекст переносит тот же интерцептор, что и в сервисе.
func newClient(t *testing.T, svc handlersgrpc.PlaceService) placev1.PlaceServiceClient {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.UnaryInterceptor(
		interceptors.NewContextEnricherInterceptor([]string{interceptors.HeaderServiceName, interceptors.HeaderUserID}),
	))
	handlersgrpc.Register(srv, svc)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return placev1.NewPlaceServiceClient(conn)
}

// asUser - вызов от имени пользователя: такие заголовки Gateway добавляет после проверки JWT.
func asUser(t *testing.T, id string) context.Context {
	t.Helper()
	return metadata.AppendToOutgoingContext(t.Context(), interceptors.HeaderServiceName, "gateway-svc", interceptors.HeaderUserID, id)
}

// requireStatus проверяет код и текст ответа сервера.
// Точное сравнение текста заодно гарантирует, что детали внутренних ошибок не уходят клиенту.
func requireStatus(t *testing.T, err error, wantCode codes.Code, wantMsg string) {
	t.Helper()

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error %v is not a gRPC status", err)
	}
	if st.Code() != wantCode || st.Message() != wantMsg {
		t.Fatalf("got status (%v, %q), want (%v, %q)", st.Code(), st.Message(), wantCode, wantMsg)
	}
}

// ----------------------------------------------------------------------------
// Аутентификация
// ----------------------------------------------------------------------------

// Оба метода вызываются только от имени пользователя: Gateway передаёт x-user-id после проверки JWT.
func TestPlaceMethods_Unauthenticated(t *testing.T) {
	// Тела запросов корректные: отказ получается только из-за заголовка.
	methods := []struct {
		name string
		call func(ctx context.Context, c placev1.PlaceServiceClient) error
	}{
		{"AddPlace", func(ctx context.Context, c placev1.PlaceServiceClient) error {
			_, err := c.AddPlace(ctx, &placev1.AddPlaceRequest{Name: "Парк"})
			return err
		}},
		{"GetUserPlaces", func(ctx context.Context, c placev1.PlaceServiceClient) error {
			_, err := c.GetUserPlaces(ctx, &placev1.GetUserPlacesRequest{UserId: otherID})
			return err
		}},
	}

	cases := []struct {
		name    string
		headers []string // пары заголовок-значение
		wantMsg string
	}{
		{"no headers", nil, "user id is required"},
		{"only service name", []string{interceptors.HeaderServiceName, "gateway-svc"}, "user id is required"},
		{"empty user id", []string{interceptors.HeaderUserID, ""}, "user id is required"},
		{"not a UUID", []string{interceptors.HeaderUserID, "user-1"}, "invalid user id"},
		{"UUID in braces", []string{interceptors.HeaderUserID, "{" + userID + "}"}, "invalid user id"},
		{"UUID without hyphens", []string{interceptors.HeaderUserID, strings.ReplaceAll(userID, "-", "")}, "invalid user id"},
	}

	for _, m := range methods {
		for _, tc := range cases {
			t.Run(m.name+"/"+tc.name, func(t *testing.T) {
				ctx := metadata.AppendToOutgoingContext(t.Context(), tc.headers...)

				// Функции в заглушке не заданы: обращение к сервису уронит тест.
				err := m.call(ctx, newClient(t, &serviceStub{t: t}))
				requireStatus(t, err, codes.Unauthenticated, tc.wantMsg)
			})
		}
	}
}

// Заголовок проверяется раньше аргументов: неаутентифицированный вызов с пустым
// телом получает Unauthenticated, а не подсказку, какие поля заполнить.
func TestAddPlace_UnauthenticatedBeforeValidation(t *testing.T) {
	_, err := newClient(t, &serviceStub{t: t}).AddPlace(t.Context(), &placev1.AddPlaceRequest{})
	requireStatus(t, err, codes.Unauthenticated, "user id is required")
}

// ----------------------------------------------------------------------------
// AddPlace
// ----------------------------------------------------------------------------

func TestAddPlace_Success(t *testing.T) {
	createdAt := time.Date(2026, 10, 22, 10, 30, 0, 123456000, time.UTC)

	tests := []struct {
		name      string
		placeName string
		placeInfo string
	}{
		{name: "name and info", placeName: "Кафе у дома", placeInfo: "столик у окна"},
		{name: "without info", placeName: "Парк", placeInfo: ""},
		// Значения сохраняются как есть: пробелы по краям не обрезаются.
		{name: "spaces around name", placeName: "  Парк  ", placeInfo: " \n "},
		// Граница - в символах: 200 кириллических букв занимают 400 байт.
		{name: "name of 200 characters", placeName: strings.Repeat("я", 200), placeInfo: ""},
		{name: "info of 2000 characters", placeName: "Парк", placeInfo: strings.Repeat("я", 2000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &serviceStub{t: t}
			svc.addPlace = func(_ context.Context, gotUser, name, info string) (models.Place, error) {
				if gotUser != userID || name != tt.placeName || info != tt.placeInfo {
					t.Errorf("AddPlace called with (%q, %q, %q)", gotUser, name, info)
				}
				return models.Place{ID: placeID, UserID: gotUser, Name: name, Info: info, CreatedAt: createdAt}, nil
			}

			resp, err := newClient(t, svc).AddPlace(asUser(t, userID), &placev1.AddPlaceRequest{Name: tt.placeName, Info: tt.placeInfo})
			if err != nil {
				t.Fatalf("AddPlace returned error: %v", err)
			}

			if resp.GetId() != placeID || resp.GetUserId() != userID || resp.GetName() != tt.placeName || resp.GetInfo() != tt.placeInfo {
				t.Errorf("unexpected response: %v", resp)
			}
			if got := resp.GetCreatedAt().AsTime(); !got.Equal(createdAt) {
				t.Errorf("created_at = %v, want %v", got, createdAt)
			}
		})
	}
}

// PostgreSQL не различает регистр в UUID, а кеш списка мест - различает. Без приведения
// место, добавленное с x-user-id в верхнем регистре, сбрасывало бы не тот ключ кеша.
func TestAddPlace_UserIDIsNormalized(t *testing.T) {
	var gotUser string
	svc := &serviceStub{t: t}
	svc.addPlace = func(_ context.Context, id, name, info string) (models.Place, error) {
		gotUser = id
		return models.Place{ID: placeID, UserID: id, Name: name, Info: info}, nil
	}

	resp, err := newClient(t, svc).AddPlace(asUser(t, strings.ToUpper(userID)), &placev1.AddPlaceRequest{Name: "Парк"})
	if err != nil {
		t.Fatalf("AddPlace returned error: %v", err)
	}
	if gotUser != userID {
		t.Errorf("service got user id %q, want %q", gotUser, userID)
	}
	if resp.GetUserId() != userID {
		t.Errorf("response user_id = %q, want %q", resp.GetUserId(), userID)
	}
}

func TestAddPlace_InvalidArgument(t *testing.T) {
	tests := []struct {
		name    string
		req     *placev1.AddPlaceRequest
		wantMsg string
	}{
		{"empty name", &placev1.AddPlaceRequest{Info: "столик у окна"}, "name is required"},
		{"name of spaces", &placev1.AddPlaceRequest{Name: "   "}, "name is required"},
		{"name of tabs and newlines", &placev1.AddPlaceRequest{Name: "\t\n"}, "name is required"},
		{"NUL in name", &placev1.AddPlaceRequest{Name: "Па\x00рк"}, "name contains invalid characters"},
		{"name of 201 characters", &placev1.AddPlaceRequest{Name: strings.Repeat("я", 201)}, "name must be at most 200 characters"},
		{"NUL in info", &placev1.AddPlaceRequest{Name: "Парк", Info: "столик\x00"}, "info contains invalid characters"},
		{"info of 2001 characters", &placev1.AddPlaceRequest{Name: "Парк", Info: strings.Repeat("я", 2001)}, "info must be at most 2000 characters"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newClient(t, &serviceStub{t: t}).AddPlace(asUser(t, userID), tt.req)
			requireStatus(t, err, codes.InvalidArgument, tt.wantMsg)
		})
	}
}

func TestAddPlace_ServiceError(t *testing.T) {
	svc := &serviceStub{t: t}
	svc.addPlace = func(context.Context, string, string, string) (models.Place, error) {
		return models.Place{}, wrap(errStorage)
	}

	_, err := newClient(t, svc).AddPlace(asUser(t, userID), &placev1.AddPlaceRequest{Name: "Парк"})
	requireStatus(t, err, codes.Internal, "failed to add place")
}

// ----------------------------------------------------------------------------
// GetUserPlaces
// ----------------------------------------------------------------------------

func TestGetUserPlaces_Success(t *testing.T) {
	createdAt := time.Date(2026, 10, 22, 10, 30, 0, 123456000, time.UTC)
	places := []models.Place{
		{ID: placeID, UserID: otherID, Name: "Кафе у дома", Info: "столик у окна", CreatedAt: createdAt},
		{ID: "0b8e3c1d-2f4a-4b6c-8d9e-1f2a3b4c5d6e", UserID: otherID, Name: "Парк", Info: "", CreatedAt: createdAt.Add(time.Hour)},
	}

	svc := &serviceStub{t: t}
	svc.getUserPlaces = func(_ context.Context, id string) ([]models.Place, error) {
		// Список запрашивается для пользователя из тела, а не для того, кто смотрит.
		if id != otherID {
			t.Errorf("GetUserPlaces called with %q, want %q", id, otherID)
		}
		return places, nil
	}

	resp, err := newClient(t, svc).GetUserPlaces(asUser(t, userID), &placev1.GetUserPlacesRequest{UserId: otherID})
	if err != nil {
		t.Fatalf("GetUserPlaces returned error: %v", err)
	}

	got := resp.GetPlaces()
	if len(got) != len(places) {
		t.Fatalf("got %d places, want %d", len(got), len(places))
	}
	for i, p := range places {
		if got[i].GetName() != p.Name || got[i].GetInfo() != p.Info || !got[i].GetCreatedAt().AsTime().Equal(p.CreatedAt) {
			t.Errorf("place %d = %v, want %+v", i, got[i], p)
		}
	}
}

func TestGetUserPlaces_NoPlaces(t *testing.T) {
	svc := &serviceStub{t: t}
	svc.getUserPlaces = func(context.Context, string) ([]models.Place, error) { return []models.Place{}, nil }

	resp, err := newClient(t, svc).GetUserPlaces(asUser(t, userID), &placev1.GetUserPlacesRequest{UserId: userID})
	if err != nil {
		t.Fatalf("GetUserPlaces returned error: %v", err)
	}
	if len(resp.GetPlaces()) != 0 {
		t.Errorf("got %v, want no places", resp.GetPlaces())
	}
}

// ID в верхнем регистре - тот же пользователь: список берётся из того же ключа кеша,
// который сбрасывает AddPlace.
func TestGetUserPlaces_UserIDIsNormalized(t *testing.T) {
	var gotUser string
	svc := &serviceStub{t: t}
	svc.getUserPlaces = func(_ context.Context, id string) ([]models.Place, error) {
		gotUser = id
		return nil, nil
	}

	_, err := newClient(t, svc).GetUserPlaces(asUser(t, userID), &placev1.GetUserPlacesRequest{UserId: strings.ToUpper(otherID)})
	if err != nil {
		t.Fatalf("GetUserPlaces returned error: %v", err)
	}
	if gotUser != otherID {
		t.Errorf("service got user id %q, want %q", gotUser, otherID)
	}
}

func TestGetUserPlaces_InvalidArgument(t *testing.T) {
	tests := []struct {
		name    string
		userID  string
		wantMsg string
	}{
		{"empty user id", "", "user id is required"},
		{"not a UUID", "user-1", "user id must be a UUID"},
		{"UUID with spaces", " " + otherID + " ", "user id must be a UUID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newClient(t, &serviceStub{t: t}).GetUserPlaces(asUser(t, userID), &placev1.GetUserPlacesRequest{UserId: tt.userID})
			requireStatus(t, err, codes.InvalidArgument, tt.wantMsg)
		})
	}
}

func TestGetUserPlaces_ServiceError(t *testing.T) {
	svc := &serviceStub{t: t}
	svc.getUserPlaces = func(context.Context, string) ([]models.Place, error) { return nil, wrap(errStorage) }

	_, err := newClient(t, svc).GetUserPlaces(asUser(t, userID), &placev1.GetUserPlacesRequest{UserId: otherID})
	requireStatus(t, err, codes.Internal, "failed to get user places")
}
