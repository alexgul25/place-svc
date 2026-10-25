//go:build integration

package redis_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/alexgul25/place-svc/internal/cache"
	"github.com/alexgul25/place-svc/internal/domain"
	"github.com/alexgul25/place-svc/internal/domain/models"
	"github.com/alexgul25/place-svc/internal/infrastructure/serializer"
	"github.com/alexgul25/place-svc/internal/storage/redis"
)

// Тесты работают с Redis из TEST_REDIS_ADDR и не очищают его целиком: каждый тест
// берёт новые ID пользователей и удаляет свои ключи.

// envAddr - переменная окружения с адресом тестового Redis (host:port).
const envAddr = "TEST_REDIS_ADDR"

// testDB - номер базы Redis для тестов. Не 0, чтобы было видно, что номер доходит до Redis.
const testDB = 1

const testTTL = time.Hour

// ----------------------------------------------------------------------------
// Заглушки
// ----------------------------------------------------------------------------

// serializerStub сериализует через функции в полях. Если функция не задана,
// используется JSON, как в сервисе.
type serializerStub struct {
	marshal func(v any) ([]byte, error)
}

func (s serializerStub) Marshal(v any) ([]byte, error) {
	if s.marshal == nil {
		return serializer.JSONSerializer{}.Marshal(v)
	}
	return s.marshal(v)
}

func (s serializerStub) Unmarshal(data []byte, v any) error {
	return serializer.JSONSerializer{}.Unmarshal(data, v)
}

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// testAddr возвращает адрес тестового Redis.
func testAddr(t *testing.T) string {
	t.Helper()

	addr := os.Getenv(envAddr)
	if addr == "" {
		t.Fatalf("%s is not set, start a test Redis with `make test-infra-up`", envAddr)
	}
	return addr
}

// newCache подключается к тестовому Redis так, как это делает сервис, и закрывает
// подключение после теста.
func newCache(t *testing.T, s cache.CacheSerializer) *redis.PlaceCache {
	t.Helper()

	c, err := redis.NewPlaceCache(testAddr(t), "", "", testDB, 5*time.Second, 3*time.Second, 3*time.Second, s, testTTL)
	if err != nil {
		t.Fatalf("NewPlaceCache returned error: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// rawClient возвращает обычный клиент к базе db тестового Redis: через него тесты
// смотрят, что записал кеш, и готовят данные.
func rawClient(t *testing.T, db int) *goredis.Client {
	t.Helper()

	client := goredis.NewClient(&goredis.Options{Addr: testAddr(t), DB: db})
	t.Cleanup(func() { client.Close() })
	return client
}

// newUserID возвращает новый ID пользователя и удаляет его ключ после теста.
func newUserID(t *testing.T) string {
	t.Helper()

	id := uuid.NewString()
	client := rawClient(t, testDB)
	t.Cleanup(func() {
		// t.Context() к этому моменту уже отменён.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Del(ctx, cache.ToKeyUserPlaces(id)).Err(); err != nil {
			t.Errorf("failed to delete the key of %s: %v", id, err)
		}
	})
	return id
}

// testPlaces возвращает список мест пользователя, как его отдаёт PostgreSQL.
func testPlaces(userID string) []models.Place {
	createdAt := time.Date(2026, 5, 1, 10, 30, 0, 123456000, time.UTC)
	return []models.Place{
		{ID: uuid.NewString(), UserID: userID, Name: "Парк Горького", Info: "Прогулка у реки 🌳", CreatedAt: createdAt},
		{ID: uuid.NewString(), UserID: userID, Name: "Кафе", Info: "", CreatedAt: createdAt.Add(time.Hour).In(time.FixedZone("MSK", 3*60*60))},
	}
}

// checkPlaces сравнивает списки мест. Время сравнивается через Equal:
// после JSON часовой пояс восстанавливается как смещение, а не как исходная зона.
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

// ----------------------------------------------------------------------------
// Get, Set и Invalidate
// ----------------------------------------------------------------------------

func TestPlaceCache_SetAndGet(t *testing.T) {
	c := newCache(t, serializer.JSONSerializer{})
	userID := newUserID(t)
	places := testPlaces(userID)

	if err := c.Set(t.Context(), userID, places); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	got, err := c.Get(t.Context(), userID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	checkPlaces(t, got, places)
}

// Список пишется в базу из настроек под ключ user:<id>:places и живёт TTL.
func TestPlaceCache_SetUsesKeyDBAndTTL(t *testing.T) {
	c := newCache(t, serializer.JSONSerializer{})
	userID := newUserID(t)
	key := cache.ToKeyUserPlaces(userID)

	if err := c.Set(t.Context(), userID, testPlaces(userID)); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	ttl, err := rawClient(t, testDB).PTTL(t.Context(), key).Result()
	if err != nil {
		t.Fatalf("PTTL returned error: %v", err)
	}
	// Без TTL Redis возвращает -1, и список хранился бы до перезапуска Redis.
	if ttl <= testTTL-time.Minute || ttl > testTTL {
		t.Errorf("TTL of %s = %v, want about %v", key, ttl, testTTL)
	}

	if n, err := rawClient(t, 0).Exists(t.Context(), key).Result(); err != nil || n != 0 {
		t.Errorf("key %s in database 0: exists = %d (%v), want only in database %d", key, n, err, testDB)
	}
}

func TestPlaceCache_GetMiss(t *testing.T) {
	c := newCache(t, serializer.JSONSerializer{})

	if _, err := c.Get(t.Context(), newUserID(t)); !errors.Is(err, domain.ErrCacheMiss) {
		t.Errorf("got error %v, want %v", err, domain.ErrCacheMiss)
	}
}

// Пустой список - тоже ответ: пользователи без мест не должны ходить в БД при каждом запросе.
func TestPlaceCache_EmptyListIsCached(t *testing.T) {
	c := newCache(t, serializer.JSONSerializer{})
	userID := newUserID(t)

	if err := c.Set(t.Context(), userID, []models.Place{}); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	got, err := c.Get(t.Context(), userID)
	if err != nil {
		t.Fatalf("Get returned error %v, want an empty list", err)
	}
	if len(got) != 0 {
		t.Errorf("got %+v, want an empty list", got)
	}
}

func TestPlaceCache_Invalidate(t *testing.T) {
	c := newCache(t, serializer.JSONSerializer{})
	alice, bob := newUserID(t), newUserID(t)
	for _, id := range []string{alice, bob} {
		if err := c.Set(t.Context(), id, testPlaces(id)); err != nil {
			t.Fatalf("Set returned error: %v", err)
		}
	}

	if err := c.Invalidate(t.Context(), alice); err != nil {
		t.Fatalf("Invalidate returned error: %v", err)
	}

	if _, err := c.Get(t.Context(), alice); !errors.Is(err, domain.ErrCacheMiss) {
		t.Errorf("Get after Invalidate: got error %v, want %v", err, domain.ErrCacheMiss)
	}
	// Список другого пользователя остаётся в кеше.
	if _, err := c.Get(t.Context(), bob); err != nil {
		t.Errorf("Get of another user returned error: %v", err)
	}
	// Повторная очистка - не ошибка: после добавления места ключа может и не быть.
	if err := c.Invalidate(t.Context(), alice); err != nil {
		t.Errorf("second Invalidate returned error: %v", err)
	}
}

// Испорченное значение - ошибка, а не промах: сервис запишет в лог ошибку
// и возьмёт список из БД, а Set перезапишет ключ.
func TestPlaceCache_GetCorruptedValue(t *testing.T) {
	c := newCache(t, serializer.JSONSerializer{})
	userID := newUserID(t)

	if err := rawClient(t, testDB).Set(t.Context(), cache.ToKeyUserPlaces(userID), "not json", time.Minute).Err(); err != nil {
		t.Fatalf("failed to write the key: %v", err)
	}

	_, err := c.Get(t.Context(), userID)
	if err == nil || errors.Is(err, domain.ErrCacheMiss) {
		t.Errorf("got error %v, want a decoding error", err)
	}
}

func TestPlaceCache_SetSerializerError(t *testing.T) {
	errMarshal := errors.New("marshal failed")
	c := newCache(t, serializerStub{marshal: func(any) ([]byte, error) { return nil, errMarshal }})
	userID := newUserID(t)

	if err := c.Set(t.Context(), userID, testPlaces(userID)); !errors.Is(err, errMarshal) {
		t.Errorf("got error %v, want %v", err, errMarshal)
	}
	if n, err := rawClient(t, testDB).Exists(t.Context(), cache.ToKeyUserPlaces(userID)).Result(); err != nil || n != 0 {
		t.Errorf("key exists = %d (%v), want nothing written", n, err)
	}
}

// После Close методы возвращают ошибку. Get не выдаёт её за промах кеша.
func TestPlaceCache_Closed(t *testing.T) {
	c := newCache(t, serializer.JSONSerializer{})
	userID := newUserID(t)
	if err := c.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	if _, err := c.Get(t.Context(), userID); err == nil || errors.Is(err, domain.ErrCacheMiss) {
		t.Errorf("Get: got error %v, want a closed client error", err)
	}
	if err := c.Set(t.Context(), userID, testPlaces(userID)); err == nil {
		t.Error("Set succeeded, want error")
	}
	if err := c.Invalidate(t.Context(), userID); err == nil {
		t.Error("Invalidate succeeded, want error")
	}
}

// Redis не отвечает дольше REDIS_CACHE_READ_TIMEOUT: запрос завершается ошибкой,
// а не ждёт, пока Redis снова начнёт отвечать.
func TestPlaceCache_ReadTimeout(t *testing.T) {
	const (
		readTimeout = 200 * time.Millisecond
		pause       = 2 * time.Second
	)

	c, err := redis.NewPlaceCache(testAddr(t), "", "", testDB, 5*time.Second, readTimeout, time.Second, serializer.JSONSerializer{}, testTTL)
	if err != nil {
		t.Fatalf("NewPlaceCache returned error: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	// CLIENT PAUSE откладывает команды всех клиентов до конца паузы: Redis как будто завис.
	admin := rawClient(t, 0)
	if err := admin.Do(t.Context(), "CLIENT", "PAUSE", pause.Milliseconds(), "ALL").Err(); err != nil {
		t.Fatalf("CLIENT PAUSE returned error: %v", err)
	}
	t.Cleanup(func() {
		// Следующие тесты начинаются после паузы. CLIENT UNPAUSE тоже ждал бы её конца,
		// поэтому тест просто ждёт ответа на PING.
		ctx, cancel := context.WithTimeout(context.Background(), 2*pause)
		defer cancel()
		if err := admin.Ping(ctx).Err(); err != nil {
			t.Errorf("Redis did not answer after the pause: %v", err)
		}
	})

	start := time.Now()
	_, err = c.Get(t.Context(), uuid.NewString())
	elapsed := time.Since(start)

	// Без таймаута из настроек (в go-redis по умолчанию 3 секунды) Get дождался бы
	// конца паузы и вернул бы промах кеша.
	if err == nil || errors.Is(err, domain.ErrCacheMiss) {
		t.Fatalf("Get during pause: got error %v, want a timeout", err)
	}
	// go-redis повторяет запрос после таймаута, но всё равно не ждёт конца паузы.
	if elapsed >= pause*3/4 {
		t.Errorf("Get returned after %v, want well before the end of the %v pause", elapsed, pause)
	}
}

// ----------------------------------------------------------------------------
// NewPlaceCache
// ----------------------------------------------------------------------------

// Логин и пароль доходят до Redis: с пользователем ACL подключение проверяет оба.
// Redis без пароля принимает любой пароль пользователя default, поэтому тест
// создаёт отдельного пользователя.
func TestNewPlaceCache_Auth(t *testing.T) {
	username := "place_svc_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	const password = "s3cret-password"

	admin := rawClient(t, 0)
	if err := admin.Do(t.Context(), "ACL", "SETUSER", username, "on", ">"+password, "~*", "+@all").Err(); err != nil {
		t.Fatalf("ACL SETUSER returned error: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := admin.Do(ctx, "ACL", "DELUSER", username).Err(); err != nil {
			t.Errorf("ACL DELUSER returned error: %v", err)
		}
	})

	tests := []struct {
		name               string
		password, username string
		wantErr            bool
	}{
		{name: "valid credentials", password: password, username: username},
		{name: "wrong password", password: "wrong", username: username, wantErr: true},
		// Аргументы стоят в порядке password, username: перепутать их легко.
		{name: "password and username swapped", password: username, username: password, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := redis.NewPlaceCache(testAddr(t), tt.password, tt.username, testDB,
				5*time.Second, 3*time.Second, 3*time.Second, serializer.JSONSerializer{}, testTTL)
			if err == nil {
				defer c.Close()
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewPlaceCache error = %v, want error: %v", err, tt.wantErr)
			}
		})
	}
}

// Номер базы, которой нет на сервере (по умолчанию их 16), - ошибка при запуске,
// а не на каждом запросе.
func TestNewPlaceCache_DBOutOfRange(t *testing.T) {
	c, err := redis.NewPlaceCache(testAddr(t), "", "", 16, 5*time.Second, 3*time.Second, 3*time.Second, serializer.JSONSerializer{}, testTTL)
	if err == nil {
		c.Close()
		t.Fatal("NewPlaceCache succeeded, want error")
	}
	if !strings.Contains(err.Error(), "DB index is out of range") {
		t.Errorf("got error %v, want DB index is out of range", err)
	}
}
