package redis_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/alexgul25/place-svc/internal/infrastructure/serializer"
	"github.com/alexgul25/place-svc/internal/storage/redis"
)

// Эти тесты не требуют Redis и запускаются вместе с unit-тестами.

// connect вызывает NewPlaceCache с заданным адресом и таймаутом подключения.
func connect(addr string, dialTimeout time.Duration) (*redis.PlaceCache, error) {
	return redis.NewPlaceCache(addr, "", "", 0, dialTimeout, time.Second, time.Second, serializer.JSONSerializer{}, time.Minute)
}

func TestNewPlaceCache_Unreachable(t *testing.T) {
	before := runtime.NumGoroutine()

	// Порт 1 на localhost закрыт: соединение отклоняется сразу.
	for range 3 {
		if c, err := connect("127.0.0.1:1", 100*time.Millisecond); err == nil {
			c.Close()
			t.Fatal("NewPlaceCache succeeded, want error")
		}
	}

	// Клиент go-redis запускает фоновую горутину при создании. Если после ошибки
	// клиент не закрыть, горутина остаётся работать: по одной на каждый вызов.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines are running after failed NewPlaceCache calls, %d before", runtime.NumGoroutine(), before)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Сервис не должен зависать при запуске, если Redis недоступен по сети:
// пакеты на адрес 10.255.255.1 никуда не доходят, и соединение ждёт ответа.
func TestNewPlaceCache_DialTimeout(t *testing.T) {
	const dialTimeout = 500 * time.Millisecond

	start := time.Now()
	c, err := connect("10.255.255.1:6379", dialTimeout)
	elapsed := time.Since(start)
	if err == nil {
		c.Close()
		t.Fatal("NewPlaceCache succeeded, want error")
	}

	// В некоторых сетях адрес сразу отвечает «нет маршрута», и таймаут не проверить.
	if elapsed < dialTimeout/2 {
		t.Skipf("10.255.255.1 failed in %v (%v), the network does not drop packets", elapsed, err)
	}
	// Без ограничения по времени go-redis повторяет подключение ещё трижды.
	if elapsed > 3*dialTimeout {
		t.Errorf("NewPlaceCache returned after %v, want about %v", elapsed, dialTimeout)
	}
}
