package kafka_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/alexgul25/place-svc/internal/infrastructure/kafka"
)

// Тесты работают с sarama.MockBroker: он отвечает по протоколу Kafka на заданные запросы,
// а на запросы без обработчика не отвечает вовсе, как зависший брокер.

const (
	topic       = "place.created"
	sendTimeout = 250 * time.Millisecond

	// Сколько тест ждёт ответа от продюсера, если брокер не отвечает. С таймаутами,
	// заданными sendTimeout, отправка занимает около четырёх таймаутов (попытки и паузы
	// между ними), а запуск - около трёх. С сетевыми таймаутами sarama по умолчанию
	// (30 с) отправка длилась 2 минуты, а запуск - 6.
	sendLimit    = 20 * sendTimeout
	startupLimit = 6 * sendTimeout
)

// newBroker запускает MockBroker, который знает о топике и отвечает на запись ответом produce.
// Если produce равен nil, запросы на запись остаются без ответа.
// Возвращает функцию, которая закрывает брокер: её можно вызвать раньше очистки теста.
func newBroker(t *testing.T, produce sarama.MockResponse) (*sarama.MockBroker, func()) {
	t.Helper()

	b := sarama.NewMockBroker(t, 1)
	handlers := map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(b.Addr(), b.BrokerID()).
			SetLeader(topic, 0, b.BrokerID()),
	}
	if produce != nil {
		handlers["ProduceRequest"] = produce
	}
	b.SetHandlerByMap(handlers)

	closeBroker := sync.OnceFunc(b.Close)
	t.Cleanup(closeBroker)
	return b, closeBroker
}

// newProducer создаёт продюсер и закрывает его после теста.
func newProducer(t *testing.T, addr string) *kafka.SyncProducer {
	t.Helper()

	p, err := kafka.NewSyncProducer([]string{addr}, "place-svc", sendTimeout)
	if err != nil {
		t.Fatalf("NewSyncProducer returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("Close returned error: %v", err)
		}
	})
	return p
}

// produceRequests возвращает запросы на запись, которые получил брокер.
func produceRequests(b *sarama.MockBroker) []*sarama.ProduceRequest {
	var result []*sarama.ProduceRequest
	for _, rr := range b.History() {
		if req, ok := rr.Request.(*sarama.ProduceRequest); ok {
			result = append(result, req)
		}
	}
	return result
}

func send(ctx context.Context, p *kafka.SyncProducer) error {
	return p.SendMessage(ctx, "record-1", topic, []byte("user-1"), []byte(`{"place_id":"place-1"}`))
}

// ----------------------------------------------------------------------------
// SendMessage
// ----------------------------------------------------------------------------

func TestSyncProducer_SendMessage(t *testing.T) {
	b, _ := newBroker(t, sarama.NewMockProduceResponse(t))
	p := newProducer(t, b.Addr())

	if err := send(t.Context(), p); err != nil {
		t.Fatalf("SendMessage returned error: %v", err)
	}

	reqs := produceRequests(b)
	if len(reqs) != 1 {
		t.Fatalf("broker got %d produce requests, want 1", len(reqs))
	}
	// Запись подтверждается, только когда её сохранили все реплики в синхронизации.
	if reqs[0].RequiredAcks != sarama.WaitForAll {
		t.Errorf("RequiredAcks = %d, want WaitForAll", reqs[0].RequiredAcks)
	}
	// Брокер ждёт подтверждения реплик не дольше KAFKA_PRODUCER_SEND_TIMEOUT.
	if want := int32(sendTimeout / time.Millisecond); reqs[0].Timeout != want {
		t.Errorf("Timeout = %d ms, want %d ms", reqs[0].Timeout, want)
	}
}

func TestSyncProducer_BrokerError(t *testing.T) {
	// Ошибку, которую не исправить повтором, продюсер возвращает сразу.
	produce := sarama.NewMockProduceResponse(t).SetError(topic, 0, sarama.ErrMessageSizeTooLarge)
	b, _ := newBroker(t, produce)
	p := newProducer(t, b.Addr())

	if err := send(t.Context(), p); !errors.Is(err, sarama.ErrMessageSizeTooLarge) {
		t.Errorf("SendMessage error = %v, want %v", err, sarama.ErrMessageSizeTooLarge)
	}
}

func TestSyncProducer_CanceledContext(t *testing.T) {
	b, _ := newBroker(t, sarama.NewMockProduceResponse(t))
	p := newProducer(t, b.Addr())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := send(ctx, p); !errors.Is(err, context.Canceled) {
		t.Errorf("SendMessage error = %v, want %v", err, context.Canceled)
	}
	if reqs := produceRequests(b); len(reqs) != 0 {
		t.Errorf("broker got %d produce requests after the context was canceled", len(reqs))
	}
}

// Брокер принял соединение, но не отвечает (завис или перегружен). Отправка и остановка
// продюсера должны укладываться в сетевые таймауты, заданные KAFKA_PRODUCER_SEND_TIMEOUT:
// остановка приложения закрывает продюсер и ждёт отправку, которая идёт в этот момент.
func TestSyncProducer_UnresponsiveBroker(t *testing.T) {
	b, closeBroker := newBroker(t, nil)
	p := newProducer(t, b.Addr())

	result := make(chan error, 1)
	go func() { result <- send(context.Background(), p) }()

	select {
	case err := <-result:
		if err == nil {
			t.Error("SendMessage returned nil, want a timeout error")
		}
	case <-time.After(sendLimit):
		// Закрытый брокер разрывает соединение, и отправка завершится до закрытия продюсера.
		closeBroker()
		t.Fatalf("SendMessage did not return within %v: network timeouts do not follow KAFKA_PRODUCER_SEND_TIMEOUT", sendLimit)
	}
}

// ----------------------------------------------------------------------------
// NewSyncProducer
// ----------------------------------------------------------------------------

func TestNewSyncProducer_UnreachableBroker(t *testing.T) {
	// Порт, на котором никто не слушает: соединение сразу отклоняется.
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find a free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	p, err := kafka.NewSyncProducer([]string{addr}, "place-svc", sendTimeout)
	if err == nil {
		p.Close()
		t.Fatal("NewSyncProducer returned nil error for an unreachable broker")
	}
	if p != nil {
		t.Errorf("NewSyncProducer returned %v with an error, want nil", p)
	}
}

// Брокер принимает соединения, но ни на что не отвечает. Запуск сервиса должен завершиться
// ошибкой за время, заданное KAFKA_PRODUCER_SEND_TIMEOUT, а не за минуты.
func TestNewSyncProducer_UnresponsiveBroker(t *testing.T) {
	b := sarama.NewMockBroker(t, 1)
	b.SetHandlerByMap(map[string]sarama.MockResponse{})
	closeBroker := sync.OnceFunc(b.Close)
	t.Cleanup(closeBroker)

	type result struct {
		p   *kafka.SyncProducer
		err error
	}
	done := make(chan result, 1)
	go func() {
		p, err := kafka.NewSyncProducer([]string{b.Addr()}, "place-svc", sendTimeout)
		done <- result{p, err}
	}()

	select {
	case r := <-done:
		if r.err == nil {
			r.p.Close()
			t.Fatal("NewSyncProducer returned nil error for an unresponsive broker")
		}
	case <-time.After(startupLimit):
		closeBroker()
		t.Fatalf("NewSyncProducer did not return within %v: network timeouts do not follow KAFKA_PRODUCER_SEND_TIMEOUT", startupLimit)
	}
}

// Адрес, до которого пакеты не доходят: подключение не отклоняется, а висит до таймаута,
// как с выключенным хостом Kafka за файрволом.
const blackholeAddr = "10.255.255.1:9092"

func TestNewSyncProducer_BlackholeBroker(t *testing.T) {
	// В некоторых сетях такой адрес сразу отвечает ошибкой: тогда проверять нечего.
	d := net.Dialer{Timeout: sendTimeout}
	conn, err := d.DialContext(t.Context(), "tcp", blackholeAddr)
	if err == nil {
		conn.Close()
		t.Skipf("%s accepts connections in this network", blackholeAddr)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Skipf("%s fails at once in this network (%v), the dial timeout cannot be checked", blackholeAddr, err)
	}

	done := make(chan error, 1)
	go func() {
		p, err := kafka.NewSyncProducer([]string{blackholeAddr}, "place-svc", sendTimeout)
		if err == nil {
			p.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("NewSyncProducer returned nil error for an unreachable broker")
		}
	case <-time.After(startupLimit):
		t.Fatalf("NewSyncProducer did not return within %v: the dial timeout does not follow KAFKA_PRODUCER_SEND_TIMEOUT", startupLimit)
	}
}
