//go:build integration

package kafka_test

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/alexgul25/place-svc/internal/infrastructure/kafka"
	"github.com/alexgul25/place-svc/internal/testutil/kafkatest"
)

// Эти тесты работают с Kafka из TEST_KAFKA_BROKERS. MockBroker в unit-тестах отвечает
// так, как его настроили; здесь проверяется, что настоящий брокер принимает запросы продюсера.

// newTopic создаёт для теста отдельный топик и удаляет его после теста.
func newTopic(t *testing.T) (brokers []string, topic string) {
	t.Helper()

	brokers, err := kafkatest.Brokers()
	if err != nil {
		t.Fatal(err)
	}

	topic = "place-svc-test." + strings.ToLower(rand.Text()[:8])
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := kafkatest.CreateTopic(ctx, brokers, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := kafkatest.DeleteTopic(brokers, topic); err != nil {
			t.Error(err)
		}
	})
	return brokers, topic
}

func TestSyncProducer_SendsToKafka(t *testing.T) {
	brokers, topic := newTopic(t)

	p, err := kafka.NewSyncProducer(t.Context(), brokers, "place-svc", 10*time.Second)
	if err != nil {
		t.Fatalf("NewSyncProducer returned error: %v", err)
	}
	defer p.Close()

	// Те же ключ и значение, что у события о новом месте: ID пользователя и JSON.
	const (
		key   = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
		value = `{"place_id":"9b2e6f1a-7c4d-4e8f-a1b2-c3d4e5f60718","name":"Парк"}`
	)
	for i, id := range []string{"record-1", "record-2"} {
		if err := p.SendMessage(t.Context(), id, topic, []byte(key), []byte(value)); err != nil {
			t.Fatalf("SendMessage %d returned error: %v", i+1, err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	msgs, err := kafkatest.Consume(ctx, brokers, topic, 2, func(kafkatest.Message) bool { return true })
	if err != nil {
		t.Fatal(err)
	}

	// Сообщения приходят в порядке отправки, с ключом, значением и заголовками,
	// по которым получатель узнаёт отправителя и отбрасывает повторы.
	for i, wantID := range []string{"record-1", "record-2"} {
		m := msgs[i]
		if m.Key != key || string(m.Value) != value {
			t.Errorf("message %d = (%q, %q), want (%q, %q)", i+1, m.Key, m.Value, key, value)
		}
		if m.Headers["source"] != "place-svc" || m.Headers["message_id"] != wantID {
			t.Errorf("message %d headers = %v, want source place-svc and message_id %s", i+1, m.Headers, wantID)
		}
	}
}
