package kafka

import (
	"bytes"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

// Внутренний тест: сообщение, которое уходит в sarama, проверяется на mocks.SyncProducer,
// подставленном вместо настоящего продюсера. MockBroker не показывает содержимое записей.

func TestSyncProducer_Message(t *testing.T) {
	mock := mocks.NewSyncProducer(t, nil)
	mock.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		if msg.Topic != "place.created" {
			t.Errorf("Topic = %q, want place.created", msg.Topic)
		}
		// Ключ - ID пользователя: события одного пользователя попадают в одну партицию по порядку.
		checkEncoded(t, "Key", msg.Key, "user-1")
		checkEncoded(t, "Value", msg.Value, `{"place_id":"place-1"}`)

		// Заголовки - контракт с потребителями: source называет сервис, а message_id
		// (ID записи outbox) позволяет отличить повторную доставку.
		want := []sarama.RecordHeader{
			{Key: []byte("source"), Value: []byte("place-svc")},
			{Key: []byte("message_id"), Value: []byte("record-1")},
		}
		if len(msg.Headers) != len(want) {
			t.Fatalf("Headers = %q, want %q", msg.Headers, want)
		}
		for i := range want {
			if !bytes.Equal(msg.Headers[i].Key, want[i].Key) || !bytes.Equal(msg.Headers[i].Value, want[i].Value) {
				t.Errorf("Headers[%d] = %s=%s, want %s=%s", i, msg.Headers[i].Key, msg.Headers[i].Value, want[i].Key, want[i].Value)
			}
		}
		return nil
	})

	p := &SyncProducer{source: []byte("place-svc"), producer: mock}
	if err := p.SendMessage(t.Context(), "record-1", "place.created", []byte("user-1"), []byte(`{"place_id":"place-1"}`)); err != nil {
		t.Fatalf("SendMessage returned error: %v", err)
	}

	// Close мока сообщает, если ожидаемая отправка не произошла.
	if err := p.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

func TestSyncProducer_SendError(t *testing.T) {
	mock := mocks.NewSyncProducer(t, nil)
	mock.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)

	p := &SyncProducer{source: []byte("place-svc"), producer: mock}
	if err := p.SendMessage(t.Context(), "record-1", "place.created", nil, []byte("{}")); !errors.Is(err, sarama.ErrOutOfBrokers) {
		t.Errorf("SendMessage error = %v, want %v", err, sarama.ErrOutOfBrokers)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

func checkEncoded(t *testing.T, name string, enc sarama.Encoder, want string) {
	t.Helper()

	if enc == nil {
		t.Errorf("%s is nil, want %q", name, want)
		return
	}
	got, err := enc.Encode()
	if err != nil {
		t.Fatalf("failed to encode %s: %v", name, err)
	}
	if string(got) != want {
		t.Errorf("%s = %q, want %q", name, got, want)
	}
}
