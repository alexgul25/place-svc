package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type SyncProducer struct {
	source   []byte
	producer sarama.SyncProducer
}

// NewSyncProducer подключается к брокерам и получает метаданные кластера. sarama не принимает
// context, а с брокером, который не отвечает, подключение длится до трёх timeout. Поэтому
// подключение идёт в отдельной горутине: если ctx отменяется раньше (например, по SIGTERM
// во время запуска сервиса), NewSyncProducer сразу возвращает ошибку, а продюсер, если
// подключение всё-таки удастся, закрывается в фоне.
func NewSyncProducer(ctx context.Context, brokers []string, source string, timeout time.Duration) (*SyncProducer, error) {
	config := sarama.NewConfig()

	// note from sarama docs:
	// both channels must be set to 'true' for sync producer
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Timeout = timeout

	// sarama не принимает context, поэтому время работы с брокером ограничивают только
	// сетевые таймауты. По умолчанию они 30 с: с брокером, который не отвечает, отправка
	// длилась 2 минуты (четыре попытки), а подключение при запуске - 6. Теперь каждая
	// сетевая операция и получение метаданных ограничены тем же timeout.
	config.Net.DialTimeout = timeout
	config.Net.ReadTimeout = timeout
	config.Net.WriteTimeout = timeout
	config.Metadata.Timeout = timeout

	type result struct {
		producer sarama.SyncProducer
		err      error
	}
	done := make(chan result, 1)
	go func() {
		producer, err := sarama.NewSyncProducer(brokers, config)
		done <- result{producer, err}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			return nil, fmt.Errorf("failed to create sync producer: %w", r.err)
		}
		return &SyncProducer{
			source:   []byte(source),
			producer: r.producer,
		}, nil
	case <-ctx.Done():
		go func() {
			if r := <-done; r.err == nil {
				_ = r.producer.Close()
			}
		}()
		return nil, fmt.Errorf("failed to create sync producer: %w", ctx.Err())
	}
}

// SendMessage отправляет сообщение и ждёт подтверждения брокера. ctx проверяется только
// перед отправкой: дальше время ограничивают сетевые таймауты продюсера.
func (sp *SyncProducer) SendMessage(ctx context.Context, messageID string, topic string, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("source"), Value: sp.source},
			{Key: []byte("message_id"), Value: []byte(messageID)},
		},
	}

	_, _, err := sp.producer.SendMessage(msg)
	return err
}

func (sp *SyncProducer) Close() error {
	return sp.producer.Close()
}
