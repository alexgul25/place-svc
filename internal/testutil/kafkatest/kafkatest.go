// Package kafkatest помогает интеграционным тестам работать с Kafka из TEST_KAFKA_BROKERS:
// создавать и удалять топики и читать из них сообщения.
//
// Тестовая Kafka, как и при деплое, не создаёт топики при первом обращении,
// поэтому тесты создают нужные им топики сами.
package kafkatest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// EnvBrokers - переменная окружения со списком брокеров тестовой Kafka через запятую.
const EnvBrokers = "TEST_KAFKA_BROKERS"

// Brokers возвращает брокеров из TEST_KAFKA_BROKERS.
func Brokers() ([]string, error) {
	var brokers []string
	for b := range strings.SplitSeq(os.Getenv(EnvBrokers), ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafkatest: %s is not set, start a test Kafka with `make test-infra-up`", EnvBrokers)
	}
	return brokers, nil
}

// CreateTopic создаёт топик с одной партицией, если его ещё нет, и ждёт, пока у партиции
// появится лидер: сразу после создания запись в топик может не пройти.
func CreateTopic(ctx context.Context, brokers []string, topic string) error {
	admin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
	if err != nil {
		return fmt.Errorf("kafkatest: connect to %v: %w", brokers, err)
	}
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
	// Топик мог создать тестовый пакет, который go test запустил параллельно.
	if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		return fmt.Errorf("kafkatest: create topic %s: %w", topic, err)
	}

	client, err := sarama.NewClient(brokers, sarama.NewConfig())
	if err != nil {
		return fmt.Errorf("kafkatest: connect to %v: %w", brokers, err)
	}
	defer client.Close()

	for {
		if err := client.RefreshMetadata(topic); err == nil {
			if _, err := client.Leader(topic, 0); err == nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafkatest: topic %s has no leader: %w", topic, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// DeleteTopic удаляет топик, созданный тестом.
func DeleteTopic(brokers []string, topic string) error {
	admin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
	if err != nil {
		return fmt.Errorf("kafkatest: connect to %v: %w", brokers, err)
	}
	defer admin.Close()

	if err := admin.DeleteTopic(topic); err != nil {
		return fmt.Errorf("kafkatest: delete topic %s: %w", topic, err)
	}
	return nil
}

// Message - прочитанное из Kafka сообщение.
type Message struct {
	Key     string
	Value   []byte
	Headers map[string]string
}

// Consume читает партицию 0 топика с самого начала и возвращает первые n сообщений,
// для которых match возвращает true. Остальные пропускаются: в общий топик пишут
// и тесты других пакетов. Если за время ctx нужных сообщений не набралось,
// Consume возвращает те, что успел найти, и ошибку.
func Consume(ctx context.Context, brokers []string, topic string, n int, match func(Message) bool) ([]Message, error) {
	consumer, err := sarama.NewConsumer(brokers, sarama.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("kafkatest: connect to %v: %w", brokers, err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return nil, fmt.Errorf("kafkatest: consume %s: %w", topic, err)
	}
	defer pc.Close()

	var found []Message
	for len(found) < n {
		select {
		case m := <-pc.Messages():
			msg := Message{Key: string(m.Key), Value: m.Value, Headers: make(map[string]string, len(m.Headers))}
			for _, h := range m.Headers {
				msg.Headers[string(h.Key)] = string(h.Value)
			}
			if match(msg) {
				found = append(found, msg)
			}
		case <-ctx.Done():
			return found, fmt.Errorf("kafkatest: got %d of %d messages from %s: %w", len(found), n, topic, ctx.Err())
		}
	}
	return found, nil
}
