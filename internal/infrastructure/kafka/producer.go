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

func NewSyncProducer(brokers []string, source string, timeout time.Duration) (*SyncProducer, error) {
	config := sarama.NewConfig()

	// note from sarama docs:
	// both channels must be set to 'true' for sync producer
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Timeout = timeout

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer: %w", err)
	}

	return &SyncProducer{
		source:   []byte(source),
		producer: producer,
	}, nil
}

func (sp *SyncProducer) SendMessage(ctx context.Context, topic string, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("source"), Value: sp.source},
		},
	}

	_, _, err := sp.producer.SendMessage(msg)
	return err
}

func (sp *SyncProducer) Close() error {
	return sp.producer.Close()
}
