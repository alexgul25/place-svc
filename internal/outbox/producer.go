package outbox

import "context"

type SyncMessageProducer interface {
	SendMessage(ctx context.Context, topic string, key, value []byte) error
	Close() error
}
