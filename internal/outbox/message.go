package outbox

import "time"

const (
	StatusPending   = "pending"
	StatusPublished = "published"
)

type Message struct {
	Topic   string `json:"topic"`
	Key     string `json:"key"`
	Payload []byte `json:"payload"`
}

type Record struct {
	ID          string
	Message     Message
	SendStatus  string
	CreatedAt   time.Time
	PublishedAt time.Time
}
