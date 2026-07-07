package outbox

type EventSerializer interface {
	Serialize(event any) ([]byte, error)
}
