package outbox

type EventSerializer interface {
	Marshal(event any) ([]byte, error)
}
