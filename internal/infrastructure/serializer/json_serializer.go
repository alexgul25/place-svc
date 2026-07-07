package serializer

import "encoding/json"

type JSONSerializer struct{}

func (s JSONSerializer) Serialize(event any) ([]byte, error) {
	return json.Marshal(event)
}
