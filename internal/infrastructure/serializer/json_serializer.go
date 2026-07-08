package serializer

import "encoding/json"

type JSONSerializer struct{}

func (s JSONSerializer) Marshal(event any) ([]byte, error) {
	return json.Marshal(event)
}

func (s JSONSerializer) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
