package cache

type CacheSerializer interface {
	Marshal(event any) (data []byte, err error)
	Unmarshal(data []byte, v any) error
}
