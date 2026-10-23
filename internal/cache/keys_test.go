package cache_test

import (
	"testing"

	"github.com/alexgul25/place-svc/internal/cache"
)

func TestToKeyUserPlaces(t *testing.T) {
	const userID = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"

	// Формат ключа виден в Redis (redis-cli keys 'user:*'): его смена оставит старые записи до истечения TTL.
	if got, want := cache.ToKeyUserPlaces(userID), "user:"+userID+":places"; got != want {
		t.Errorf("ToKeyUserPlaces(%q) = %q, want %q", userID, got, want)
	}

	if cache.ToKeyUserPlaces("user-1") == cache.ToKeyUserPlaces("user-2") {
		t.Error("different users must get different keys")
	}
}
