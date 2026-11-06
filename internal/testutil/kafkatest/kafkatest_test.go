package kafkatest

import (
	"slices"
	"strings"
	"testing"
)

// Тест не требует Kafka и запускается вместе с unit-тестами.

func TestBrokers(t *testing.T) {
	t.Run("not set", func(t *testing.T) {
		t.Setenv(EnvBrokers, "")

		if _, err := Brokers(); err == nil || !strings.Contains(err.Error(), "make test-infra-up") {
			t.Errorf("got error %v, want hint about make test-infra-up", err)
		}
	})

	t.Run("list with spaces and empty items", func(t *testing.T) {
		t.Setenv(EnvBrokers, " localhost:59092, ,kafka:9092,")

		got, err := Brokers()
		if err != nil {
			t.Fatalf("Brokers returned error: %v", err)
		}
		if want := []string{"localhost:59092", "kafka:9092"}; !slices.Equal(got, want) {
			t.Errorf("Brokers() = %q, want %q", got, want)
		}
	})
}
