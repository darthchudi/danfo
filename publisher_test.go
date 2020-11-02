package danfo

import (
	"testing"
)

func TestPublish(t *testing.T) {
	publisher, err := NewPublisher("amqp://localhost:5672")

	if err != nil {
		t.Fatalf("Couldn't start publisher")
	}

	err = publisher.Queue("NOTIFICATION_QUEUE", []byte(`{"name": "random-name", "age": 22}`))

	if err != nil {
		t.Fatalf("Queue error: %v", err)
	}
}
