package danfo

import "testing"

func TestNewSubscriber(t *testing.T) {
	subscriber, err := NewSubscriber("amqp://localhost")

	if err != nil {
		t.Fatalf("Failed to create subscriber")
	}

	if !subscriber.IsConnected || subscriber.Connection == nil || subscriber.Channel == nil {
		t.Fatal("Subscriber could not connect")
	}
}
