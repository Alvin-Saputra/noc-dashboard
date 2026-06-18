package api

import (
	"testing"
	"time"
)

func TestBrokerFanOut(t *testing.T) {
	broker := NewBroker()

	client1 := make(chan []byte, 1)
	client2 := make(chan []byte, 1)

	broker.newClients <- client1
	broker.newClients <- client2

	time.Sleep(50 * time.Millisecond)

	testMessage := []byte(`{"alert": "CPU High"}`)
	broker.Notifier <- testMessage

	select {
	case msg1 := <-client1:
		if string(msg1) != string(testMessage) {
			t.Errorf("Client 1 receives wrong message")
		}
	case <-time.After(1 * time.Second):
		t.Errorf("Client 1 did not receive broadcast!")
	}

	select {
	case msg2 := <-client2:
		if string(msg2) != string(testMessage) {
			t.Errorf("Client 2 receives wrong message")
		}
	case <-time.After(1 * time.Second):
		t.Errorf("Client 2 did not receive broadcast!")
	}
}
