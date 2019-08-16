package client

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

const (
	TestKey   = "test_key"
	TimeoutMs = 100
)

func getMsSince(startTime time.Time) int {
	return int(time.Now().Sub(startTime).Nanoseconds() / 1e6)
}

func TestHandlerMapEarlyStoreRetrieve(t *testing.T) {

	handler_map := NewHandlerMap()
	var handler ResponseHandler = func(*Response) {
		t.Logf("test: got a response \n")
	}
	handler_map.Put(TestKey, handler)
	myHandler, ok := handler_map.Get(TestKey, 20)
	if !ok {
		t.Error("Failed to get test key")
	}
	myHandler(nil)

}

func TestHandlerMapDelayedPutRetrieve(t *testing.T) {

	handler_map := NewHandlerMap()
	startTime := time.Now()

	go func() {
		time.Sleep(10 * time.Millisecond)

		// at this point the Get would be waiting for the response.
		counts, waiters := handler_map.GetCounts()
		assert.Equal(t, 0, counts, "Map Elements")
		assert.Equal(t, 1, waiters, "Waiter groups")

		var handler ResponseHandler = func(*Response) {
			t.Logf("test: got a response at time %d ms after start\n", getMsSince(startTime))
		}
		handler_map.Put(TestKey, handler)
	}()

	t.Logf("test: started waiting for key at %d ms after start\n", getMsSince(startTime))
	t.Logf("test: started waiting for key at %d ms after start\n", getMsSince(startTime))
	myHandler, ok := handler_map.Get(TestKey, 20)
	if !ok {
		t.Error("Failed to get test key")
	}

	myHandler(nil)
}

func TestHandlerMapTimeoutPutTooLate(t *testing.T) {

	handler_map := NewHandlerMap()
	startTime := time.Now()

	go func() {
		time.Sleep(2 * TimeoutMs * time.Millisecond)
		var handler ResponseHandler = func(*Response) {
			t.Logf("test: got a response at time %d ms after start\n", getMsSince(startTime))
		}
		handler_map.Put(TestKey, handler)
	}()

	t.Logf("test: started waiting for key at %d ms after start\n", getMsSince(startTime))
	_, ok := handler_map.Get(TestKey, TimeoutMs)
	if ok {
		t.Error("Should have timed out when getting the key")
		return
	} else {
		actualTimeoutMs := getMsSince(startTime)
		t.Logf("test: timed out waiting for key at %d ms after start\n", actualTimeoutMs)
		var comp assert.Comparison = func() (success bool) {
			return math.Abs(float64(actualTimeoutMs-TimeoutMs))/TimeoutMs < 0.1
		}
		assert.Condition(t, comp, "Timeout did not occur within 10%% margin, expected timeout ms: %d", TimeoutMs)
		// wait till producer has added the element
		time.Sleep(3 * TimeoutMs * time.Millisecond)
		counts, waiters := handler_map.GetCounts()
		assert.Equal(t, 1, counts, "Map elements")
		assert.Equal(t, 0, waiters, "Waiter groups")
	}

}
