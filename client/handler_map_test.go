package client

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

const (
	testKey        = "test_key"
	timeoutMs      = 200
	marginErrorPct = 10
)

func getMsSince(startTime time.Time) int {
	return int(time.Now().Sub(startTime).Nanoseconds() / 1e6)
}

func TestHandlerMapEarlyStoreRetrieve(t *testing.T) {

	handler_map := NewHandlerMap()
	var handler ResponseHandler = func(*Response) {
		t.Logf("test: got a response \n")
	}
	handler_map.Put(testKey, handler)
	myHandler, ok := handler_map.Get(testKey, timeoutMs)
	if !ok {
		t.Error("Failed to get test key")
	}
	myHandler(nil)

}

func TestHandlerMapDelayedPutRetrieve(t *testing.T) {

	handler_map := NewHandlerMap()
	startTime := time.Now()
	expectedResponseMs := timeoutMs / 2

	go func() {
		time.Sleep(time.Duration(expectedResponseMs) * time.Millisecond)

		// at this point the Get would be waiting for the response.
		counts, waiters := handler_map.GetCounts()
		assert.Equal(t, 0, counts, "Map Elements")
		assert.Equal(t, 1, waiters, "Waiter groups")

		var handler ResponseHandler = func(*Response) {
			t.Logf("test: got a response at time %d ms after start\n", getMsSince(startTime))
		}
		handler_map.Put(testKey, handler)
	}()

	t.Logf("test: started waiting for key at %d ms after start\n", getMsSince(startTime))
	myHandler, ok := handler_map.Get(testKey, timeoutMs)
	if !ok {
		t.Error("Failed to get test key")
	} else {
		myHandler(nil)
		actualResponseMs := getMsSince(startTime)
		var comp assert.Comparison = func() (success bool) {
			return math.Abs(float64(actualResponseMs-expectedResponseMs))/float64(expectedResponseMs) < float64(marginErrorPct)/100
		}
		assert.Condition(t, comp, "Response did not arrive within %d%% margin, expected time %d ms", marginErrorPct, expectedResponseMs)

	}

}

func TestHandlerMapTimeoutPutTooLate(t *testing.T) {

	handler_map := NewHandlerMap()
	startTime := time.Now()

	go func() {
		time.Sleep(2 * timeoutMs * time.Millisecond)
		handler_map.Put(testKey, func(*Response) {})
	}()

	t.Logf("test: started waiting for key at %d ms after start\n", getMsSince(startTime))
	_, ok := handler_map.Get(testKey, timeoutMs)
	if ok {
		t.Error("Should have timed out when getting the key")
		return
	} else {
		actualTimeoutMs := getMsSince(startTime)
		t.Logf("test: timed out waiting for key at %d ms after start\n", actualTimeoutMs)
		var comp assert.Comparison = func() (success bool) {
			return math.Abs(float64(actualTimeoutMs-timeoutMs))/timeoutMs < float64(marginErrorPct)/100
		}
		assert.Condition(t, comp, "Timeout did not occur within %d%% margin, expected timeout ms: %d", marginErrorPct, timeoutMs)
		// wait till producer has added the element
		time.Sleep(3 * timeoutMs * time.Millisecond)
		counts, waiters := handler_map.GetCounts()
		assert.Equal(t, 1, counts, "Map elements")
		assert.Equal(t, 0, waiters, "Waiter groups")
	}

}
