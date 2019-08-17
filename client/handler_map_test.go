package client

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

const (
	testKey       = "test_key"
	timeoutMs     = 200
	marginErrorMs = 10
	numHandlers   = 20
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
			return math.Abs(float64(actualResponseMs-expectedResponseMs)) < marginErrorMs
		}
		assert.Condition(t, comp, "Response did not arrive within %d ms margin, expected time %d ms, actual time %d ms", marginErrorMs, expectedResponseMs, actualResponseMs)

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
			return math.Abs(float64(actualTimeoutMs-timeoutMs)) < marginErrorMs
		}
		assert.Condition(t, comp, "Timeout did not occur within %d ms margin, expected timeout ms: %d, actual time %d ms", marginErrorMs, timeoutMs, actualTimeoutMs)
		// wait till producer has added the element
		time.Sleep(3 * timeoutMs * time.Millisecond)
		counts, waiters := handler_map.GetCounts()
		assert.Equal(t, 1, counts, "Map elements")
		assert.Equal(t, 0, waiters, "Waiter groups")
	}

}

func TestMixedMultiPutGet(t *testing.T) {

	handler_map := NewHandlerMap()
	startTime := time.Now()
	delayedResponseTimeMs := timeoutMs / 2
	timedoutResponseTimeMs := timeoutMs + marginErrorMs

	go func() {
		var handler ResponseHandler = func(r *Response) {
			t.Logf("test: got a response [%s] at time %d ms after start\n", r.Handle, getMsSince(startTime))
		}

		for i := 0; i < numHandlers; i++ {
			key := fmt.Sprintf("%s-%d", testKey, i)
			handler_map.Put(key, handler)
		}

		// at this point the Get would be waiting for the response.
		counts, _ := handler_map.GetCounts()
		assert.Equal(t, numHandlers, counts, "Map Elements")

		time.Sleep(time.Duration(delayedResponseTimeMs) * time.Millisecond)

		// by now we have some non-delayed elements, and also have waiters for the delayed elements
		counts, waiters := handler_map.GetCounts()
		assert.Equal(t, numHandlers, counts, "Map Elements")    // elements for non-delayed ones
		assert.Equal(t, numHandlers*2, waiters, "Map Elements") // for delayed ones

		for i := 0; i < numHandlers; i++ {
			key := fmt.Sprintf("%s-%d-delayed", testKey, i)
			handler_map.Put(key, handler)
		}

		// at this point the Get would be waiting for the response.
		counts, _ = handler_map.GetCounts()
		assert.Equal(t, numHandlers*2, counts, "Map Elements")

		time.Sleep(time.Duration(timedoutResponseTimeMs) * time.Millisecond)
		for i := 0; i < numHandlers; i++ {
			key := fmt.Sprintf("%s-%d-toolate", testKey, i)
			handler_map.Put(key, handler)
		}

	}()

	completion := make(chan bool)
	getData := func(expectedTimeMs int, key string) {
		myHandler, ok := handler_map.Get(key, timeoutMs)

		if !ok {
			t.Errorf("Failed to get test key at time %d ms after start\n", getMsSince(startTime))
		} else {
			myHandler(&Response{Handle: key})
			actualResponseMs := getMsSince(startTime)
			var comp assert.Comparison = func() (success bool) {
				return math.Abs(float64(actualResponseMs-expectedTimeMs)) < marginErrorMs
			}
			assert.Condition(t, comp, "Response did not arrive within %d ms margin, expected time %d ms, actual time: %d ms", marginErrorMs, expectedTimeMs, actualResponseMs)
		}
		completion <- true
	}

	getTimeouts := func(expectedTimeMs int, key string) {
		_, ok := handler_map.Get(key, timeoutMs)

		if ok {
			t.Errorf("Should have gotten a timeout on key %s but received ok %d ms after start\n", key, getMsSince(startTime))
		} else {
			t.Logf("test: got the expected timeout on key %s %d ms after start\n", key, getMsSince(startTime))
			actualResponseMs := getMsSince(startTime)
			var comp assert.Comparison = func() (success bool) {
				return math.Abs(float64(actualResponseMs-expectedTimeMs)) < marginErrorMs
			}
			assert.Condition(t, comp, "Timeout did not occur within %d ms margin, expected time %d ms, actual time: %d ms", marginErrorMs, expectedTimeMs, actualResponseMs)
		}
		completion <- true
	}

	for i := 0; i < numHandlers; i++ {
		go getData(0, fmt.Sprintf("%s-%d", testKey, i))
		go getData(delayedResponseTimeMs, fmt.Sprintf("%s-%d-delayed", testKey, i))
		// second set of receivers
		go getData(delayedResponseTimeMs, fmt.Sprintf("%s-%d-delayed", testKey, i))
		go getTimeouts(timeoutMs, fmt.Sprintf("%s-%d-timedout", testKey, i))
	}

	// wait for completion of non-timed out response (immediate + delayed ones)
	for i := 0; i < numHandlers*3; i++ {
		_ = <-completion
	}

	counts, waiters := handler_map.GetCounts()
	assert.Equal(t, numHandlers*2, counts, "Map Elements")
	assert.Equal(t, numHandlers, waiters, "Waiter groups") // the "timedout" keys are waiting for timeout

	for i := 0; i < numHandlers; i++ {
		handler_map.Delete(fmt.Sprintf("%s-%d", testKey, i))
		handler_map.Delete(fmt.Sprintf("%s-%d-delayed", testKey, i))
	}

	counts, waiters = handler_map.GetCounts()
	assert.Equal(t, 0, counts, "Map Elements")
	assert.Equal(t, numHandlers, waiters, "Waiter groups")

	// wait for timed out responses
	for i := 0; i < numHandlers; i++ {
		_ = <-completion
	}

	// delete the timed out keys
	for i := 0; i < numHandlers; i++ {
		handler_map.Delete(fmt.Sprintf("%s-%d-timedout", testKey, i))
	}

	// at last should have no elements and no waiters
	counts, waiters = handler_map.GetCounts()
	assert.Equal(t, 0, counts, "Map Elements")
	assert.Equal(t, 0, waiters, "Waiter groups")

}
