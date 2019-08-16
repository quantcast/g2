package client

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	TEST_KEY = "test_key"
)

func getMsSince(startTime time.Time) int {
	return int(time.Now().Sub(startTime).Nanoseconds() / 1e6)
}

func TestHandlerMapEarlyStoreRetrieve(t *testing.T) {

	handler_map := NewHandlerMap()
	var handler ResponseHandler = func(*Response) {
		fmt.Printf("test: I got a response \n")
	}
	handler_map.Put(TEST_KEY, handler)
	myHandler, ok := handler_map.Get(TEST_KEY, 20)
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
			fmt.Printf("test: I got a response at time %d ms after start\n", getMsSince(startTime))
		}
		handler_map.Put(TEST_KEY, handler)
	}()

	fmt.Printf("test: Started waiting for key at %d ms after start\n", getMsSince(startTime))
	myHandler, ok := handler_map.Get(TEST_KEY, 20)
	if !ok {
		t.Error("Failed to get test key")
	}

	myHandler(nil)
}

func TestHandlerMapTimeoutPutTooLate(t *testing.T) {

	handler_map := NewHandlerMap()
	startTime := time.Now()

	go func() {
		time.Sleep(30 * time.Millisecond)
		var handler ResponseHandler = func(*Response) {
			fmt.Printf("test: I got a response at time %d ms after start\n", getMsSince(startTime))
		}
		handler_map.Put(TEST_KEY, handler)
	}()

	fmt.Printf("test: Started waiting for key at %d ms after start\n", getMsSince(startTime))
	_, ok := handler_map.Get(TEST_KEY, 20)
	if ok {
		t.Error("Should have timed out when getting the key")
		return
	} else {
		// wait till producer has added the element
		time.Sleep(20 * time.Millisecond)
		counts, waiters := handler_map.GetCounts()
		assert.Equal(t, 1, counts, "Map elements")
		assert.Equal(t, 0, waiters, "Waiter groups")
	}

}
