package client

import (
	"container/list"
	"golang.org/x/net/context"

	"sync"
	"time"
)

type HandlerMap struct {
	mu         sync.Mutex
	innerMap   map[string]ResponseHandler
	waitersMap map[string]*list.List
}

type waiter struct {
	ready chan<- ResponseHandler // Closed when semaphore acquired.
}

func NewHandlerMap() *HandlerMap {
	return &HandlerMap{sync.Mutex{},
		make(map[string]ResponseHandler, 100),
		make(map[string]*list.List, 100),
	}
}

func (m *HandlerMap) GetCounts() (counts int, waiters int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.innerMap), len(m.waitersMap)
}

func (m *HandlerMap) Put(key string, value ResponseHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.innerMap[key] = value
	// signal to any waiters here
	if waiters, ok := m.waitersMap[key]; ok {
		for {
			next := waiters.Front()
			if next == nil {
				break // No more waiters blocked.
			}
			w := next.Value.(waiter)
			waiters.Remove(next)
			w.ready <- value
			close(w.ready)
		}
		delete(m.waitersMap, key)
	}
}

func (m *HandlerMap) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.innerMap, key)
}

func (m *HandlerMap) Get(key string, timeoutMs int) (value ResponseHandler, ok bool) {
	m.mu.Lock()

	// optimistic check first
	value, ok = m.innerMap[key]
	if ok {
		m.mu.Unlock()
		return
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	waiters, wok := m.waitersMap[key]
	if !wok {
		waiters = &list.List{}
		m.waitersMap[key] = waiters
	}
	ready := make(chan ResponseHandler)
	w := waiter{ready: ready}
	elem := waiters.PushBack(w)
	m.mu.Unlock() // unlock before waiting

	select {
	case <-ctx.Done():
		m.mu.Lock()
		// check if the response arrived when it timed out
		select {
		case value = <-ready:
			ok = true
		default:
			// got timeout, let's remove waiter
			waiters.Remove(elem)
			if waiters.Len() == 0 {
				delete(m.waitersMap, key)
			}
		}
		m.mu.Unlock()
	case value = <-ready:
		ok = true
	}

	return
}
