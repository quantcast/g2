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
	waitersMap map[string]list.List
}

type waiter struct {
	ready chan<- struct{} // Closed when semaphore acquired.
}

func NewHandlerMap() *HandlerMap {
	return &HandlerMap{sync.Mutex{},
		make(map[string]ResponseHandler, 100),
		make(map[string]list.List, 100),
	}
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

	// let's remember the current time
	curTime := time.Now()
	maxTime := curTime.Add(time.Duration(timeoutMs) * time.Millisecond)

	for time.Now().Before(maxTime) && !ok {
		value, ok = m.innerMap[key]
		if !ok {
			nsLeft := maxTime.Sub(time.Now()).Nanoseconds()
			ctx, _ := context.WithTimeout(context.Background(), time.Duration(nsLeft)*time.Nanosecond)

			waiters, wok := m.waitersMap[key]
			if !wok {
				waiters = list.List{}
				m.waitersMap[key] = waiters
			}
			ready := make(chan struct{})
			w := waiter{ready: ready}
			elem := waiters.PushBack(w)
			m.mu.Unlock() // unlock before we start waiting on stuff

			select {
			case <-ctx.Done():
				m.mu.Lock()
				select {
				case <-ready:
					// in case we got signalled during cancellation
					continue
				default:
					// we got timeout, let's remove
					waiters.Remove(elem)
				}
				m.mu.Unlock()
				return

			case <-ready:
				m.mu.Lock() // going back to the loop, gotta lock
				continue
			}
		}
	}

	m.mu.Unlock()
	return
}
