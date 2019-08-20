package client

import (
	"sync"
	"time"
)

type HandlerMap struct {
	innerMap sync.Map
	handlerMap   sync.Map
}

type HandHolder struct {
	once *sync.Once
	handlerChan chan ResponseHandler
	handler ResponseHandler
	innerMap sync.Map
}

func (handHolder *HandHolder) Get(timoeutMs int) ResponseHandler {
	handHolder.once.Do(func() {
		select {
		case handler := <-handHolder.handlerChan:
			handHolder.handler = handler
		case <- time.After( time.Duration( timoeutMs) * time.Millisecond):
		}})
	return handHolder.handler
}

func (handHolder *HandHolder) Put(handler ResponseHandler) {
	handHolder.handlerChan <- handler
}

func NewHandlerMap() *HandlerMap {
	return &HandlerMap{}
}

func (m *HandlerMap) OptimisticLoadOrStore(key string) *HandHolder {
	val, ok := m.handlerMap.Load(key)
	if !ok {
		val, _ = m.handlerMap.LoadOrStore(key, &HandHolder{once:new(sync.Once), handlerChan: make(chan ResponseHandler, 1)})
	}
	holder, _ := val.(*HandHolder)
	return holder
}

func (m *HandlerMap) Put(key string, responseHandler ResponseHandler) {
	holder := m.OptimisticLoadOrStore(key)
	holder.Put(responseHandler)
	holder.Get(1)
}

func (m *HandlerMap) Delete(key string) {
	m.handlerMap.Delete(key)
}

func (m *HandlerMap) Get(key string, timeoutMs int) (value ResponseHandler, ok bool) {
	holder := m.OptimisticLoadOrStore(key)
	handler := holder.Get(timeoutMs)
	return handler, handler != nil
}
