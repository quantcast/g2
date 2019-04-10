package worker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	rt "github.com/quantcast/g2/pkg/runtime"
)

// The agent of job server.
type agent struct {
	sync.Mutex
	reconnectLock      sync.Mutex
	reconnectingActive bool
	conn               net.Conn
	connectionVersion  uint32
	rw                 *bufio.ReadWriter
	worker             *Worker
	in                 chan []byte
	net, addr          string
}

// Create the agent of job server.
func newAgent(net, addr string, worker *Worker) (a *agent, err error) {
	a = &agent{
		net:    net,
		addr:   addr,
		worker: worker,
		in:     make(chan []byte, rt.QueueSize),
	}
	return
}

func (a *agent) work() {
	log.Println("Starting agent Work For:", a.addr)
	defer func() {
		if err := recover(); err != nil {
			a.reconnect_error(err.(error))
		}
	}()

	var inpack *inPack
	var l int
	var err error
	var data, leftdata []byte
	startConnVersion := a.connectionVersion

	// exit the loop if connection has been replaced because reconnect will launch a new work() thread
	for startConnVersion == a.connectionVersion && !a.worker.isShuttingDown() {

		if data, err = a.read(); err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Temporary() {
					log.Println("opErr.Temporary():", a.addr)
					continue
				} else {
					log.Println("got permanent network error with server:", a.addr, "comm thread exiting.")
					a.reconnect_error(err)
					// else - we're probably dc'ing due to a Close()
					break
				}
			} else {
				log.Println("got error", err, "with server:", a.addr, "comm thread exiting...")
				a.reconnect_error(err)
				break
			}
		}
		if len(leftdata) > 0 { // some data left for processing
			data = append(leftdata, data...)
		}
		if len(data) < rt.MinPacketLength { // not enough data
			leftdata = data
			continue
		}
		for {
			if inpack, l, err = decodeInPack(data); err != nil {
				a.reconnect_error(err)
				break
			} else {
				leftdata = nil
				inpack.a = a
				a.worker.in <- inpack
				if len(data) == l {
					break
				}
				if len(data) > l {
					data = data[l:]
				}
			}
		}

	}
}

func (a *agent) reconnect_error(err error) {
	if a.conn != nil {
		err = &WorkerDisconnectError{
			err:   err,
			agent: a,
		}
		a.worker.err(err)
	}
	a.Connect()

}

func (a *agent) Close() {
	a.Lock()
	defer a.Unlock()
	if a.conn != nil {
		_ = a.conn.Close()
		a.conn = nil
	}
}

func (a *agent) Grab() (err error) {
	a.Lock()
	defer a.Unlock()
	return a.grab()
}

func (a *agent) grab() (err error) {
	outpack := getOutPack()
	outpack.dataType = rt.PT_GrabJobUniq
	return a.Write(outpack)
}

func (a *agent) PreSleep() (err error) {
	a.Lock()
	defer a.Unlock()
	outpack := getOutPack()
	outpack.dataType = rt.PT_PreSleep
	return a.Write(outpack)
}

func (a *agent) grabReconnectState() (success bool) {
	a.reconnectLock.Lock()
	defer a.reconnectLock.Unlock()
	if a.reconnectingActive { // another thread is already reconnecting to server, this thread will exit
		return false
	}
	a.reconnectingActive = true // I am first to attempt reconnection
	return true
}

// called by owner of reconnect state to tell that it has finished reconnecting
func (a *agent) resetReconnectState() {
	a.reconnectingActive = false
}

func (a *agent) Connect() {

	ownReconnect := a.grabReconnectState()
	a.Lock()
	defer a.Unlock()

	if !ownReconnect {
		//Reconnect collision, this thread will exit and wait on next a.Lock() for other to complete reconnection
		return
	}
	defer a.resetReconnectState() // before releasing client lock we will reset reconnection state

	log.Println("Trying to Connect to server:", a.addr, "...")

	var conn net.Conn
	var err error

	for !a.worker.isShuttingDown() {
		for num_tries := 1; !a.worker.isShuttingDown(); num_tries++ {

			if a.conn != nil {
				_ = a.conn.Close()
				a.conn = nil
			}

			// nil-out the rw pointer since it's no longer valid
			_ = atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&a.rw)), nil)

			if num_tries%100 == 0 {
				log.Printf("Still trying to connect to server %v, attempt# %v ...", a.addr, num_tries)
			}
			conn, err = net.Dial(a.net, a.addr)
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			break
		}

		if conn == nil {
			// in case worker is shutting down
			break
		}
		// at this point the server is back online, we will disconnect and reconnect again to make sure that we don't have
		// one of those dud connections which could happen if we've reconnected to gearman too quickly after it started
		_ = conn.Close()
		time.Sleep(3 * time.Second)

		conn, err = net.Dial(a.net, a.addr)
		if err != nil {
			// looks like there is another problem, go back to the main loop
			time.Sleep(time.Second)
			continue
		}

		if err = conn.(*net.TCPConn).SetKeepAlive(true); err != nil {
			log.Println("Error callng SetKeepAlive for %, ingoring...", a.addr)
		}

		a.conn = conn
		a.connectionVersion++

		log.Println("Successfully Connected to:", a.addr)

		newRw := bufio.NewReadWriter(bufio.NewReader(a.conn), bufio.NewWriter(a.conn))

		if swapped := atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&a.rw)),
			unsafe.Pointer(nil), unsafe.Pointer(newRw)); !swapped {
			log.Println("Was expecting nil when replacing with new ReadWriter")
		}

		if err := a.worker.reRegisterFuncsForAgent(a); err != nil {
			log.Println("Failed to register funcs for agent, error=%v", err)
			continue
		}

		if err := a.grab(); err != nil {
			log.Println("Failed to grab(), error=%v", err)
			continue
		}

		// only threads are a.work() and a.Work(),
		// a.work() is exited when connectionVersion is incremented
		// a.Work() does not exit because it uses an anonymous function to process writes
		go a.work()

		break
	}

	return
}

// read length bytes from the socket
func (a *agent) read() (data []byte, err error) {
	n := 0

	tmp := rt.NewBuffer(rt.BufferSize)
	var buf bytes.Buffer

	// read the header so we can get the length of the data
	if n, err = a.rw.Read(tmp); err != nil {
		return
	}
	dl := int(binary.BigEndian.Uint32(tmp[8:12]))

	// write what we read so far
	buf.Write(tmp[:n])

	// read until we receive all the data
	for buf.Len() < dl+rt.MinPacketLength {
		if n, err = a.rw.Read(tmp); err != nil {
			return buf.Bytes(), err
		}
		buf.Write(tmp[:n])
	}

	return buf.Bytes(), err
}

// Internal write the encoded job.
func (a *agent) Write(outpack *outPack) (err error) {

	if a.rw == nil {
		return errors.New("Reconnect is active, discarding the response")
	}

	var n int
	buf := outpack.Encode()
	for i := 0; i < len(buf); i += n {
		n, err = a.rw.Write(buf[i:])
		if err != nil {
			return err
		}
	}
	return a.rw.Flush()
}
