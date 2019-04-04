package worker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"sync"
	"time"

	rt "github.com/quantcast/g2/pkg/runtime"
)

// The agent of job server.
type agent struct {
	sync.Mutex
	conn      net.Conn
	rw        *bufio.ReadWriter
	worker    *Worker
	in        chan []byte
	net, addr string
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
			a.worker.err(err.(error))
		}
	}()

	var inpack *inPack
	var l int
	var err error
	var data, leftdata []byte
	for {
		if !a.worker.isShuttingDown() {
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
					a.worker.err(err)
					leftdata = data
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
}

func (a *agent) reconnect_error(err error) {
	if a.conn != nil {
		err = &WorkerDisconnectError{
			err:   err,
			agent: a,
		}
		a.worker.err(err)
	}
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
	return a.write(outpack)
}

func (a *agent) PreSleep() (err error) {
	a.Lock()
	defer a.Unlock()
	outpack := getOutPack()
	outpack.dataType = rt.PT_PreSleep
	return a.write(outpack)
}

func (a *agent) Connect() error {
	a.Lock()
	defer a.Unlock()
	log.Println("Trying to Connect to server:", a.addr, "...")
	for num_tries := 1; !a.worker.isShuttingDown(); num_tries++ {

		if a.conn != nil {
			_ = a.conn.Close()
			a.conn = nil
		}

		conn, err := net.Dial(a.net, a.addr)
		if err != nil {
			if num_tries%100 == 0 {
				log.Println("Attempt#", num_tries, "Still trying to Connect to ", a.addr)
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		log.Println("Successfully Connected to:", a.addr, "attempt#", num_tries)
		a.conn = conn
		a.rw = bufio.NewReadWriter(bufio.NewReader(a.conn),
			bufio.NewWriter(a.conn))

		if err := a.worker.reRegisterFuncsForAgent(a); err != nil {
			continue
		}

		if err := a.grab(); err != nil {
			continue
		}

		go a.work()
		break
	}

	return nil
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
func (a *agent) write(outpack *outPack) (err error) {
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

// Write with lock
func (a *agent) Write(outpack *outPack) (err error) {
	a.Lock()
	defer a.Unlock()
	return a.write(outpack)
}
