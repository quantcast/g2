// The client package helps developers connect to Gearmand, send
// jobs and fetch result.
package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	rt "github.com/quantcast/g2/pkg/runtime"
)

var (
	DefaultTimeout time.Duration = time.Second
	Null                         = byte('\x00')
	NullBytes                    = []byte{Null}
)

type connection struct {
	// This simple wrapper struct is just a convenient way to deal with the
	// fact that accessing the underlying pointer for an interface value type
	// is not reasonably possible in Go. Using a pointer to a struct makes use
	// of `atomic.SwapPointer` and `atomic.CompareAndSwapPointer` much more
	// convenient.
	net.Conn
	connVersion int
}

type chanStruct struct {
	outbound chan *request
	expected chan *Response
}

type ConnCloseHandler func(conn net.Conn) (err error)
type ConnOpenHandler func() (conn net.Conn, err error)

// One client connect to one server.
// Use Pool for multi-connections.
type Client struct {
	sync.Mutex
	reconnectLock      sync.Mutex
	reconnectingActive bool
	net, addr          string
	handlers           sync.Map
	conn               *connection
	//rw        *bufio.ReadWriter
	channels *chanStruct

	responsePool *sync.Pool
	requestPool  *sync.Pool

	ResponseTimeout time.Duration

	ErrorHandler     ErrorHandler
	connCloseHandler ConnCloseHandler
	connOpenHandler  ConnOpenHandler
	logHandler       LogHandler
}

type LogLevel int

const (
	Error   LogLevel = 0
	Warning LogLevel = 1
	Info    LogLevel = 2
	Debug   LogLevel = 3
)

type LogHandler func(level LogLevel, message ...string)

func (client *Client) Log(level LogLevel, message ...string) {
	if client.logHandler != nil {
		client.logHandler(level, message...)
	}
}

// Return a client.
func NewNetClient(network, addr string, logHandler LogHandler) (client *Client, err error) {

	connOpenHandler := func() (conn net.Conn, err error) {
		if logHandler != nil {
			logHandler(Info, fmt.Sprintf("Trying to connect to server %v ...", addr))
		}
		for {
			for num_tries := 1; ; num_tries++ {
				if num_tries%100 == 0 && logHandler != nil {
					logHandler(Info, fmt.Sprintf("Still trying to connect to server %v, attempt# %v ...", addr, num_tries))
				}
				conn, err = net.Dial(network, addr)
				if err != nil {
					time.Sleep(time.Second)
					continue
				}
				break
			}
			// at this point the server is back online, we will disconnect and reconnect again to make sure that we don't have
			// one of those dud connections which could happen if we've reconnected to gearman too quickly after it started
			_ = conn.Close()
			time.Sleep(3 * time.Second)

			// todo: come up with a more reliable way to determine if we have a working connection to gearman, pehaps by performing a test
			conn, err = net.Dial(network, addr)
			if err != nil {
				// looks like there is another problem, go back to the main loop
				time.Sleep(time.Second)
				continue
			}
			conn.(*net.TCPConn).SetKeepAlive(true)

			break
		}
		if logHandler != nil {
			logHandler(Info, fmt.Sprintf("Connected to server %v", addr))
		}

		return
	}

	client, err = NewClient(nil, connOpenHandler, logHandler)

	return
}

/// handler_conn_close: optional
func NewClient(connCloseHandler ConnCloseHandler,
	connOpenHandler ConnOpenHandler,
	logHandler LogHandler) (client *Client, err error) {

	conn, err := connOpenHandler()
	if err != nil {
		// if we're emitting errors we wont log them, they can be logged by the codebase that's using this client
		err = errors.New(fmt.Sprintf("Failed to create new client: %v", err))
		return
	}

	addr := conn.RemoteAddr()

	client = &Client{
		net:              addr.Network(),
		addr:             addr.String(),
		conn:             &connection{Conn: conn},
		channels:         &chanStruct{expected: make(chan *Response), outbound: make(chan *request)},
		ResponseTimeout:  DefaultTimeout,
		responsePool:     &sync.Pool{New: func() interface{} { return &Response{} }},
		requestPool:      &sync.Pool{New: func() interface{} { return &request{} }},
		connCloseHandler: connCloseHandler,
		connOpenHandler:  connOpenHandler,
		logHandler:       logHandler,
	}

	go client.readLoop()
	go client.writeLoop()

	return
}

func (client *Client) IsConnectionSet() bool {
	return client.conn != nil
}

func (client *Client) getConn() *connection {
	client.Lock()
	defer client.Unlock()
	return client.conn
}

func (client *Client) writeReconnectCleanup(cleanupHandle func(), ibufs ...[]byte) error {
	for _, ibuf := range ibufs {
		if _, err := client.getConn().Write(ibuf); err != nil {
			cleanupHandle()
			go client.reconnect(err)
			return err // return non-nil will cause writeLoop to exit, it will be restarted upon successful reconnect
		}
	}
	return nil
}

func (client *Client) writeLoop() {
	ibuf := make([]byte, 4)
	length := uint32(0)
	var i int

	// Pipeline requests; but only write them one at a time. To allow multiple
	// goroutines to all write as quickly as possible, uses a channel and the
	// writeLoop lives in a separate goroutine.
	for req := range client.channels.outbound {

		cleanupHandle := func() {
			client.requestPool.Put(req)
		}

		if err := client.writeReconnectCleanup(cleanupHandle, []byte(rt.ReqStr)); err != nil {
			return
		}

		binary.BigEndian.PutUint32(ibuf, req.pt.Uint32())

		if err := client.writeReconnectCleanup(cleanupHandle, ibuf); err != nil {
			return
		}

		length = 0

		for _, chunk := range req.data {
			length += uint32(len(chunk))
		}

		// nil separators
		length += uint32(len(req.data)) - 1

		binary.BigEndian.PutUint32(ibuf, length)

		if err := client.writeReconnectCleanup(cleanupHandle, ibuf, req.data[0]); err != nil {
			return
		}

		for i = 1; i < len(req.data); i++ {
			if err := client.writeReconnectCleanup(cleanupHandle, NullBytes, req.data[i]); err != nil {
				return
			}
		}

		cleanupHandle()
	}
}

func decodeHeader(header []byte) (code []byte, pt uint32, length int) {
	code = header[0:4]
	pt = binary.BigEndian.Uint32(header[4:8])
	length = int(binary.BigEndian.Uint32(header[8:12]))

	return
}

func (client *Client) grabReconnectState() (success bool) {
	client.reconnectLock.Lock()
	defer client.reconnectLock.Unlock()
	if client.reconnectingActive { // another thread is already reconnecting to server, this thread will exit
		return false
	}
	client.reconnectingActive = true // I am first to attempt reconnection
	return true
}

// called by owner of reconnect state to tell that it has finished reconnecting
func (client *Client) resetReconnectState() {
	client.reconnectingActive = false
}

func (client *Client) reconnect(err error) error {

	// not actioning on error if it's deemed Temporary
	// we might want to take note of timestamp and eventually recycle this connection
	// if it persists too long (even though classified as Temporary here)
	if opErr, ok := err.(*net.OpError); ok {
		if opErr.Temporary() {
			return nil
		}
	}

	ownReconnect := client.grabReconnectState()

	client.Lock()
	defer client.Unlock()

	if !ownReconnect {
		//Reconnect collision, this thread will exit and wait on next client.Lock() for other to complete reconnection
		return nil
	}

	defer client.resetReconnectState() // before releasing client lock we will reset reconnection state

	connVersion := client.conn.connVersion
	close(client.channels.expected)
	close(client.channels.outbound)

	client.Log(Error, fmt.Sprintf("Closing connection to %v due to error %v, will reconnect...", client.addr, err))
	if close_err := client.Close(); close_err != nil {
		client.Log(Warning, fmt.Sprintf("Non-fatal error %v, while closing connection to %v", close_err, client.addr))
	}

	conn, err := client.connOpenHandler()
	if err != nil {
		client.err(err)
		return err
	}

	newConn := &connection{conn, connVersion + 1}

	if swapped := atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&client.conn)),
		unsafe.Pointer(nil), unsafe.Pointer(newConn)); !swapped {
		return errors.New("Was expecting nil when replacing with new connection")
	}

	_ = atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&client.channels)),
		unsafe.Pointer(&chanStruct{
			expected: make(chan *Response),
			outbound: make(chan *request)}))

	// writeLoop() will be dead because the channel has been closed, it can be restarted now
	go client.writeLoop()

	return nil
}

func (client *Client) readLoop() {
	header := make([]byte, rt.HeaderSize)

	var err error
	var resp *Response

	for client.conn != nil {

		if _, err = io.ReadFull(client.getConn(), header); err != nil {
			if err = client.reconnect(err); err != nil {
				break
			}

			continue
		}

		_, pt, length := decodeHeader(header)

		contents := make([]byte, length)

		if _, err = io.ReadFull(client.getConn(), contents); err != nil {
			if err = client.reconnect(err); err != nil {
				break
			}

			continue
		}

		resp = client.responsePool.Get().(*Response)

		resp.DataType, err = rt.NewPT(pt)

		if err != nil {
			client.err(err)

			client.responsePool.Put(resp)

			continue
		}

		switch resp.DataType {
		case rt.PT_JobCreated:
			resp.Handle = string(contents)

		case rt.PT_StatusRes, rt.PT_WorkData, rt.PT_WorkWarning, rt.PT_WorkStatus,
			rt.PT_WorkComplete, rt.PT_WorkException:

			sl := bytes.IndexByte(contents, Null)

			resp.Handle = string(contents[:sl])
			resp.Data = contents[sl+1:]

		case rt.PT_WorkFail:
			resp.Handle = string(contents)

		case rt.PT_EchoRes:
			fallthrough

		default:
			resp.Data = contents
		}

		client.process(resp)
	}
}

func (client *Client) process(resp *Response) {
	// NOTE Any waiting goroutine which reads from `channels` should return the
	// response object to the pool; but the conditions which handle it
	// terminally should return it here.
	switch resp.DataType {
	case rt.PT_Error:

		client.err(getError(resp.Data))

		client.channels.expected <- resp

	case rt.PT_StatusRes, rt.PT_JobCreated, rt.PT_EchoRes:
		client.channels.expected <- resp
	case rt.PT_WorkComplete, rt.PT_WorkFail, rt.PT_WorkException:
		defer client.handlers.Delete(resp.Handle)
		fallthrough
	case rt.PT_WorkData, rt.PT_WorkWarning, rt.PT_WorkStatus:
		// These alternate conditions should not happen so long as
		// everyone is following the specification.
		if handler, ok := client.handlers.Load(resp.Handle); ok {
			if h, ok := handler.(ResponseHandler); ok {
				h(resp)
			} else {
				client.err(errors.New(fmt.Sprintf("Could not cast handler to ResponseHandler for %v", resp.Handle)))
			}
		} else {
			client.err(errors.New(fmt.Sprintf("unexpected %s response for \"%s\" with no handler", resp.DataType, resp.Handle)))
		}

		client.responsePool.Put(resp)
	}
}

func (client *Client) err(e error) {
	if client.ErrorHandler != nil {
		client.ErrorHandler(e)
	} else {
		client.Log(Error, e.Error()) // in case ErrorHandler is not supplied we try the Log, this might be important
	}
}

func (client *Client) request() *request {
	return client.requestPool.Get().(*request)
}

func (client *Client) submit(pt rt.PT, funcname string, payload []byte) (handle string, err error) {

	defer func() {
		if e := recover(); e != nil {
			handle = ""
			err = e.(error)
		}
	}()

	client.channels.outbound <- client.request().submitJob(pt, funcname, IdGen.Id(), payload)

	if res := <-client.channels.expected; res != nil {
		if res.DataType == rt.PT_Error {
			err = getError(res.Data)
		}
		defer client.responsePool.Put(res)
		return res.Handle, err
	}

	return "", errors.New("Got empty client.channels queue, please resubmit your message")
}

// Call the function and get a response.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) Do(funcname string, payload []byte,
	flag byte, h ResponseHandler) (handle string, err error) {
	var pt rt.PT

	switch flag {
	case rt.JobLow:
		pt = rt.PT_SubmitJobLow
	case rt.JobHigh:
		pt = rt.PT_SubmitJobHigh
	default:
		pt = rt.PT_SubmitJob
	}

	handle, err = client.submit(pt, funcname, payload)

	client.handlers.Store(handle, h)

	return
}

// Call the function in background, no response needed.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoBg(funcname string, payload []byte, flag byte) (handle string, err error) {
	var pt rt.PT
	switch flag {
	case rt.JobLow:
		pt = rt.PT_SubmitJobLowBG
	case rt.JobHigh:
		pt = rt.PT_SubmitJobHighBG
	default:
		pt = rt.PT_SubmitJobBG
	}

	handle, err = client.submit(pt, funcname, payload)

	return
}

func (client *Client) DoCron(funcname string, cronExpr string, funcParam []byte) (string, error) {
	cf := strings.Fields(cronExpr)
	expLen := len(cf)
	switch expLen {
	case 5:
		return client.doCron(funcname, cronExpr, funcParam)
	case 6:
		if cf[5] == "*" {
			return client.doCron(funcname, strings.Join([]string{cf[0], cf[1], cf[2], cf[3], cf[4]}, " "), funcParam)
		} else {
			epoch, err := ToEpoch(strings.Join([]string{cf[0], cf[1], cf[2], cf[3], cf[5]}, " "))
			if err != nil {
				return "", err
			}
			return client.DoAt(funcname, epoch, funcParam)
		}
	default:
		return "", errors.New("invalid cron expression")

	}
}

func (client *Client) doCron(funcname string, cronExpr string, funcParam []byte) (handle string, err error) {
	ce, err := rt.NewCronSchedule(cronExpr)
	if err != nil {
		return "", err
	}
	dbyt := []byte(fmt.Sprintf("%v%v", string(ce.Bytes()), string(funcParam)))

	handle, err = client.submit(rt.PT_SubmitJobSched, funcname, dbyt)

	return
}

func (client *Client) DoAt(funcname string, epoch int64, funcParam []byte) (handle string, err error) {
	if client.conn == nil {
		return "", ErrLostConn
	}

	dbyt := []byte(fmt.Sprintf("%v\x00%v", epoch, string(funcParam)))

	handle, err = client.submit(rt.PT_SubmitJobEpoch, funcname, dbyt)

	return
}

// Get job status from job server.
func (client *Client) Status(handle string) (status *Status, err error) {

	defer func() {
		if e := recover(); e != nil {
			status = nil
			err = e.(error)
		}
	}()

	client.channels.outbound <- client.request().status(handle)

	res := <-client.channels.expected

	if res == nil {
		return nil, errors.New("Status response queue is empty, please resend")
	}
	status, err = res.Status()

	client.responsePool.Put(res)
	return status, nil
}

// Echo.
func (client *Client) Echo(data []byte) (echo []byte, err error) {

	defer func() {
		if e := recover(); e != nil {
			echo = nil
			err = e.(error)
		}
	}()

	client.channels.outbound <- client.request().echo(data)

	res := <-client.channels.expected

	if res == nil {
		return nil, errors.New("Echo request got empty response, please resend")
	}

	echo = res.Data

	client.responsePool.Put(res)

	return
}

// Close connection
func (client *Client) Close() (err error) {
	ptr := atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&client.conn)), nil)

	conn := (*connection)(ptr)

	if conn != nil {
		if client.connCloseHandler != nil {
			err = client.connCloseHandler(conn)
		} else {
			err = conn.Close()
		}
		return
	}

	return fmt.Errorf("client disconnected")
}
