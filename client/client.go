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

const (
	WORK_HANDLE_DELAY_MS = 5 // milliseconds delay for re-try processing of work completion requests if handler hasn't been yet stored in hash map.
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

type channels struct {
	outbound chan *request
	expected chan *Response
}

type ConnCloseHandler func(conn net.Conn) (err error)
type ConnOpenHandler func() (conn net.Conn, err error)

// One client connect to one server.
// Use Pool for multi-connections.
type Client struct {
	reconnectState uint32
	net, addr      string
	handlers       sync.Map
	conn           *connection
	//rw        *bufio.ReadWriter
	chans        *channels
	responsePool *sync.Pool
	requestPool  *sync.Pool

	ResponseTimeout time.Duration

	ErrorHandler     ErrorHandler
	connCloseHandler ConnCloseHandler
	connOpenHandler  ConnOpenHandler
	logHandler       LogHandler
	submitLock       sync.Mutex
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

func NewConnected(conn net.Conn) (client *Client) {

	existingConnection := &connection{conn, 0}

	connOpenHandler := func() (conn net.Conn, err error) {
		if existingConnection != nil {
			conn = existingConnection.Conn
			existingConnection = nil
		} else {
			err = errors.New("Connection supplied to NewConnected() failed")
		}
		return
	}

	client, _ = NewClient(nil, connOpenHandler, nil)

	return client
}

// Return a client.
func New(network, addr string, logHandler LogHandler) (client *Client, err error) {

	if logHandler == nil {
		logHandler = func(level LogLevel, message ...string) {}
	}

	retryPeriod := 3 * time.Second

	connOpenHandler := func() (conn net.Conn, err error) {
		logHandler(Info, fmt.Sprintf("Trying to connect to server %v ...", addr))
		for {
			for numTries := 1; ; numTries++ {
				if numTries%100 == 0 {
					logHandler(Info, fmt.Sprintf("Still trying to connect to server %v, attempt# %v ...", addr, numTries))
				}
				conn, err = net.Dial(network, addr)
				if err != nil {
					time.Sleep(retryPeriod)
					continue
				}
				break
			}
			// at this point the server is back online, we will disconnect and reconnect again to make sure that we don't have
			// one of those dud connections which could happen if we've reconnected to gearman too quickly after it started
			_ = conn.Close()
			time.Sleep(retryPeriod)

			// todo: come up with a more reliable way to determine if we have a working connection to gearman, pehaps by performing a test
			conn, err = net.Dial(network, addr)
			if err != nil {
				// looks like there is another problem, go back to the main loop
				time.Sleep(retryPeriod)
				continue
			}

			break
		}
		logHandler(Info, fmt.Sprintf("Connected to server %v", addr))

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
		net:  addr.Network(),
		addr: addr.String(),
		conn: &connection{Conn: conn},
		chans: &channels{
			expected: make(chan *Response),
			outbound: make(chan *request)},
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
	return client.loadConn() != nil
}

func (client *Client) writeReconnectCleanup(conn *connection, req *request, ibufs ...[]byte) bool {
	for _, ibuf := range ibufs {
		if _, err := conn.Write(ibuf); err != nil {
			client.requestPool.Put(req)
			go client.reconnect(err)
			return true // return true will cause writeLoop to exit, it will be restarted upon successful reconnect
		}
	}
	return false
}

func (client *Client) loadChans() *channels {
	return (*channels)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&client.chans))))
}

func (client *Client) writeLoop() {
	ibuf := make([]byte, 4)
	length := uint32(0)
	var i int
	chans := client.loadChans()

	// Pipeline requests; but only write them one at a time. To allow multiple
	// goroutines to all write as quickly as possible, uses a channel and the
	// writeLoop lives in a separate goroutine.
	for req := range chans.outbound {

		conn := client.loadConn()
		if conn == nil {
			client.requestPool.Put(req)
			return
		}

		if exit := client.writeReconnectCleanup(conn, req, []byte(rt.ReqStr)); exit {
			return
		}

		binary.BigEndian.PutUint32(ibuf, req.pt.Uint32())

		if exit := client.writeReconnectCleanup(conn, req, ibuf); exit {
			return
		}

		length = 0

		for _, chunk := range req.data {
			length += uint32(len(chunk))
		}

		// nil separators
		length += uint32(len(req.data)) - 1

		binary.BigEndian.PutUint32(ibuf, length)

		if client.writeReconnectCleanup(conn, req, ibuf, req.data[0]) {
			return
		}

		for i = 1; i < len(req.data); i++ {
			if exit := client.writeReconnectCleanup(conn, req, NullBytes, req.data[i]); exit {
				return
			}
		}

		client.requestPool.Put(req)
	}
}

func decodeHeader(header []byte) (code []byte, pt uint32, length int) {
	code = header[0:4]
	pt = binary.BigEndian.Uint32(header[4:8])
	length = int(binary.BigEndian.Uint32(header[8:12]))

	return
}

func (client *Client) lockReconnect() (success bool) {
	return atomic.CompareAndSwapUint32(&client.reconnectState, 0, 1)
}

// called by owner of reconnect state to tell that it has finished reconnecting
func (client *Client) resetReconnectState() {
	atomic.StoreUint32(&client.reconnectState, 0)
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

	ownReconnect := client.lockReconnect()

	if !ownReconnect {
		//Reconnect collision, this thread will exit and wait on next client.Lock() for other to complete reconnection
		return nil
	}

	defer client.resetReconnectState() // before releasing client lock we will reset reconnection state

	connVersion := client.loadConn().connVersion

	client.Log(Error, fmt.Sprintf("Closing connection to %v due to error %v, will reconnect...", client.addr, err))
	if closeErr := client.Close(); closeErr != nil {
		client.Log(Warning, fmt.Sprintf("Non-fatal error %v, while closing connection to %v", closeErr, client.addr))
	}

	oldChans := client.loadChans()
	close(oldChans.expected)
	close(oldChans.outbound)

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

	// replace closed channels with new ones
	_ = (*channels)(atomic.SwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&client.chans)),
		unsafe.Pointer(&channels{expected: make(chan *Response), outbound: make(chan *request)})))

	go client.readLoop()
	go client.writeLoop()

	return nil
}

func (client *Client) loadConn() *connection {
	return (*connection)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&client.conn))))
}

func (client *Client) readReconnect(startConn *connection, buf []byte) (n int, exit bool) {
	conn := client.loadConn()
	if startConn != conn {
		return 0, true
	}
	var err error
	if n, err = io.ReadFull(conn, buf); err != nil {
		go client.reconnect(err)
		return n, true
	} else {
		return n, false
	}
}

func (client *Client) readLoop() {
	header := make([]byte, rt.HeaderSize)

	var err error
	var resp *Response
	startConn := client.loadConn()
	newConn := startConn
	if startConn == nil {
		return
	}

	for ; newConn == startConn; newConn = client.loadConn() {

		if _, exit := client.readReconnect(startConn, header); exit {
			return
		}

		_, pt, length := decodeHeader(header)

		contents := make([]byte, length)

		if _, exit := client.readReconnect(startConn, contents); exit {
			return
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

		client.loadChans().expected <- resp

	case rt.PT_StatusRes, rt.PT_JobCreated, rt.PT_EchoRes:
		client.loadChans().expected <- resp
	case rt.PT_WorkComplete, rt.PT_WorkFail, rt.PT_WorkException:
		defer client.handlers.Delete(resp.Handle)
		fallthrough
	case rt.PT_WorkData, rt.PT_WorkWarning, rt.PT_WorkStatus:
		// These alternate conditions should not happen so long as
		// everyone is following the specification.
		var handler interface{}
		var ok bool
		if handler, ok = client.handlers.Load(resp.Handle); !ok {
			// possibly the response arrived faster than the job handler was added to client.handlers, we'll wait a bit and give it another try
			time.Sleep(WORK_HANDLE_DELAY_MS * time.Millisecond)
			if handler, ok = client.handlers.Load(resp.Handle); !ok {
				client.err(errors.New(fmt.Sprintf("unexpected %s response for \"%s\" with no handler", resp.DataType, resp.Handle)))
			}
		}
		if ok {
			if h, ok := handler.(ResponseHandler); ok {
				h(resp)
			} else {
				client.err(errors.New(fmt.Sprintf("Could not cast handler to ResponseHandler for %v", resp.Handle)))
			}
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

	client.submitLock.Lock()
	defer client.submitLock.Unlock()

	defer func() {
		if e := safeCastError(recover(), "panic in submit()"); e != nil {
			err = e
		}
	}()

	chans := client.loadChans()
	chans.outbound <- client.request().submitJob(pt, funcname, IdGen.Id(), payload)

	if res := <-chans.expected; res != nil {
		var err error
		if res.DataType == rt.PT_Error {
			err = getError(res.Data)
		}
		defer client.responsePool.Put(res)
		return res.Handle, err
	}

	return "", errors.New("Channels are closed, please resubmit your message")
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
	if client.loadConn() == nil {
		return "", ErrLostConn
	}

	dbyt := []byte(fmt.Sprintf("%v\x00%v", epoch, string(funcParam)))

	handle, err = client.submit(rt.PT_SubmitJobEpoch, funcname, dbyt)

	return
}

// Get job status from job server.
func (client *Client) Status(handle string) (status *Status, err error) {

	defer func() {
		if e := safeCastError(recover(), "panic in Status"); e != nil {
			err = e
		}
	}()

	chans := client.loadChans()
	chans.outbound <- client.request().status(handle)

	res := <-chans.expected

	if res == nil {
		return nil, errors.New("Status response queue is empty, please resend")
	}
	status, err = res.Status()

	client.responsePool.Put(res)

	return
}

// Echo.
func (client *Client) Echo(data []byte) (echo []byte, err error) {

	defer func() {
		if e := safeCastError(recover(), "panic in Echo"); e != nil {
			err = e
		}
	}()

	chans := client.loadChans()
	chans.outbound <- client.request().echo(data)

	res := <-chans.expected

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
