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

	"github.com/appscode/go/log"
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
}

// One client connect to one server.
// Use Pool for multi-connections.
type Client struct {
	net, addr string
	handlers  sync.Map
	expected  chan *Response
	outbound  chan *request
	conn      *connection
	//rw        *bufio.ReadWriter

	responsePool *sync.Pool
	requestPool  *sync.Pool

	ResponseTimeout time.Duration

	ErrorHandler ErrorHandler
}

// Return a client.
func New(network, addr string) (client *Client, err error) {
	conn, err := net.Dial(network, addr)

	if err != nil {
		return
	}

	conn.(*net.TCPConn).SetKeepAlive(true)

	client = NewConnected(conn)

	return
}

// Return a new client from an established connection. Largely used for
// testing, though other use-cases can be imagined.
func NewConnected(conn net.Conn) (client *Client) {
	addr := conn.RemoteAddr()

	client = &Client{
		net:             addr.Network(),
		addr:            addr.String(),
		conn:            &connection{conn},
		outbound:        make(chan *request),
		expected:        make(chan *Response),
		ResponseTimeout: DefaultTimeout,
		responsePool:    &sync.Pool{New: func() interface{} { return &Response{} }},
		requestPool:     &sync.Pool{New: func() interface{} { return &request{} }},
	}

	//kaConn, _ := tcpkeepalive.EnableKeepAlive(client.conn.Conn)
	//kaConn.SetKeepAliveIdle(1000*time.Second)
	//kaConn.SetKeepAliveCount(4)
	//kaConn.SetKeepAliveInterval(10*time.Second)
	//
	//client.conn.Conn = kaConn



	go client.readLoop()
	go client.writeLoop()

	return
}

func (client *Client) writeLoop() {
	ibuf := make([]byte, 4)
	length := uint32(0)
	var i int

	// Pipeline requests; but only write them one at a time. To allow multiple
	// goroutines to all write as quickly as possible, uses a channel and the
	// writeLoop lives in a separate goroutine.
	for req := range client.outbound {
		client.conn.Write([]byte(rt.ReqStr))

		// todo handle errors.

		binary.BigEndian.PutUint32(ibuf, req.pt.Uint32())

		client.conn.Write(ibuf)

		length = 0

		for _, chunk := range req.data {
			length += uint32(len(chunk))
		}

		// nil separators
		length += uint32(len(req.data)) - 1

		binary.BigEndian.PutUint32(ibuf, length)

		client.conn.Write(ibuf)

		client.conn.Write(req.data[0])

		for i = 1; i < len(req.data); i++ {
			client.conn.Write(NullBytes)
			client.conn.Write(req.data[i])
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

func (client *Client) reconnect(err error) error {
	if client.conn != nil {
		return nil
	}

	// TODO I doubt this error handling is right because it looks
	// really complicated.
	if opErr, ok := err.(*net.OpError); ok {
		if opErr.Timeout() {
			client.err(err)
		}
		if opErr.Temporary() {
			return nil
		}

		return err
	}

	if err != nil {
		client.err(err)
	}

	// If it is unexpected error and the connection wasn't
	// closed by Gearmand, the client should close the conection
	// and reconnect to job server.
	client.Close()

	conn, err := net.Dial(client.net, client.addr)


	if err != nil {
		client.err(err)
		return err
	}

	//kaConn, _ := tcpkeepalive.EnableKeepAlive(conn)
	//kaConn.SetKeepAliveIdle(1000*time.Second)
	//kaConn.SetKeepAliveCount(4)
	//kaConn.SetKeepAliveInterval(10*time.Second)
	//
	//client.conn.Conn = kaConn

	swapped := atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&client.conn)),
		unsafe.Pointer(nil),
		unsafe.Pointer(&connection{conn}))

	if !swapped {
		conn.Close()
	}

	return nil
}

func (client *Client) readLoop() {
	header := make([]byte, rt.HeaderSize)

	var err error
	var resp *Response

	for client.conn != nil {
		if _, err = io.ReadFull(client.conn, header); err != nil {
			if err = client.reconnect(err); err != nil {
				break
			}

			continue
		}

		_, pt, length := decodeHeader(header)

		contents := make([]byte, length)

		if _, err = io.ReadFull(client.conn, contents); err != nil {
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
	// NOTE Any waiting goroutine which reads from `expected` should return the
	// response object to the pool; but the conditions which handle it
	// terminally should return it here.
	switch resp.DataType {
	case rt.PT_Error:
		log.Errorln("Received error", resp.Data)

		client.err(getError(resp.Data))

		client.expected <- resp

	case rt.PT_StatusRes, rt.PT_JobCreated, rt.PT_EchoRes:
		client.expected <- resp
	case rt.PT_WorkComplete, rt.PT_WorkFail, rt.PT_WorkException:
		defer client.handlers.Delete(resp.Handle)
		fallthrough
	case rt.PT_WorkData, rt.PT_WorkWarning, rt.PT_WorkStatus:
		// These alternate conditions should not happen so long as
		// everyone is following the specification.
		if handler, ok := client.handlers.Load(resp.Handle); ok {
			if h, ok := handler.(ResponseHandler); ok {
				h(resp)
			}
		} else {
			client.err(fmt.Errorf("unexpected %s response for \"%s\" with no handler", resp.DataType, resp.Handle))
		}

		client.responsePool.Put(resp)
	}
}

func (client *Client) err(e error) {
	if client.ErrorHandler != nil {
		client.ErrorHandler(e)
	}
}

func (client *Client) request() *request {
	return client.requestPool.Get().(*request)
}

func (client *Client) submit(pt rt.PT, funcname string, payload []byte) (string, error) {
	var err error

	client.outbound <- client.request().submitJob(pt, funcname, IdGen.Id(), payload)

	res := <-client.expected

	if res.DataType == rt.PT_Error {
		err = getError(res.Data)
	}

	defer client.responsePool.Put(res)

	return res.Handle, err
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
	if err = client.reconnect(nil); err != nil {
		return
	}

	client.outbound <- client.request().status(handle)

	res := <-client.expected

	status, err = res.Status()

	client.responsePool.Put(res)

	return
}

// Echo.
func (client *Client) Echo(data []byte) (echo []byte, err error) {
	if err = client.reconnect(nil); err != nil {
		return
	}

	client.outbound <- client.request().echo(data)

	res := <-client.expected

	echo = res.Data

	client.responsePool.Put(res)

	return
}

// Close connection
func (client *Client) Close() (err error) {
	ptr := atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&client.conn)), nil)

	conn := (*connection)(ptr)

	if conn != nil {
		err = conn.Close()

		return
	}

	return fmt.Errorf("client disconnected")
}
