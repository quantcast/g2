// The client package helps developers connect to Gearmand, send
// jobs and fetch result.
package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/appscode/go/log"
	rt "github.com/ssmccoy/g2/pkg/runtime"
)

var (
	DefaultTimeout time.Duration = time.Second
	Null                         = byte('\x00')
	NullBytes                    = []byte{Null}
)

// One client connect to one server.
// Use Pool for multi-connections.
type Client struct {
	sync.Mutex

	net, addr string
	handlers  sync.Map
	expected  chan *Response
	outbound  chan *request
	conn      net.Conn
	rw        *bufio.ReadWriter

	responsePool *sync.Pool
	requestPool  *sync.Pool

	ResponseTimeout time.Duration

	ErrorHandler ErrorHandler
}

type responseHandlerMap struct {
	sync.RWMutex
	holder map[string]ResponseHandler
}

func newResponseHandlerMap() *responseHandlerMap {
	return &responseHandlerMap{holder: make(map[string]ResponseHandler, int(rt.QueueSize))}
}

func (r *responseHandlerMap) remove(key string) {
	r.Lock()
	delete(r.holder, key)
	r.Unlock()
}

func (r *responseHandlerMap) get(key string) (ResponseHandler, bool) {
	r.RLock()
	rh, b := r.holder[key]
	r.RUnlock()
	return rh, b
}
func (r *responseHandlerMap) put(key string, rh ResponseHandler) {
	r.Lock()
	r.holder[key] = rh
	r.Unlock()
}

// Return a client.
func New(network, addr string) (client *Client, err error) {
	client = &Client{
		net:             network,
		addr:            addr,
		outbound:        make(chan *request),
		expected:        make(chan *Response),
		ResponseTimeout: DefaultTimeout,
		responsePool:    &sync.Pool{New: func() interface{} { return &Response{} }},
		requestPool:     &sync.Pool{New: func() interface{} { return &request{} }},
	}

	client.conn, err = net.Dial(client.net, client.addr)

	if err != nil {
		return
	}

	client.rw = bufio.NewReadWriter(bufio.NewReader(client.conn),
		bufio.NewWriter(client.conn))

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
		client.rw.Write([]byte(rt.ReqStr))

		binary.BigEndian.PutUint32(ibuf, req.pt.Uint32())

		client.rw.Write(ibuf)

		length = 0

		for _, chunk := range req.data {
			length += uint32(len(chunk))
		}

		// nil separators
		length += uint32(len(req.data)) - 1

		binary.BigEndian.PutUint32(ibuf, length)

		client.rw.Write(ibuf)

		client.rw.Write(req.data[0])

		for i = 1; i < len(req.data); i++ {
			client.rw.Write(NullBytes)
			client.rw.Write(req.data[i])
		}

		client.requestPool.Put(req)

		client.rw.Flush()
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
	client.conn, err = net.Dial(client.net, client.addr)

	if err != nil {
		client.err(err)
		return err
	}

	client.rw = bufio.NewReadWriter(bufio.NewReader(client.conn),
		bufio.NewWriter(client.conn))

	return nil
}

func (client *Client) readLoop() {
	header := make([]byte, rt.HeaderSize)

	var err error
	var resp *Response

	for client.conn != nil {
		if _, err = io.ReadFull(client.rw, header); err != nil {
			if err = client.reconnect(err); err != nil {
				break
			}

			continue
		}

		_, pt, length := decodeHeader(header)

		contents := make([]byte, length)

		if _, err = io.ReadFull(client.rw, contents); err != nil {
			if err = client.reconnect(err); err != nil {
				break
			}

			continue
		}

		resp = client.responsePool.Get().(*Response)

		resp.DataType, err = rt.NewPT(pt)

		if err != nil {
			client.err(err)
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
	switch resp.DataType {
	case rt.PT_Error:
		log.Errorln("Received error", resp.Data)

		client.err(getError(resp.Data))

		client.responsePool.Put(resp)

	case rt.PT_StatusRes, rt.PT_JobCreated, rt.PT_EchoRes:
		// NOTE Anything which reads from `expected` must return the
		// response object to the pool.
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
		}

		client.responsePool.Put(resp)
	}
}

func (client *Client) err(e error) {
	if client.ErrorHandler != nil {
		client.ErrorHandler(e)
	}
}

type handleOrError struct {
	handle string
	err    error
}

func (client *Client) request() *request {
	return client.requestPool.Get().(*request)
}

func (client *Client) submit(pt rt.PT, funcname string, arg []byte) string {
	client.outbound <- client.request().submitJob(pt, funcname, IdGen.Id(), arg)

	res := <-client.expected

	client.responsePool.Put(res)

	return res.Handle
}

// Call the function and get a response.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) Do(funcname string, arg []byte,
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

	handle = client.submit(pt, funcname, arg)

	client.handlers.Store(handle, h)

	return
}

// Call the function in background, no response needed.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoBg(funcname string, arg []byte, flag byte) (handle string, err error) {
	var pt rt.PT
	switch flag {
	case rt.JobLow:
		pt = rt.PT_SubmitJobLowBG
	case rt.JobHigh:
		pt = rt.PT_SubmitJobHighBG
	default:
		pt = rt.PT_SubmitJobBG
	}

	handle = client.submit(pt, funcname, arg)

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

	handle = client.submit(rt.PT_SubmitJobSched, funcname, dbyt)

	return
}

func (client *Client) DoAt(funcname string, epoch int64, funcParam []byte) (handle string, err error) {
	if client.conn == nil {
		return "", ErrLostConn
	}

	dbyt := []byte(fmt.Sprintf("%v\x00%v", epoch, string(funcParam)))

	handle = client.submit(rt.PT_SubmitJobEpoch, funcname, dbyt)

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
	client.Lock()
	defer client.Unlock()
	if client.conn != nil {
		err = client.conn.Close()
		client.conn = nil
	}
	return
}
