package client

import (
	rt "github.com/quantcast/g2/pkg/runtime"
)

// Request from client
type request struct {
	pt       rt.PT
	expected chan *Response
	data     [][]byte
}

func (req *request) close() {
	if req.expected != nil {
		close(req.expected)
	}
}

func (req *request) args(args ...[]byte) {
	req.data = req.data[:0]
	req.data = append(req.data, args...)
	req.expected = make(chan *Response, 1)
}

func (req *request) submit(reqType rt.PT, funcname, id string, arg []byte) *request {
	req.pt = reqType

	req.args([]byte(funcname), []byte(id), arg)

	return req
}

func (req *request) status(handle string) *request {
	req.pt = rt.PT_GetStatus

	req.args([]byte(handle))

	return req
}

func (req *request) echo(arg []byte) *request {
	req.pt = rt.PT_EchoReq

	req.args(arg)

	return req
}
