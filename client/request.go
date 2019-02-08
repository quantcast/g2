package client

import (
	rt "github.com/quantcast/g2/pkg/runtime"
)

// Request from client
type request struct {
	pt   rt.PT
	data [][]byte
}

func (req *request) args(args ...[]byte) {
	req.data = req.data[:0]
	req.data = append(req.data, args...)
}

func (req *request) submitJob(pt rt.PT, funcname, id string, arg []byte) *request {
	req.pt = pt

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
