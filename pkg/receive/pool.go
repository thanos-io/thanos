// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"sync"

	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

type requestPool struct {
	pool *sync.Pool
}

func newRequestPool() *requestPool {
	return &requestPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &prompb.WriteRequest{}
			},
		},
	}
}

func (p *requestPool) get() *prompb.WriteRequest {
	return p.pool.Get().(*prompb.WriteRequest)
}

func (p *requestPool) put(request *prompb.WriteRequest) {
	request.Timeseries = request.Timeseries[:0]
	request.Metadata = request.Metadata[:0]
	p.pool.Put(request)
}

type bytesPool struct {
	pool *sync.Pool
}

func newBytesPool() *bytesPool {
	return &bytesPool{
		pool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 0, 1024)
				return &buf
			},
		},
	}
}

func (p *bytesPool) get() *[]byte {
	return p.pool.Get().(*[]byte)
}

func (p *bytesPool) put(b *[]byte) {
	*b = (*b)[:0]
	p.pool.Put(b)
}
