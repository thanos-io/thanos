package pool

import (
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

// BytesPool is a bucketed pool for variably sized byte slices. It can be configured to not allow
// more than a maximum number of bytes being used at a given time.
// Every byte slice obtained from the pool must be returned.
type BytesPool struct {
	buckets   []sync.Pool
	sizes     []int
	maxTotal  uint64
	usedTotal uint64

	new func(s int) []byte
}

// NewBytesPool returns a new BytesPool with size buckets for minSize to maxSize
// increasing by the given factor and maximum number of used bytes.
// No more than maxTotal bytes can be used at any given time unless maxTotal is set to 0.
func NewBytesPool(minSize, maxSize int, factor float64, maxTotal uint64) (*BytesPool, error) {
	if minSize < 1 {
		return nil, errors.New("invalid minimum pool size")
	}
	if maxSize < 1 {
		return nil, errors.New("invalid maximum pool size")
	}
	if factor < 1 {
		return nil, errors.New("invalid factor")
	}

	var sizes []int

	for s := minSize; s <= maxSize; s = int(float64(s) * factor) {
		sizes = append(sizes, s)
	}
	p := &BytesPool{
		buckets:  make([]sync.Pool, len(sizes)),
		sizes:    sizes,
		maxTotal: maxTotal,
		new: func(sz int) []byte {
			return make([]byte, 0, sz)
		},
	}
	return p, nil
}

// ErrPoolExhausted is returned if a pool cannot provide the request bytes.
var ErrPoolExhausted = errors.New("pool exhausted")

// Get returns a new byte slices that fits the given size.
func (p *BytesPool) Get(sz int) ([]byte, error) {
	used := atomic.LoadUint64(&p.usedTotal)

	if p.maxTotal > 0 && used+uint64(sz) > p.maxTotal {
		return nil, ErrPoolExhausted
	}
	for i, bktSize := range p.sizes {
		if sz > bktSize {
			continue
		}
		b, ok := p.buckets[i].Get().([]byte)
		if !ok {
			b = p.new(bktSize)
		}
		atomic.AddUint64(&p.usedTotal, uint64(cap(b)))
		return b, nil
	}

	// The requested size exceeds that of our highest bucket, allocate it directly.
	atomic.AddUint64(&p.usedTotal, uint64(sz))
	return p.new(sz), nil
}

// Put returns a byte slice to the right bucket in the pool.
func (p *BytesPool) Put(b []byte) {
	for i, bktSize := range p.sizes {
		if cap(b) > bktSize {
			continue
		}
		p.buckets[i].Put(b[:0])
		break
	}
	atomic.AddUint64(&p.usedTotal, ^uint64(p.usedTotal-1))
}
