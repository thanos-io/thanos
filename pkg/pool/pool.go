// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pool

import (
	"sync"

	"github.com/pkg/errors"
)

// Pool is a pool for slices of type T that can be reused.
type Pool[T any] interface {
	// Get returns a new T slice that fits the given size.
	Get(sz int) (*[]T, error)
	// Put returns a T slice to the right bucket in the pool.
	Put(b *[]T)
}

// NoopPool is pool that always allocated required slice on heap and ignore puts.
type NoopPool[T any] struct{}

func (p NoopPool[T]) Get(sz int) (*[]T, error) {
	b := make([]T, 0, sz)
	return &b, nil
}

func (p NoopPool[T]) Put(*[]T) {}

// BucketedPool is a bucketed pool for variably sized T slices. It can be
// configured to not allow more than a maximum number of T items being used at a
// given time. Every slice obtained from the pool must be returned.
type BucketedPool[T any] struct {
	buckets   []sync.Pool
	sizes     []int
	maxTotal  uint64
	usedTotal uint64
	mtx       sync.RWMutex

	new func(s int) *[]T
}

// MustNewBucketedPool is like NewBucketedPool but panics if construction fails.
// Useful for package internal pools.
func MustNewBucketedPool[T any](minSize, maxSize int, factor float64, maxTotal uint64) *BucketedPool[T] {
	pool, err := NewBucketedPool[T](minSize, maxSize, factor, maxTotal)
	if err != nil {
		panic(err)
	}
	return pool
}

// NewBucketedPool returns a new BucketedPool with size buckets for minSize to
// maxSize increasing by the given factor and maximum number of used items. No
// more than maxTotal items can be used at any given time unless maxTotal is set
// to 0.
func NewBucketedPool[T any](minSize, maxSize int, factor float64, maxTotal uint64) (*BucketedPool[T], error) {
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
	p := &BucketedPool[T]{
		buckets:  make([]sync.Pool, len(sizes)),
		sizes:    sizes,
		maxTotal: maxTotal,
		new: func(sz int) *[]T {
			s := make([]T, 0, sz)
			return &s
		},
	}
	return p, nil
}

// ErrPoolExhausted is returned if a pool cannot provide the requested slice.
var ErrPoolExhausted = errors.New("pool exhausted")

// Get returns a slice into from the bucket that fits the given size.
func (p *BucketedPool[T]) Get(sz int) (*[]T, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.maxTotal > 0 && p.usedTotal+uint64(sz) > p.maxTotal {
		return nil, ErrPoolExhausted
	}

	for i, bktSize := range p.sizes {
		if sz > bktSize {
			continue
		}
		b, ok := p.buckets[i].Get().(*[]T)
		if !ok {
			b = p.new(bktSize)
		}

		p.usedTotal += uint64(cap(*b))
		return b, nil
	}

	// The requested size exceeds that of our highest bucket, allocate it directly.
	p.usedTotal += uint64(sz)
	return p.new(sz), nil
}

// Put returns a slice to the right bucket in the pool.
func (p *BucketedPool[T]) Put(b *[]T) {
	if b == nil {
		return
	}

	sz := cap(*b)
	for i, bktSize := range p.sizes {
		if sz > bktSize {
			continue
		}
		*b = (*b)[:0]
		p.buckets[i].Put(b)
		break
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	// We could assume here that our users will not make the slices larger
	// but lets be on the safe side to avoid an underflow of p.usedTotal.
	if uint64(sz) >= p.usedTotal {
		p.usedTotal = 0
	} else {
		p.usedTotal -= uint64(sz)
	}
}

func (p *BucketedPool[T]) UsedBytes() uint64 {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	return p.usedTotal
}
