package comp_chunks

import (
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/errors"
)

var ErrNativeHistogramsUnsupported = errors.Newf("querying native histograms is not supported")

// Iterator is a simple iterator that can only get the next value.
// Iterator iterates over the samples of a time series, in timestamp-increasing order.
type Iterator interface {
	// Next advances the iterator by one.
	Next() bool
	// Seek advances the iterator forward to the first sample with the timestamp equal or greater than t.
	// If current sample found by previous `Next` or `Seek` operation already has this property, Seek has no effect.
	// Seek returns true, if such sample exists, false otherwise.
	// Iterator is exhausted when the Seek returns false.
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	// Before the iterator has advanced At behaviour is unspecified.
	At() (int64, float64)
	// Err returns the current error. It should be used only after iterator is
	// exhausted, that is `Next` or `Seek` returns false.
	Err() error

	It() chunkenc.Iterator
}

type compChunksIterator struct {
	it  chunkenc.Iterator
	err error
}

func (c *compChunksIterator) It() chunkenc.Iterator {
	return c.it
}

func (c *compChunksIterator) Next() bool {
	switch c.it.Next() {
	case chunkenc.ValNone:
		return false
	case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
		c.err = ErrNativeHistogramsUnsupported
	}

	return true
}

func (c *compChunksIterator) Seek(t int64) bool {
	switch c.it.Seek(t) {
	case chunkenc.ValNone:
		return false
	case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
		c.err = ErrNativeHistogramsUnsupported
	}

	return true
}

func (c *compChunksIterator) At() (int64, float64) {
	return c.At()
}

func (c *compChunksIterator) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.it.Err()
}

func NewCompChunksIterator(i chunkenc.Iterator) Iterator {
	return &compChunksIterator{it: i}
}
