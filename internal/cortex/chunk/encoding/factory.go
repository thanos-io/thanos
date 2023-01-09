// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package encoding

import "fmt"

// Encoding defines which encoding we are using, delta, doubledelta, or varbit
type Encoding byte

var (
	// DefaultEncoding exported for use in unit tests elsewhere
	DefaultEncoding      = Bigchunk
	bigchunkSizeCapBytes = 0
)

// String implements flag.Value.
func (e Encoding) String() string {
	if known, found := encodings[e]; found {
		return known.Name
	}
	return fmt.Sprintf("%d", e)
}

const (
	// Delta encoding is no longer supported and will be automatically changed to DoubleDelta.
	// It still exists here to not change the `ingester.chunk-encoding` flag values.
	Delta Encoding = iota
	// DoubleDelta encoding
	DoubleDelta
	// Varbit encoding
	Varbit
	// Bigchunk encoding
	Bigchunk
	// PrometheusXorChunk is a wrapper around Prometheus XOR-encoded chunk.
	PrometheusXorChunk
)

type encoding struct {
	Name string
	New  func() Chunk
}

var encodings = map[Encoding]encoding{
	DoubleDelta: {
		Name: "DoubleDelta",
		New: func() Chunk {
			return newDoubleDeltaEncodedChunk(d1, d0, true, ChunkLen)
		},
	},
	Varbit: {
		Name: "Varbit",
		New: func() Chunk {
			return newVarbitChunk(varbitZeroEncoding)
		},
	},
	Bigchunk: {
		Name: "Bigchunk",
		New: func() Chunk {
			return newBigchunk()
		},
	},
	PrometheusXorChunk: {
		Name: "PrometheusXorChunk",
		New: func() Chunk {
			return newPrometheusXorChunk()
		},
	},
}

// New creates a new chunk according to the encoding set by the
// DefaultEncoding flag.
func New() Chunk {
	chunk, err := NewForEncoding(DefaultEncoding)
	if err != nil {
		panic(err)
	}
	return chunk
}

// NewForEncoding allows configuring what chunk type you want
func NewForEncoding(encoding Encoding) (Chunk, error) {
	enc, ok := encodings[encoding]
	if !ok {
		return nil, fmt.Errorf("unknown chunk encoding: %v", encoding)
	}

	return enc.New(), nil
}
