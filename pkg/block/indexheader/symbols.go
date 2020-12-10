package indexheader

import (
	"fmt"
	"sort"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
)

// Symbols are similar to TSDB's index.Symbols except with symbolFactor == 1 (cache all) to avoid latency penalty.
// Since we do lazy header reading, we can afford mmaping all in memory.
// NOTE: Looked up symbols' strings are valid until the header is open (mmaped).
type Symbols struct {
	bs      index.ByteSlice
	version int
	off     int

	symbols map[uint32]string
	keys    []uint32
}

// NewSymbols returns a Symbols object for symbol lookups.
func NewSymbols(bs index.ByteSlice, version int, off int) (*Symbols, error) {
	s := &Symbols{
		bs:      bs,
		version: version,
		off:     off,
	}

	d := encoding.NewDecbufAt(bs, off, castagnoliTable)
	var (
		origLen = d.Len()
		cnt     = d.Be32int()
		basePos = uint32(off) + 4
		nextPos = basePos + uint32(origLen-d.Len())
	)

	if version == index.FormatV2 {
		nextPos = 0
	}

	s.symbols = make(map[uint32]string, cnt)
	s.keys = make([]uint32, 0, cnt)
	fmt.Println("number of symbols", cnt, "size s", unsafe.Sizeof(s.symbols), "size", unsafe.Sizeof(s.keys))
	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		s.symbols[nextPos] = yoloString(d.UvarintBytes())
		s.keys = append(s.keys, nextPos)

		if version == index.FormatV2 {
			nextPos++
		} else {
			nextPos = basePos + uint32(origLen-d.Len())
		}
		cnt--
	}
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "read symbols")
	}
	fmt.Println("number of symbols", cnt, "size s", s.Size())
	return s, nil
}

func (s *Symbols) Lookup(o uint32) (string, error) {
	str, ok := s.symbols[o]
	if !ok {
		return "", errors.Errorf("symbol not found for %q", o)
	}
	return str, nil
}

func (s Symbols) ReverseLookup(sym string) (uint32, error) {
	i := sort.Search(len(s.keys), func(i int) bool {
		return s.symbols[s.keys[i]] > sym
	})
	if i < 0 || i > len(s.keys) {
		return 0, errors.Errorf("unknown symbol %q", sym)
	}
	return s.keys[i], nil
}

func (s Symbols) Size() int {
	return len(s.symbols) * 8
}

// Contribute to Prometheus.
func NewSymbolsIterator(bs index.ByteSlice, off int) index.StringIter {
	d := encoding.NewDecbufAt(bs, off, castagnoliTable)
	cnt := d.Be32int()
	return &symbolsIter{
		d:   d,
		cnt: cnt,
	}
}

func (s Symbols) Iter() index.StringIter {
	return NewSymbolsIterator(s.bs, s.off)
}

// symbolsIter implements StringIter.
type symbolsIter struct {
	d   encoding.Decbuf
	cnt int
	cur string
	err error
}

func (s *symbolsIter) Next() bool {
	if s.cnt == 0 || s.err != nil {
		return false
	}
	s.cur = yoloString(s.d.UvarintBytes())
	s.cnt--
	if s.d.Err() != nil {
		s.err = s.d.Err()
		return false
	}
	return true
}

func (s symbolsIter) At() string { return s.cur }
func (s symbolsIter) Err() error { return s.err }
