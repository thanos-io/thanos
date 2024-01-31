package postings

import (
	"bytes"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
)

const (
	RoaringEncoder = "roaringbitmap"
	RawEncoder     = "raw"
)

var _ index.PostingsEncoder = EncodePostingsRoaring

func EncodePostingsRoaring(e *encoding.Encbuf, in []uint32) error {
	bm := roaring.NewBitmap()

	bm.AddMany(in)
	bm.RunOptimize()

	out, err := bm.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshaling roaring bitmap: %w", err)
	}

	e.PutBytes(out)
	return nil
}

var _ index.PostingsDecoder = DecodePostingsRoaring

func DecodePostingsRoaring(b []byte) (int, index.Postings, error) {
	rb := roaring.New()

	_, err := rb.ReadFrom(bytes.NewReader(b))
	if err != nil {
		return 0, nil, err
	}

	rbIter := rb.Iterator()

	return int(rb.GetCardinality()), &postingsRoaringWrapper{IntPeekable: rbIter}, nil
}

type postingsRoaringWrapper struct {
	roaring.IntPeekable

	cur storage.SeriesRef
}

func (w *postingsRoaringWrapper) At() storage.SeriesRef {
	return w.cur
}

func (w *postingsRoaringWrapper) Next() bool {
	if !w.IntPeekable.HasNext() {
		return false
	}

	w.cur = storage.SeriesRef(w.IntPeekable.Next())

	return true
}

func (w *postingsRoaringWrapper) Err() error {
	return nil
}

func (w *postingsRoaringWrapper) Seek(v storage.SeriesRef) bool {
	w.IntPeekable.AdvanceIfNeeded(uint32(v))

	return w.IntPeekable.HasNext()
}
