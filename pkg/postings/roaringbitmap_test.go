package postings

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"
)

func TestRoaringEncodeDecode(t *testing.T) {
	e := &encoding.Encbuf{}

	require.NoError(t, EncodePostingsRoaring(e, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))

	n, p, err := DecodePostingsRoaring(e.Get())
	require.NoError(t, err)

	require.Equal(t, 10, n)
	require.Equal(t, true, p.Next())
}
