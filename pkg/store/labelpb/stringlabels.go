package labelpb

import (
	fmt "fmt"
	"unsafe"

	"github.com/VictoriaMetrics/easyproto"
	protohelpers "github.com/planetscale/vtprotobuf/protohelpers"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/pool"
)

var bufPool = pool.MustNewBucketedPool[byte](
	1,
	2048,
	2,
	0,
)

type StringLabelsBuilder struct {
	cur []byte
	buf []byte
}

func EncodeStringSize(stringLen int) int {
	return sizeVarint(uint64(stringLen)) + stringLen
}

func labelSize(m *Label) (n int) {
	// strings are encoded as length followed by contents.
	l := len(m.Name)
	n += l + sizeVarint(uint64(l))
	l = len(m.Value)
	n += l + sizeVarint(uint64(l))
	return n
}

func encodeSize(data []byte, offset, v int) int {
	if v < 1<<7 {
		data[offset] = uint8(v)
		offset++
		return offset
	}
	return encodeVarint(data, offset, uint64(v))
}

func sizeVarint(x uint64) (n int) {
	// Most common case first
	if x < 1<<7 {
		return 1
	}
	if x >= 1<<56 {
		return 9
	}
	if x >= 1<<28 {
		x >>= 28
		n = 4
	}
	if x >= 1<<14 {
		x >>= 14
		n += 2
	}
	if x >= 1<<7 {
		n++
	}
	return n + 1
}

func encodeVarint(data []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	offset++
	return offset
}

func NewStringsLabelsBuilder(size int) (*StringLabelsBuilder, error) {
	b, err := bufPool.Get(size)
	if err != nil {
		return nil, fmt.Errorf("cannot get buffer from pool: %w", err)
	}
	return &StringLabelsBuilder{
		buf: (*b)[:size],
		cur: (*b)[:size],
	}, nil
}

func ReturnPromLabelsToPool(lbls labels.Labels) {
	type promLabels struct {
		data string
	}
	var origLabels = *(*promLabels)(unsafe.Pointer(&lbls))

	bytes := *(*[]byte)(unsafe.Pointer(&origLabels.data))

	bufPool.Put(&bytes)
}

func (b *StringLabelsBuilder) Labels() labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&struct {
		data string
	}{
		data: unsafe.String(unsafe.SliceData(b.buf), len(b.buf)),
	}))
}

// GetLabelsBufferSize returns how many bytes you will need
// for labels.Labels allocation. fieldNumLabels must be
// repeated Labels foo = fieldNumLabels; (in proto file).
func GetLabelsBufferSize(src []byte, fieldNumLabels uint32) (int, error) {
	var (
		fc   easyproto.FieldContext
		size int
		err  error
	)

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return 0, fmt.Errorf("cannot read next field in given byte slice: %w", err)
		}

		switch fc.FieldNum {
		case fieldNumLabels:
			data, ok := fc.MessageData()
			if !ok {
				return 0, fmt.Errorf("cannot read message data")
			}
			sz, err := getLabelEncodingProtobufSize(data)
			if err != nil {
				return 0, fmt.Errorf("getting label encoding size: %w", err)
			}
			size += sz
		}
	}
	return size, nil
}

func getLabelEncodingProtobufSize(src []byte) (int, error) {
	var (
		fc  easyproto.FieldContext
		err error
		n   int
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return 0, fmt.Errorf("cannot read next field in labels")
		}
		if fc.FieldNum > 2 {
			return 0, fmt.Errorf("unexpected field number in labels")
		}
		val, ok := fc.String()
		if !ok {
			return 0, fmt.Errorf("cannot read string value in labels")
		}

		n += EncodeStringSize(len(val))
	}
	return n, nil
}

func (b *StringLabelsBuilder) Add(name, value string) {
	i := 0

	i = encodeSize(b.cur, i, len(name))
	copy(b.cur[i:], name)
	i += len(name)

	i = encodeSize(b.cur, i, len(value))
	copy(b.cur[i:], value)
	i += len(value)

	b.cur = b.cur[i:]
}

func ParseLabelsField(in []byte, b *StringLabelsBuilder) error {
	var (
		fc          easyproto.FieldContext
		err         error
		name, value string
	)
	for len(in) > 0 {
		in, err = fc.NextField(in)
		if err != nil {
			return fmt.Errorf("cannot read next field in given byte slice: %w", err)
		}

		if fc.FieldNum > 2 {
			return fmt.Errorf("unexpected field number in labels")
		}

		switch fc.FieldNum {
		case 1:
			n, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read message data")
			}
			name = n
		case 2:
			v, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read message data")
			}
			value = v
		}
	}

	b.Add(name, value)
	return nil
}

func VTProtoSizeStringLabels(m labels.Label) (n int) {
	var l int

	l = len(m.Name)
	if l > 0 {
		n += 1 + l + protohelpers.SizeOfVarint(uint64(l))
	}
	l = len(m.Value)
	if l > 0 {
		n += 1 + l + protohelpers.SizeOfVarint(uint64(l))
	}
	return n
}
