// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package labelpbv2

import (
	"fmt"
	"math/bits"
	"sync"

	"github.com/CrowdStrike/csproto"
	"github.com/VictoriaMetrics/easyproto"
	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func (l *LabelSetV2) Unmarshal(src []byte) (err error) {
	var b = builderPool.Get().(*labels.ScratchBuilder)
	b.Reset()

	defer builderPool.Put(b)

	var fc easyproto.FieldContext

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return errors.Wrap(err, "unmarshal next field")
		}

		if fc.FieldNum != 1 {
			return fmt.Errorf("expected field 1, got %d", fc.FieldNum)
		}

		dat, ok := fc.MessageData()
		if !ok {
			return fmt.Errorf("expected message data for field %d", fc.FieldNum)
		}

		var n, v string
		var msgFc easyproto.FieldContext
		for len(dat) > 0 {
			dat, err = msgFc.NextField(dat)
			if err != nil {
				return errors.Wrap(err, "unmarshal next field in message")
			}

			switch msgFc.FieldNum {
			case 1:
				n, ok = msgFc.String()
				if !ok {
					return fmt.Errorf("expected string data for field %d", msgFc.FieldNum)
				}
			case 2:
				v, ok = msgFc.String()
				if !ok {
					return fmt.Errorf("expected string data for field %d", msgFc.FieldNum)
				}
			default:
				return fmt.Errorf("unexpected field %d in label message", msgFc.FieldNum)
			}
		}

		b.Add(n, v)

	}

	*l = LabelSetV2(b.Labels())
	return nil
}

// A label is only used part of a labelset so we will never have one label by itself.
type LabelSetV2 labels.Labels

type LabelV2 labels.Label

var _ extgrpc.GogoMsg = (func() *LabelSetV2 { lbls := labels.EmptyLabels(); return (*LabelSetV2)(&lbls) }())

// Size calculates and returns the size, in bytes, required to hold the contents of m using the Protobuf
// binary encoding.
func (m *LabelV2) Size() int {
	// calculate and cache
	var sz, l int
	_ = l // avoid unused variable

	// Name (string,optional)
	if l = len(m.Name); l > 0 {
		sz += csproto.SizeOfTagKey(1) + csproto.SizeOfVarint(uint64(l)) + l
	}
	// Value (string,optional)
	if l = len(m.Value); l > 0 {
		sz += csproto.SizeOfTagKey(2) + csproto.SizeOfVarint(uint64(l)) + l
	}
	return sz
}

func sovTypes(x uint64) (n int) {
	return (bits.Len64(x|1) + 6) / 7
}

// nolint:unparam
func (m *LabelV2) unrolledMarshal(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Value) > 0 {
		i -= len(m.Value)
		copy(dAtA[i:], m.Value)
		i -= sovTypes(uint64(len(m.Value)))
		_ = csproto.EncodeVarint(dAtA[i:], uint64(len(m.Value)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Name) > 0 {
		i -= len(m.Name)
		copy(dAtA[i:], m.Name)
		i -= sovTypes(uint64(len(m.Name)))
		_ = csproto.EncodeVarint(dAtA[i:], uint64(len(m.Name)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *LabelV2) MarshalTo(dest []byte) error {
	var (
		enc    = csproto.NewEncoder(dest)
		buf    []byte
		err    error
		extVal interface{}
	)
	// ensure no unused variables
	_ = enc
	_ = buf
	_ = err
	_ = extVal

	// Name (1,string,optional)
	if len(m.Name) > 0 {
		enc.EncodeString(1, m.Name)
	}
	// Value (2,string,optional)
	if len(m.Value) > 0 {
		enc.EncodeString(2, m.Value)
	}
	return nil
}

// MarshalTo converts the contents of m to the Protobuf binary encoding and writes the result to dest.
func (l *LabelSetV2) MarshalToSizedBuffer(dest []byte) (int, error) {
	if l == nil {
		return 0, nil
	}

	var outErr error
	var wrote int

	// NOTE(GiedriusS): easyproto contains some internal buffer that could be reused but it still allocs.
	// This is a very optimized function.

	(*labels.Labels)(l).Range(func(lb labels.Label) {
		ls := (*LabelV2)(&lb)
		lsSize := ls.Size()

		dest[0] = 0xa // field 1, wiretype 2
		dest = dest[1:]

		wrote += 1

		n := csproto.EncodeVarint(dest, uint64(lsSize))
		dest = dest[n:]

		wrote += n

		n, err := ls.unrolledMarshal(dest[:lsSize])
		if err != nil {
			outErr = err
			return
		}
		dest = dest[n:]
		wrote += n
	})

	if outErr != nil {
		return 0, outErr
	}

	return len(dest), nil
}

// Size calculates and returns the size, in bytes, required to hold the contents of m using the Protobuf
// binary encoding.
func (m *LabelSetV2) Size() int {
	// nil message is always 0 bytes
	if m == nil {
		return 0
	}
	var ls = &LabelV2{}

	// calculate and cache
	var sz, l int
	_ = l // avoid unused variable

	// Labels (message,repeated)

	lbls := (*labels.Labels)(m)
	lbls.Range(func(lb labels.Label) {
		ls = (*LabelV2)(&lb)
		if l = ls.Size(); l > 0 {
			sz += csproto.SizeOfTagKey(1) + csproto.SizeOfVarint(uint64(l)) + l
		}
	})
	return sz
}

var builderPool = sync.Pool{
	New: func() any {
		bl := labels.NewScratchBuilder(0)
		return &bl
	},
}

func DebugConvertZLabelsToLabelSetV2(lbls []labelpb.ZLabel) LabelSetV2 {
	var builder = builderPool.Get().(*labels.ScratchBuilder)
	defer builderPool.Put(builder)
	builder.Reset()

	for _, l := range lbls {
		builder.Add(l.Name, l.Value)
	}

	return LabelSetV2(builder.Labels())
}
