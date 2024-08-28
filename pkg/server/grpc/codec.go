// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package grpc

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	// Guarantee that the built-in proto is called registered before this one
	// so that it can be replaced.
	_ "google.golang.org/grpc/encoding/proto"
)

const Name = "proto"

type protoMessage interface {
	Unmarshal([]byte) error
	MarshalTo([]byte) (int, error)
	MarshalToSizedBuffer([]byte) (int, error)
	Size() int
}

type Codec struct {
	fallback encoding.CodecV2
}

func (c *Codec) Marshal(v any) (data mem.BufferSlice, err error) {
	if m, ok := v.(protoMessage); ok {
		size := m.Size()
		if mem.IsBelowBufferPoolingThreshold(size) {
			buf := make([]byte, size)
			if _, err := m.MarshalToSizedBuffer(buf); err != nil {
				return nil, err
			}
			data = append(data, mem.SliceBuffer(buf))
		} else {
			pool := mem.DefaultBufferPool()
			buf := pool.Get(size)
			if _, err := m.MarshalToSizedBuffer((*buf)[:size]); err != nil {
				pool.Put(buf)
				return nil, err
			}

			data = append(data, mem.NewBuffer(buf, pool))
		}
		return data, nil
	}

	return c.fallback.Marshal(v)
}

func (Codec) Name() string { return Name }

func (c *Codec) Unmarshal(data mem.BufferSlice, v any) error {
	if m, ok := v.(protoMessage); ok {
		buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
		defer buf.Free()
		return m.Unmarshal(buf.ReadOnlyData())
	}

	return c.fallback.Unmarshal(data, v)
}

func init() {
	encoding.RegisterCodecV2(&Codec{
		fallback: encoding.GetCodecV2("proto"),
	})
}
