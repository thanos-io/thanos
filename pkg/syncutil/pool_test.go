// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package syncutil

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestPool_GetReturn(t *testing.T) {
	p := NewPool(func() *bytes.Buffer {
		return new(bytes.Buffer)
	}).Build()

	buf, ret := p.Get()
	buf.WriteString("hello")
	testutil.Equals(t, "hello", buf.String())
	ret(buf)
}

func TestPool_WithReset(t *testing.T) {
	p := NewPool(func() *bytes.Buffer {
		return &bytes.Buffer{}
	}).WithReset(func(b *bytes.Buffer) bool {
		b.Reset()
		b.WriteString("cleared")
		return true
	}).Build()

	buf, ret := p.Get()
	testutil.Equals(t, "", buf.String())
	buf.WriteString("data")
	ret(buf)
	// !!! usually we would never hold onto a reference
	// after returning it to the pool, but this is for testing.
	testutil.Equals(t, "cleared", buf.String())

	buf2, ret := p.Get()
	defer ret(buf2)
	if buf2 == buf { // Note: sometimes the same object is returned, sometimes it is not.
		testutil.Equals(t, "cleared", buf2.String())
	} else {
		testutil.Equals(t, "", buf2.String())
	}
}

func TestPool_WithByteSlicePointer(t *testing.T) {
	p := NewPool(func() *[]byte {
		b := make([]byte, 0, 10)
		return &b
	}).WithReset(func(b *[]byte) bool {
		*b = (*b)[:0]
		return true
	}).Build()

	buf, ret := p.Get()
	testutil.Equals(t, 10, cap(*buf))
	testutil.Equals(t, 0, len(*buf))
	*buf = append(*buf, make([]byte, 20)...)
	testutil.Equals(t, 24, cap(*buf))
	testutil.Equals(t, 20, len(*buf))
	ret(buf)
	// !!! usually we would never hold onto a reference
	// after returning it to the pool, but this is for testing.
	testutil.Equals(t, 24, cap(*buf))
	testutil.Equals(t, 0, len(*buf))
	buf2, ret := p.Get()
	defer ret(buf)
	if buf2 == buf { // Note: sometimes the same object is returned, sometimes it is not.
		testutil.Equals(t, 24, cap(*buf2))
	} else {
		testutil.Equals(t, 10, cap(*buf2))
	}
	testutil.Equals(t, 0, len(*buf2))
}

func TestPool_Disabled(t *testing.T) {
	p := NewPool(func() *bytes.Buffer {
		return new(bytes.Buffer)
	}).
		WithDisabled(true).
		Build()

	seen := make(map[*bytes.Buffer]struct{})

	for i := 0; i < 10; i++ {
		buf, ret := p.Get()
		_, exists := seen[buf]
		testutil.Equals(t, false, exists, fmt.Sprintf("iteration %d: got a recycled pointer from a disabled pool", i))
		seen[buf] = struct{}{}
		ret(buf)
	}
}

func TestPool_EnabledByDefault(t *testing.T) {
	p := NewPool(func() *bytes.Buffer {
		return new(bytes.Buffer)
	}).Build()

	seen := make(map[*bytes.Buffer]struct{})

	// Iterate 10 times because sometimes the pool will return the same object,
	// sometimes it will return a new object.
	// We expect to find at least one object in the pool.
	foundOne := false
	for i := 0; i < 10; i++ {
		buf, ret := p.Get()
		_, exists := seen[buf]
		foundOne = foundOne || exists
		seen[buf] = struct{}{}
		ret(buf)
	}
	testutil.Equals(t, true, foundOne, "expected to find at least one object in the pool")
}
