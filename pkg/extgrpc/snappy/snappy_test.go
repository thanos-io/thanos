// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package snappy

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnappy(t *testing.T) {
	c := newCompressor()
	assert.Equal(t, "snappy", c.Name())

	tests := []struct {
		test  string
		input string
	}{
		{"empty", ""},
		{"short", "hello world"},
		{"long", strings.Repeat("123456789", 1024)},
	}
	for _, test := range tests {
		t.Run(test.test, func(t *testing.T) {
			var buf bytes.Buffer
			// Compress
			w, err := c.Compress(&buf)
			require.NoError(t, err)
			n, err := w.Write([]byte(test.input))
			require.NoError(t, err)
			assert.Len(t, test.input, n)
			err = w.Close()
			require.NoError(t, err)
			// Decompress
			r, err := c.Decompress(&buf)
			require.NoError(t, err)
			out, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, test.input, string(out))
		})
	}
}

func BenchmarkSnappyCompress(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))
	c := newCompressor()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w, _ := c.Compress(io.Discard)
		_, _ = w.Write(data)
		_ = w.Close()
	}
}

func BenchmarkSnappyDecompress(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))
	c := newCompressor()
	var buf bytes.Buffer
	w, _ := c.Compress(&buf)
	_, _ = w.Write(data)
	reader := bytes.NewReader(buf.Bytes())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := c.Decompress(reader)
		_, _ = io.ReadAll(r)
		_, _ = reader.Seek(0, io.SeekStart)
	}
}
