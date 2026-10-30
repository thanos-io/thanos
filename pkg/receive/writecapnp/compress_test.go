// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"capnproto.org/go/capnp/v3"
	rpccp "capnproto.org/go/capnp/v3/std/capnp/rpc"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func wireSeries(n int) []prompb.TimeSeries {
	ts := make([]prompb.TimeSeries, 0, n)
	now := time.Now().UnixMilli()
	for i := range n {
		pod := fmt.Sprintf("app-%d-7d9f8c6b5-%05d", i%40, (i/40)%300)
		ls := []labelpb.ZLabel{
			{Name: "__name__", Value: fmt.Sprintf("http_server_requests_seconds_bucket_%d", i%60)},
			{Name: "cluster", Value: "live1"},
			{Name: "container", Value: fmt.Sprintf("app-%d", i%40)},
			{Name: "endpoint", Value: "http-metrics"},
			{Name: "env", Value: "prod"},
			{Name: "instance", Value: fmt.Sprintf("10.42.%d.%d:8080", (i/40)%250, i%250)},
			{Name: "job", Value: fmt.Sprintf("app-%d", i%40)},
			{Name: "le", Value: fmt.Sprintf("%g", float64(i%12)*0.25)},
			{Name: "method", Value: []string{"GET", "POST", "PUT"}[i%3]},
			{Name: "namespace", Value: fmt.Sprintf("team-%d", i%12)},
			{Name: "pod", Value: pod},
			{Name: "prometheus", Value: "monitoring/prometheus-app"},
			{Name: "prometheus_replica", Value: "prometheus-app-0"},
			{Name: "service", Value: fmt.Sprintf("app-%d", i%40)},
			{Name: "status", Value: []string{"200", "404", "500"}[i%3]},
		}
		ts = append(ts, prompb.TimeSeries{Labels: ls, Samples: []prompb.Sample{{Value: float64(i * 7), Timestamp: now + int64(i%15)}}})
	}
	return ts
}

// largeSeries builds n series whose label values are valueSize bytes each, so a
// message of several MiB costs only a handful of allocations.
func largeSeries(n, valueSize int) []prompb.TimeSeries {
	ts := make([]prompb.TimeSeries, 0, n)
	for i := range n {
		ts = append(ts, prompb.TimeSeries{
			Labels:  []labelpb.ZLabel{{Name: "__name__", Value: "m"}, {Name: "blob", Value: fmt.Sprintf("%d-", i) + strings.Repeat("x", valueSize)}},
			Samples: []prompb.Sample{{Value: float64(i), Timestamp: int64(i)}},
		})
	}
	return ts
}

// zstdFrameHeader returns a zstd frame header declaring contentSize bytes of
// content without any blocks behind it.
func zstdFrameHeader(contentSize uint64) []byte {
	hdr := []byte{0x28, 0xb5, 0x2f, 0xfd, 0xe0} // magic, FCS flag = 8 bytes, single segment
	return binary.LittleEndian.AppendUint64(hdr, contentSize)
}

type nopCloser struct{ *bytes.Buffer }

func (nopCloser) Close() error { return nil }

func testEncoder(tb testing.TB) *zstd.Encoder {
	tb.Helper()
	enc, err := zstdEncoderFor(DefaultCompressionConcurrency)
	require.NoError(tb, err)
	return enc
}

func testCodec(tb testing.TB, buf *bytes.Buffer) *zstdCodec {
	tb.Helper()
	return newZstdCodec(bufio.NewReader(buf), nopCloser{buf}, testEncoder(tb))
}

func TestZstdCodecRoundTrip(t *testing.T) {
	wr, err := Build("tenant", wireSeries(500))
	require.NoError(t, err)
	want, err := wr.Message().Marshal()
	require.NoError(t, err)

	var buf bytes.Buffer
	z := testCodec(t, &buf)
	for range 3 {
		require.NoError(t, z.Encode(wr.Message()))
	}
	for range 3 {
		msg, err := z.Decode()
		require.NoError(t, err)
		got, err := msg.Marshal()
		require.NoError(t, err)
		require.Equal(t, want, got)
		msg.Release()
	}
}

func BenchmarkCodecRoundTrip(b *testing.B) {
	wr, err := Build("tenant", wireSeries(2000))
	require.NoError(b, err)
	msg := wr.Message()

	b.Run("packed", func(b *testing.B) {
		var buf bytes.Buffer
		enc, dec := capnp.NewPackedEncoder(&buf), capnp.NewPackedDecoder(&buf)
		b.ReportAllocs()
		for b.Loop() {
			if err := enc.Encode(msg); err != nil {
				b.Fatal(err)
			}
			m, err := dec.Decode()
			if err != nil {
				b.Fatal(err)
			}
			m.Release()
		}
	})
	b.Run("zstd", func(b *testing.B) {
		var buf bytes.Buffer
		z := testCodec(b, &buf)
		b.ReportAllocs()
		for b.Loop() {
			if err := z.Encode(msg); err != nil {
				b.Fatal(err)
			}
			m, err := z.Decode()
			if err != nil {
				b.Fatal(err)
			}
			m.Release()
		}
	})
}

func TestZstdCodecRoundTripLargeMessage(t *testing.T) {
	wr, err := Build("tenant", largeSeries(4, 512<<10))
	require.NoError(t, err)
	want, err := wr.Message().Marshal()
	require.NoError(t, err)
	require.Greater(t, len(want), zstdWindowSize)

	var buf bytes.Buffer
	z := testCodec(t, &buf)
	require.NoError(t, z.Encode(wr.Message()))
	msg, err := z.Decode()
	require.NoError(t, err)
	got, err := msg.Marshal()
	require.NoError(t, err)
	require.Equal(t, want, got)
	msg.Release()
}

func TestZstdCodecConcurrentEncode(t *testing.T) {
	const workers, perWorker = 8, 10

	var want [][]byte
	var reqs []WriteRequest
	for w := range workers {
		wr, err := Build(fmt.Sprintf("tenant-%d", w), wireSeries(100))
		require.NoError(t, err)
		b, err := wr.Message().Marshal()
		require.NoError(t, err)
		reqs, want = append(reqs, wr), append(want, b)
	}

	var buf bytes.Buffer
	z := testCodec(t, &buf)

	errs := make(chan error, workers*perWorker)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for range perWorker {
				if err := z.Encode(reqs[w].Message()); err != nil {
					errs <- fmt.Errorf("worker %d: %w", w, err)
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	seen := map[string]int{}
	for range workers * perWorker {
		msg, err := z.Decode()
		require.NoError(t, err)
		got, err := msg.Marshal()
		require.NoError(t, err)
		seen[string(got)]++
		msg.Release()
	}
	for w := range workers {
		require.Equal(t, perWorker, seen[string(want[w])], "worker %d", w)
	}
}

func TestZstdCodecRejectsInvalidFrames(t *testing.T) {
	frame := func(payload []byte) []byte {
		return append(binary.AppendUvarint(nil, uint64(len(payload))), payload...)
	}
	wr, err := Build("tenant", wireSeries(10))
	require.NoError(t, err)
	raw, err := wr.Message().Marshal()
	require.NoError(t, err)
	enc := testEncoder(t)
	valid := frame(enc.EncodeAll(raw, nil))

	for _, tc := range []struct {
		name string
		data []byte
		err  string
	}{
		{"length over limit", binary.AppendUvarint(nil, maxMessageSize+1), "exceeds"},
		{"truncated frame", valid[:len(valid)-1], "unexpected EOF"},
		{"not zstd", frame([]byte("not a zstd frame")), "decompress message"},
		{"decompressed size over limit", frame(zstdFrameHeader(maxMessageSize + 1)), zstd.ErrDecoderSizeExceeded.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			z := newZstdCodec(bufio.NewReader(bytes.NewReader(tc.data)), nopCloser{&bytes.Buffer{}}, enc)
			_, err := z.Decode()
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestZstdCodecReleasesLargeBuffers(t *testing.T) {
	big, err := Build("tenant", largeSeries(8, 1<<20))
	require.NoError(t, err)
	bigRaw, err := big.Message().Marshal()
	require.NoError(t, err)
	require.Greater(t, len(bigRaw), maxRetainedBufferSize, "test message must exceed the retained buffer size")

	small, err := Build("tenant", wireSeries(100))
	require.NoError(t, err)

	var buf bytes.Buffer
	z := testCodec(t, &buf)

	require.NoError(t, z.Encode(small.Message()))
	msg, err := z.Decode()
	require.NoError(t, err)
	msg.Release()
	require.Positive(t, cap(z.out))
	require.Positive(t, z.raw.Cap())
	require.Positive(t, cap(z.in))
	require.Positive(t, cap(z.plain))

	require.NoError(t, z.Encode(big.Message()))
	require.Zero(t, z.raw.Cap(), "uncompressed scratch buffer must be dropped")
	require.LessOrEqual(t, cap(z.out), maxRetainedBufferSize, "compressed buffer is below the limit and may be kept")
	msg, err = z.Decode()
	require.NoError(t, err)
	got, err := msg.Marshal()
	require.NoError(t, err)
	require.Equal(t, bigRaw, got)
	msg.Release()
	require.LessOrEqual(t, cap(z.in), maxRetainedBufferSize, "compressed input is below the limit and may be kept")
	require.Zero(t, cap(z.plain), "decompressed scratch buffer must be dropped")

	require.NoError(t, z.Encode(small.Message()))
	msg, err = z.Decode()
	require.NoError(t, err)
	msg.Release()
	require.LessOrEqual(t, cap(z.in), maxRetainedBufferSize)
	require.LessOrEqual(t, cap(z.plain), maxRetainedBufferSize)
}

func TestZstdEncoderFor(t *testing.T) {
	a, err := zstdEncoderFor(3)
	require.NoError(t, err)
	b, err := zstdEncoderFor(3)
	require.NoError(t, err)
	require.Same(t, a, b, "same concurrency must share one encoder")

	c, err := zstdEncoderFor(5)
	require.NoError(t, err)
	require.NotSame(t, a, c, "different concurrency must not share an encoder")

	auto, err := zstdEncoderFor(0)
	require.NoError(t, err)
	explicit, err := zstdEncoderFor(runtime.GOMAXPROCS(0))
	require.NoError(t, err)
	require.Same(t, explicit, auto, "0 must resolve to GOMAXPROCS")

	_, err = zstdEncoderFor(-1)
	require.ErrorContains(t, err, "must not be negative")
}

func TestServerTransportPrefaceShortRead(t *testing.T) {
	client, server := net.Pipe()
	go func() {
		_, _ = client.Write(zstdPreface[:2])
		_ = client.Close()
	}()
	_, err := NewServerTransport(server, DefaultCompressionConcurrency)
	require.ErrorContains(t, err, "read connection preface")
	_ = server.Close()
}

func TestServerTransportRejectsBadConcurrency(t *testing.T) {
	client, server := net.Pipe()
	go func() {
		_, _ = client.Write(zstdPreface[:])
	}()
	_, err := NewServerTransport(server, -1)
	require.ErrorContains(t, err, "must not be negative")
	_ = client.Close()
	_ = server.Close()
}

type recordingConn struct {
	net.Conn
	written  bytes.Buffer
	writeErr error
}

func (c *recordingConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return c.written.Write(p)
}

func TestClientTransport(t *testing.T) {
	t.Run("none writes no preface", func(t *testing.T) {
		conn := &recordingConn{}
		_, err := NewClientTransport(conn, CompressionNone, DefaultCompressionConcurrency)
		require.NoError(t, err)
		require.Zero(t, conn.written.Len())
	})
	t.Run("zstd writes the preface", func(t *testing.T) {
		conn := &recordingConn{}
		_, err := NewClientTransport(conn, CompressionZstd, DefaultCompressionConcurrency)
		require.NoError(t, err)
		require.Equal(t, zstdPreface[:], conn.written.Bytes())
	})
	t.Run("zstd preface write error", func(t *testing.T) {
		conn := &recordingConn{writeErr: io.ErrClosedPipe}
		_, err := NewClientTransport(conn, CompressionZstd, DefaultCompressionConcurrency)
		require.ErrorContains(t, err, "write compression preface")
		require.True(t, errors.Is(err, io.ErrClosedPipe))
	})
	t.Run("zstd bad concurrency", func(t *testing.T) {
		conn := &recordingConn{}
		_, err := NewClientTransport(conn, CompressionZstd, -1)
		require.ErrorContains(t, err, "must not be negative")
		require.Zero(t, conn.written.Len(), "no preface must be written when the transport cannot be built")
	})
}

func TestTransportsRoundTrip(t *testing.T) {
	for _, c := range []Compression{CompressionNone, CompressionZstd} {
		t.Run(string(c), func(t *testing.T) {
			clientConn, serverConn := net.Pipe()
			t.Cleanup(func() { _ = clientConn.Close(); _ = serverConn.Close() })

			type result struct {
				got []byte
				err error
			}
			results := make(chan result, 1)
			go func() {
				st, err := NewServerTransport(serverConn, DefaultCompressionConcurrency)
				if err != nil {
					results <- result{err: err}
					return
				}
				in, err := st.RecvMessage()
				if err != nil {
					results <- result{err: err}
					return
				}
				defer in.Release()
				if in.Message().Which() != rpccp.Message_Which_bootstrap {
					results <- result{err: fmt.Errorf("unexpected message %v", in.Message().Which())}
					return
				}
				boot, err := in.Message().Bootstrap()
				if err != nil {
					results <- result{err: err}
					return
				}
				if boot.QuestionId() != 42 {
					results <- result{err: fmt.Errorf("unexpected question id %d", boot.QuestionId())}
					return
				}
				got, err := in.Message().Message().Marshal()
				results <- result{got: got, err: err}
			}()

			ct, err := NewClientTransport(clientConn, c, DefaultCompressionConcurrency)
			require.NoError(t, err)
			out, err := ct.NewMessage()
			require.NoError(t, err)
			boot, err := out.Message().NewBootstrap()
			require.NoError(t, err)
			boot.SetQuestionId(42)
			want, err := out.Message().Message().Marshal()
			require.NoError(t, err)
			require.NoError(t, out.Send())
			out.Release()

			select {
			case r := <-results:
				require.NoError(t, r.err)
				require.Equal(t, want, r.got)
			case <-time.After(5 * time.Second):
				t.Fatal("server did not receive the message")
			}
		})
	}
}
