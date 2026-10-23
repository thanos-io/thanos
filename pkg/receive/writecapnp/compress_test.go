// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"capnproto.org/go/capnp/v3"
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

type nopCloser struct{ *bytes.Buffer }

func (nopCloser) Close() error { return nil }

func TestZstdCodecRoundTrip(t *testing.T) {
	wr, err := Build("tenant", wireSeries(500))
	require.NoError(t, err)
	want, err := wr.Message().Marshal()
	require.NoError(t, err)

	var buf bytes.Buffer
	z := newZstdCodec(bufio.NewReader(&buf), nopCloser{&buf})
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
		z := newZstdCodec(bufio.NewReader(&buf), nopCloser{&buf})
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
	wr, err := Build("tenant", wireSeries(20000))
	require.NoError(t, err)
	want, err := wr.Message().Marshal()
	require.NoError(t, err)
	require.Greater(t, len(want), 1<<20)

	var buf bytes.Buffer
	z := newZstdCodec(bufio.NewReader(&buf), nopCloser{&buf})
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
	z := newZstdCodec(bufio.NewReader(&buf), nopCloser{&buf})
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for range perWorker {
				require.NoError(t, z.Encode(reqs[w].Message()))
			}
		})
	}
	wg.Wait()

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
	valid := frame(zstdEncoder().EncodeAll(raw, nil))

	for _, tc := range []struct {
		name string
		data []byte
		err  string
	}{
		{"length over limit", binary.AppendUvarint(nil, maxFrameSize+1), "exceeds"},
		{"truncated frame", valid[:len(valid)-1], "unexpected EOF"},
		{"not zstd", frame([]byte("not a zstd frame")), "decompress message"},
		{"decompressed size over limit", frame(zstdEncoder().EncodeAll(make([]byte, maxFrameSize+1), nil)), "decompress message"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			z := newZstdCodec(bufio.NewReader(bytes.NewReader(tc.data)), nopCloser{&bytes.Buffer{}})
			_, err := z.Decode()
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestServerTransportPrefaceShortRead(t *testing.T) {
	client, server := net.Pipe()
	go func() {
		_, _ = client.Write(zstdPreface[:2])
		_ = client.Close()
	}()
	_, err := NewServerTransport(server)
	require.Error(t, err)
	_ = server.Close()
}
