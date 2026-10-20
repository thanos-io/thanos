// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// scribblingPool overwrites every buffer returned to it, so reads after release see garbage.
type scribblingPool struct {
	puts int
}

func (p *scribblingPool) Get(length int) *[]byte {
	b := make([]byte, length)
	return &b
}

func (p *scribblingPool) Put(b *[]byte) {
	p.puts++
	for i := range *b {
		(*b)[i] = 0xAA
	}
}

func setMaterializePoolForTesting(p mem.BufferPool) func() {
	prev := materializePool
	materializePool = p
	return func() { materializePool = prev }
}

func testWriteRequest() *storepb.WriteRequest {
	req := &storepb.WriteRequest{Tenant: "tenant-a", Replica: 1}
	for i := range 50 {
		req.Timeseries = append(req.Timeseries, prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{Name: "__name__", Value: fmt.Sprintf("metric_%d", i)},
				{Name: "instance", Value: fmt.Sprintf("host-%d:9100", i)},
				{Name: "job", Value: "node"},
			},
			Samples: []prompb.Sample{{Value: float64(i), Timestamp: int64(i)}},
			Exemplars: []prompb.Exemplar{{
				Labels:    []labelpb.ZLabel{{Name: "trace_id", Value: fmt.Sprintf("trace-%d", i)}},
				Value:     1,
				Timestamp: int64(i),
			}},
		})
	}
	return req
}

func splitInto(data []byte, n int, pool mem.BufferPool) mem.BufferSlice {
	var s mem.BufferSlice
	chunk := (len(data) + n - 1) / n
	for off := 0; off < len(data); off += chunk {
		end := min(off+chunk, len(data))
		b := pool.Get(end - off)
		copy(*b, data[off:end])
		s = append(s, mem.NewBuffer(b, pool))
	}
	return s
}

func TestCodecUnmarshalSurvivesBufferReuse(t *testing.T) {
	want := testWriteRequest()
	raw, err := want.Marshal()
	require.NoError(t, err)

	for _, buffers := range []int{1, 4} {
		t.Run(fmt.Sprintf("buffers=%d", buffers), func(t *testing.T) {
			pool := &scribblingPool{}
			restore := setMaterializePoolForTesting(pool)
			defer restore()

			codec := encoding.GetCodecV2("proto")
			data := splitInto(raw, buffers, pool)

			got := &storepb.WriteRequest{}
			require.NoError(t, codec.Unmarshal(data, got))
			data.Free()

			require.Equal(t, want.Tenant, got.Tenant)
			require.Len(t, got.Timeseries, len(want.Timeseries))
			for i := range want.Timeseries {
				require.Equal(t, want.Timeseries[i].Labels, got.Timeseries[i].Labels, "series %d", i)
				require.Equal(t, want.Timeseries[i].Exemplars[0].Labels, got.Timeseries[i].Exemplars[0].Labels, "exemplar %d", i)
			}
		})
	}
}

func requireSameSeries(t *testing.T, want, got *storepb.WriteRequest) {
	t.Helper()
	require.Len(t, got.Timeseries, len(want.Timeseries))
	for i := range want.Timeseries {
		require.Equal(t, want.Timeseries[i].Labels, got.Timeseries[i].Labels, "series %d", i)
		require.Equal(t, want.Timeseries[i].Exemplars[0].Labels, got.Timeseries[i].Exemplars[0].Labels, "exemplar %d", i)
	}
}

func retainedBuffersCount(c *retainingCodec) int {
	n := 0
	c.buffers.Range(func(_, _ any) bool { n++; return true })
	return n
}

func TestCodecRetainedBufferLifetime(t *testing.T) {
	want := testWriteRequest()
	raw, err := want.Marshal()
	require.NoError(t, err)

	for _, buffers := range []int{1, 4} {
		t.Run(fmt.Sprintf("buffers=%d", buffers), func(t *testing.T) {
			pool := &scribblingPool{}
			codec := newRetainingCodec(&storepb.WriteRequest{})
			codec.pool = pool
			data := splitInto(raw, buffers, pool)

			got := &storepb.WriteRequest{}
			require.NoError(t, codec.Unmarshal(data, got))
			data.Free()
			// A single buffer is used in place, several are copied and freed.
			putsAfterDecode := pool.puts
			if buffers == 1 {
				require.Equal(t, 0, putsAfterDecode)
			} else {
				require.Equal(t, buffers, putsAfterDecode)
			}
			require.Equal(t, 1, retainedBuffersCount(codec))

			_, err := codec.releaseInterceptor(context.Background(), got, &grpc.UnaryServerInfo{}, func(_ context.Context, req any) (any, error) {
				requireSameSeries(t, want, req.(*storepb.WriteRequest))
				return nil, nil
			})
			require.NoError(t, err)

			require.Equal(t, putsAfterDecode+1, pool.puts, "retained buffer must return to the pool after the handler")
			require.Equal(t, 0, retainedBuffersCount(codec))
		})
	}
}

func TestCodecRetainedUnmarshalError(t *testing.T) {
	raw, err := testWriteRequest().Marshal()
	require.NoError(t, err)

	pool := &scribblingPool{}
	codec := newRetainingCodec(&storepb.WriteRequest{})
	codec.pool = pool

	data := splitInto(raw[:len(raw)/2], 1, pool)
	require.Error(t, codec.Unmarshal(data, &storepb.WriteRequest{}))
	data.Free()

	require.Equal(t, 1, pool.puts)
	require.Equal(t, 0, retainedBuffersCount(codec))
}

func TestCodecRetainsOnlyRegisteredTypes(t *testing.T) {
	raw, err := (&storepb.SeriesRequest{MinTime: 1, MaxTime: 2}).Marshal()
	require.NoError(t, err)

	codec := newRetainingCodec(&storepb.WriteRequest{})
	got := &storepb.SeriesRequest{}
	require.NoError(t, codec.Unmarshal(splitInto(raw, 1, mem.DefaultBufferPool()), got))
	require.Equal(t, int64(2), got.MaxTime)
	require.Equal(t, 0, retainedBuffersCount(codec))
}

func BenchmarkCodecUnmarshalWriteRequest(b *testing.B) {
	raw, err := testWriteRequest().Marshal()
	require.NoError(b, err)
	pool := mem.DefaultBufferPool()
	noop := func(context.Context, any) (any, error) { return nil, nil }

	for _, retain := range []bool{false, true} {
		b.Run(fmt.Sprintf("retain=%v", retain), func(b *testing.B) {
			codec := encoding.GetCodecV2("proto")
			interceptor := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				return handler(ctx, req)
			}
			if retain {
				c := newRetainingCodec(&storepb.WriteRequest{})
				codec, interceptor = c, c.releaseInterceptor
			}
			b.ReportAllocs()
			for b.Loop() {
				data := splitInto(raw, 1, pool)
				req := &storepb.WriteRequest{}
				if err := codec.Unmarshal(data, req); err != nil {
					b.Fatal(err)
				}
				data.Free()
				if _, err := interceptor(context.Background(), req, &grpc.UnaryServerInfo{}, noop); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
