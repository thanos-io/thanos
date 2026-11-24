// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func BenchmarkCapNProtoServer_SingleConcurrentClient(b *testing.B) {
	wreq := storepb.WriteRequest{
		Tenant: "example-tenant",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "up"},
					{Name: "job", Value: "prometheus"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 1},
					{Timestamp: 2, Value: 2},
				},
			},
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "up"},
					{Name: "job", Value: "thanos"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 3, Value: 3},
					{Timestamp: 4, Value: 4},
				},
			},
		},
	}

	var (
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener = bufconn.Listen(1024)
		handler  = NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer, newNopLimiter(b))
		srv      = NewCapNProtoServer(listener, handler, log.NewNopLogger())
	)
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown()

	const numIterations = 10000
	var totalWrites float64

	b.ReportAllocs()
	client := writecapnp.NewRemoteWriteClient(listener, log.NewLogfmtLogger(os.Stdout))
	for b.Loop() {
		for range numIterations {
			_, err := client.RemoteWrite(context.Background(), &wreq)
			require.NoError(b, err)
		}
		totalWrites += numIterations
	}
	require.NoError(b, client.Close())
	require.NoError(b, listener.Close())
	b.ReportMetric(totalWrites, "total_writes")
}

type nopAppender struct {
	*fakeAppender
}

func (nopAppender) Append(ref storage.SeriesRef, l labels.Labels, _ int64, _ float64) (storage.SeriesRef, error) {
	if ref == 0 {
		ref = storage.SeriesRef(l.Hash())
	}
	return ref, nil
}

func BenchmarkCapNProtoServer_MessageSize(b *testing.B) {
	for _, series := range []int{1000, 2500, 6000} {
		wreq := storepb.WriteRequest{Tenant: "example-tenant"}
		for i := range series {
			lbls := []labelpb.ZLabel{{Name: "__name__", Value: fmt.Sprintf("metric_%d", i%50)}}
			for j := range 9 {
				lbls = append(lbls, labelpb.ZLabel{Name: fmt.Sprintf("label_%d", j), Value: fmt.Sprintf("value-%d-%d", i, j)})
			}
			wreq.Timeseries = append(wreq.Timeseries, prompb.TimeSeries{Labels: lbls, Samples: []prompb.Sample{{Timestamp: int64(i), Value: float64(i)}}})
		}
		size, err := writecapnp.Marshal(wreq.Tenant, wreq.Timeseries)
		require.NoError(b, err)

		b.Run(fmt.Sprintf("message=%dKiB", len(size)>>10), func(b *testing.B) {
			var (
				writer = NewCapNProtoWriter(
					log.NewNopLogger(),
					newFakeTenantAppendable(&fakeAppendable{appender: nopAppender{newFakeAppender(nil, nil, nil)}}),
					&CapNProtoWriterOptions{},
				)
				listener = bufconn.Listen(1 << 20)
				handler  = NewCapNProtoHandler(prometheus.NewRegistry(), log.NewNopLogger(), writer, newNopLimiter(b))
				srv      = NewCapNProtoServer(listener, handler, log.NewNopLogger())
			)
			go func() { _ = srv.ListenAndServe() }()
			defer srv.Shutdown()

			client := writecapnp.NewRemoteWriteClient(listener, log.NewNopLogger())
			_, err := client.RemoteWrite(context.Background(), &wreq)
			require.NoError(b, err)
			b.ReportAllocs()
			for b.Loop() {
				_, err := client.RemoteWrite(context.Background(), &wreq)
				require.NoError(b, err)
			}
			require.NoError(b, client.Close())
			require.NoError(b, listener.Close())
		})
	}
}
