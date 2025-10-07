// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func BenchmarkCapNProtoServer_SingleConcurrentClient(b *testing.B) {
	wreq := storepb.WriteRequest{
		Tenant: "example-tenant",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up",
					"job":      "prometheus",
				}),
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 1},
					{Timestamp: 2, Value: 2},
				},
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up",
					"job":      "thanos",
				}),
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
		handler  = NewCapNProtoHandler(log.NewNopLogger(), writer)
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
