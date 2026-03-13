// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"

	"github.com/efficientgo/core/testutil"

	deduppkg "github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"

	_ "github.com/thanos-io/thanos/pkg/extgrpc"
)

func BenchmarkGRPCServer(b *testing.B) {
	tmpDir := b.TempDir()

	const totalSamples = 1e3
	const totalSeries = 1e2

	const numOfReplicas = 1

	samplesPerSeriesPerReplica := int(totalSamples / numOfReplicas)
	if samplesPerSeriesPerReplica == 0 {
		samplesPerSeriesPerReplica = 1
	}
	seriesPerReplica := int(totalSeries / numOfReplicas)
	if seriesPerReplica == 0 {
		seriesPerReplica = 1
	}

	random := rand.New(rand.NewSource(120))
	var resps []*storepb.SeriesResponse
	for j := range numOfReplicas {
		// Note 0 argument - this is because we want to have two replicas for the same time duration.
		head, created := storetestutil.CreateHeadWithSeries(b, 0, storetestutil.HeadGenOptions{
			TSDBDir:          filepath.Join(tmpDir, fmt.Sprintf("%d", j)),
			SamplesPerSeries: samplesPerSeriesPerReplica,
			Series:           seriesPerReplica,
			Random:           random,
			PrependLabels:    labels.FromStrings("a_replica", fmt.Sprintf("%d", j)), // a_ prefix so we keep sorted order.
		})
		testutil.Ok(b, head.Close())
		for i := range created {
			resps = append(resps, storepb.NewSeriesResponse(created[i]))
		}
	}

	b.Log(len(resps))

	ss := &mockedStoreServer{responses: resps}

	g := grpcserver.New(
		log.NewNopLogger(), prometheus.NewRegistry(), opentracing.NoopTracer{}, []grpc_logging.Option{}, []string{}, component.Compact, prober.NewGRPC(),
		grpcserver.WithServer(store.RegisterStoreServer(ss, log.NewNopLogger())),
		grpcserver.WithListen("localhost:0"),
	)

	go func() {
		err := g.ListenAndServe()
		if errors.Is(err, grpc.ErrServerStopped) {
			return
		}
		testutil.Ok(b, err)
	}()

	for g.Address() == "" {
		time.Sleep(100 * time.Millisecond)
	}

	b.Cleanup(func() {
		g.Shutdown(nil)
	})

	gc, err := grpc.NewClient(g.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.UseCompressor("snappy")))
	testutil.Ok(b, err)
	b.Cleanup(func() {
		testutil.Ok(b, gc.Close())
	})
	sc := storepb.NewStoreClient(gc)

	b.Log("initializing the loop")
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		retS, err := sc.Series(context.Background(), &storepb.SeriesRequest{
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
			Matchers: []storepb.LabelMatcher{
				{
					Type:  storepb.LabelMatcher_RE,
					Name:  model.MetricNameLabel,
					Value: "a_.*",
				},
			},
		}, grpc.UseCompressor("snappy"))

		testutil.Ok(b, err)

		var got int

		for {
			_, err := retS.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				testutil.Ok(b, err)
			}
			got++
		}

		require.Equal(b, len(resps), got)
	}
}

// TestQuerySelect benchmarks querier Select method. Note that this is what PromQL is using, but PromQL might invoke
// this many times and within different interval e.g
// TODO(bwplotka): Add benchmarks with PromQL involvement.
func TestQuerySelect(t *testing.T) {
	if testing.
		Short() {
		t.Skip("too slow for testing.Short")
	}

	t.Parallel()

	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchQuerySelect(t, samplesPerSeries, series, true)
	})
}

func BenchmarkQuerySelect(b *testing.B) {
	tb := testutil.NewTB(b)
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchQuerySelect(t, samplesPerSeries, series, true)
	})
}

func benchQuerySelect(t testutil.TB, totalSamples, totalSeries int, dedup bool) {
	tmpDir := t.TempDir()

	const numOfReplicas = 2

	samplesPerSeriesPerReplica := totalSamples / numOfReplicas
	if samplesPerSeriesPerReplica == 0 {
		samplesPerSeriesPerReplica = 1
	}
	seriesPerReplica := totalSeries / numOfReplicas
	if seriesPerReplica == 0 {
		seriesPerReplica = 1
	}

	random := rand.New(rand.NewSource(120))
	var resps []*storepb.SeriesResponse
	var expectedSeries []labels.Labels
	for j := range numOfReplicas {
		// Note 0 argument - this is because we want to have two replicas for the same time duration.
		head, created := storetestutil.CreateHeadWithSeries(t, 0, storetestutil.HeadGenOptions{
			TSDBDir:          filepath.Join(tmpDir, fmt.Sprintf("%d", j)),
			SamplesPerSeries: samplesPerSeriesPerReplica,
			Series:           seriesPerReplica,
			Random:           random,
			PrependLabels:    labels.FromStrings("a_replica", fmt.Sprintf("%d", j)), // a_ prefix so we keep sorted order.
		})
		testutil.Ok(t, head.Close())
		for i := range created {
			if !dedup || j == 0 {
				lset := labelpb.ZLabelsToPromLabels(created[i].Labels).Copy()
				if dedup {
					lset = lset.MatchLabels(false, "a_replica")
				}
				expectedSeries = append(expectedSeries, lset)
			}

			resps = append(resps, storepb.NewSeriesResponse(created[i]))
		}

	}

	logger := log.NewNopLogger()
	q := newQuerier(
		logger,
		math.MinInt64,
		math.MaxInt64,
		deduppkg.AlgorithmPenalty,
		[]string{"a_replica"},
		nil,
		newProxyStore(&mockedStoreServer{responses: resps}),
		dedup,
		0,
		false,
		false,
		gate.NewNoop(),
		10*time.Second,
		nil,
		NoopSeriesStatsReporter,
		1,
	)
	testSelect(t, q, expectedSeries)
}

type mockedStoreServer struct {
	storepb.StoreServer

	responses []*storepb.SeriesResponse
}

func (m *mockedStoreServer) Series(_ *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	for _, r := range m.responses {
		if err := server.Send(r); err != nil {
			return err
		}
	}
	return nil
}

var (
	testT    int64
	testV    float64
	testLset labels.Labels
)

func testSelect(t testutil.TB, q *querier, expectedSeries []labels.Labels) {
	t.Run("select", func(t testutil.TB) {
		t.ResetTimer()

		ctx := context.Background()
		for i := 0; i < t.N(); i++ {
			ss := q.Select(ctx, true, nil, &labels.Matcher{Value: "foo", Name: "bar", Type: labels.MatchEqual})
			testutil.Ok(t, ss.Err())
			testutil.Equals(t, 0, len(ss.Warnings()))

			if t.IsBenchmark() {
				var gotSeriesCount int
				for ss.Next() {
					s := ss.At()
					testLset = s.Labels()
					gotSeriesCount++

					// This is when resource usage should actually start growing.
					iter := s.Iterator(nil)
					for iter.Next() != chunkenc.ValNone {
						testT, testV = iter.At()
					}
					testutil.Ok(t, iter.Err())
				}

				testutil.Equals(t, len(expectedSeries), gotSeriesCount)
				testutil.Ok(t, ss.Err())
				return
			}

			// Check more carefully.
			var gotSeries []labels.Labels
			for ss.Next() {
				s := ss.At()
				gotSeries = append(gotSeries, s.Labels())

				iter := s.Iterator(nil)
				for iter.Next() != chunkenc.ValNone {
					testT, testV = iter.At()
				}
				testutil.Ok(t, iter.Err())
			}
			testutil.Equals(t, expectedSeries, gotSeries)
			testutil.Ok(t, ss.Err())
		}
	})
}

// batchingMockedStoreServer batches series responses before sending.
type batchingMockedStoreServer struct {
	storepb.StoreServer

	responses []*storepb.SeriesResponse
	batchSize int
}

func (m *batchingMockedStoreServer) Series(_ *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	var batch []*storepb.Series

	for _, r := range m.responses {
		series := r.GetSeries()
		if series == nil {
			if len(batch) > 0 {
				resp := storepb.NewBatchResponse(batch)
				if err := server.Send(resp); err != nil {
					return err
				}
				batch = batch[:0]
			}
			if err := server.Send(r); err != nil {
				return err
			}
			continue
		}

		batch = append(batch, series)

		if len(batch) >= m.batchSize {
			resp := storepb.NewBatchResponse(batch)
			if err := server.Send(resp); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		resp := storepb.NewBatchResponse(batch)
		if err := server.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func BenchmarkGRPCServerBatching(b *testing.B) {
	seriesCounts := []int{100, 1000, 10000, 100000}
	samplesPerSeriesCounts := []int{100, 240}
	batchSizes := []int{1, 10, 100, 1000, 10000}
	compressors := []string{"snappy"}

	for _, seriesCount := range seriesCounts {
		b.Run(fmt.Sprintf("series=%v", seriesCount), func(b *testing.B) {
			for _, samplesPerSeriesCount := range samplesPerSeriesCounts {
				b.Run(fmt.Sprintf("sample_count_per_series=%v", samplesPerSeriesCount), func(b *testing.B) {
					for _, batchSize := range batchSizes {
						b.Run(fmt.Sprintf("batch_size=%v", batchSize), func(b *testing.B) {
							for _, compressor := range compressors {
								b.Run(fmt.Sprintf("compressor=%v", compressor), func(b *testing.B) {
									b.Log("initializing the loop")
									b.ResetTimer()
									b.ReportAllocs()
									benchmarkGRPCServerBatching(b, seriesCount, samplesPerSeriesCount, batchSize, compressor)
								})
							}
						})
					}
				})
			}
		})
	}
}

func benchmarkGRPCServerBatching(b *testing.B, seriesCount, samplesPerSeries, batchSize int, compressor string) {
	tmpDir := b.TempDir()

	random := rand.New(rand.NewSource(120))
	var resps []*storepb.SeriesResponse

	// Use realistic, repetitive labels that simulate real-world Prometheus metrics.
	head, created := storetestutil.CreateHeadWithSeries(b, 0, storetestutil.HeadGenOptions{
		TSDBDir:          tmpDir,
		SamplesPerSeries: samplesPerSeries,
		Series:           seriesCount,
		Random:           random,
		PrependLabels: labels.FromStrings(
			"a_replica", fmt.Sprintf("%d", 0), // a_ prefix so we keep sorted order.
			"__name__", "http_requests_total",
			"cluster", "us-east-1",
			"instance", "localhost:9090",
			"job", "prometheus",
			"namespace", "default",
			"replica", "0",
		),
	})
	testutil.Ok(b, head.Close())
	for i := range created {
		resps = append(resps, storepb.NewSeriesResponse(created[i]))
	}

	var ss storepb.StoreServer
	if batchSize == 1 {
		ss = &mockedStoreServer{responses: resps}
	} else {
		ss = &batchingMockedStoreServer{responses: resps, batchSize: batchSize}
	}

	g := grpcserver.New(
		log.NewNopLogger(), prometheus.NewRegistry(), opentracing.NoopTracer{}, []grpc_logging.Option{}, []string{}, component.Compact, prober.NewGRPC(),
		grpcserver.WithServer(store.RegisterStoreServer(ss, log.NewNopLogger())),
		grpcserver.WithListen("localhost:0"),
	)

	go func() {
		err := g.ListenAndServe()
		if errors.Is(err, grpc.ErrServerStopped) {
			return
		}
		testutil.Ok(b, err)
	}()

	for g.Address() == "" {
		time.Sleep(100 * time.Millisecond)
	}

	b.Cleanup(func() {
		g.Shutdown(nil)
	})

	gc, err := grpc.NewClient(g.Address(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.UseCompressor(compressor),
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
		),
	)
	testutil.Ok(b, err)
	b.Cleanup(func() {
		testutil.Ok(b, gc.Close())
	})
	sc := storepb.NewStoreClient(gc)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		retS, err := sc.Series(context.Background(), &storepb.SeriesRequest{
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
			Matchers: []storepb.LabelMatcher{
				{
					Type:  storepb.LabelMatcher_RE,
					Name:  model.MetricNameLabel,
					Value: "a_.*",
				},
			},
		}, grpc.UseCompressor(compressor))

		testutil.Ok(b, err)

		var got int

		for {
			resp, err := retS.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				testutil.Ok(b, err)
			}
			// Count series from both single series responses and batch responses
			if series := resp.GetSeries(); series != nil {
				got++
			} else if batch := resp.GetBatch(); batch != nil {
				got += len(batch.Series)
			}
		}

		require.Equal(b, len(resps), got)
	}

}
