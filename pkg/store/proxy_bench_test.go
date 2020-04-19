// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

//
//import (
//	"fmt"
//	"math"
//	"math/rand"
//	"testing"
//
//	"github.com/go-kit/kit/log"
//	"github.com/thanos-io/thanos/pkg/store/storepb"
//	"github.com/thanos-io/thanos/pkg/testutil"
//	"github.com/thanos-io/thanos/pkg/testutil/benchutil"
//)
//
//func TestProxySeries(t *testing.T) {
//	tb := testutil.NewTB(t)
//	tb.Run(benchutil.OneSampleSeriesSubTestName(200e3), func(tb testutil.TB) {
//		benchProxySeries(tb, 200e3, benchutil.SeriesDimension)
//	})
//	tb.Run(benchutil.OneSeriesManySamplesSubTestName(200e3), func(tb testutil.TB) {
//		benchProxySeries(tb, 200e3, benchutil.SamplesDimension)
//	})
//}
//
//func BenchmarkProxySeries(b *testing.B) {
//	tb := testutil.NewTB(b)
//	tb.Run(benchutil.OneSampleSeriesSubTestName(10e6), func(tb testutil.TB) {
//		benchProxySeries(tb, 10e6, benchutil.SeriesDimension)
//	})
//	tb.Run(benchutil.OneSeriesManySamplesSubTestName(100e6), func(tb testutil.TB) {
//		// 100e6 samples = ~17361 days with 15s scrape.
//		benchProxySeries(tb, 100e6, benchutil.SamplesDimension)
//	})
//}
//
//func benchProxySeries(t testutil.TB, number int, dimension benchutil.Dimension) {
//	const numOfClients = 4
//
//	var (
//		numberPerClient = number / 4
//		random          = rand.New(rand.NewSource(120))
//	)
//
//	// Build numOfClients of clients.
//	clients := make([]Client, numOfClients)
//
//	for j := range clients {
//		var resps []*storepb.SeriesResponse
//
//		switch dimension {
//		case benchutil.SeriesDimension:
//			fmt.Println("Building client with numSeries:", numberPerClient)
//
//			h, created := benchutil.CreateSeriesWithOneSample(t, j, numberPerClient)
//			testutil.Ok(t, h.Close())
//
//			for i := 0; i < len(created); i++ {
//				resps = append(resps, storepb.NewSeriesResponse(&created[i]))
//			}
//
//			clients[j] = &testClient{
//				StoreClient: &mockedStoreAPI{
//					RespSeries: resps,
//				},
//				minTime: math.MinInt64,
//				maxTime: math.MaxInt64,
//			}
//		case benchutil.SamplesDimension:
//			fmt.Println("Building client with one series with numSamples:", numberPerClient)
//
//			func() {
//				h := benchutil.CreateOneSeriesWithManySamples(t, j, numberPerClient, random)
//				defer h.Close()
//
//				clients[j] = &testClient{
//					StoreClient: &mockedStoreAPI{
//						RespSeries: resps,
//					},
//					minTime: math.MinInt64,
//					maxTime: math.MaxInt64,
//				}
//				testutil.Ok(t, h.Close())
//			}()
//
//		default:
//			t.Fatal("unknown dimension", dimension)
//		}
//	}
//
//	logger := log.NewNopLogger()
//	store := &ProxyStore{
//		logger:          logger,
//		stores:          func() []Client { return clients },
//		metrics:         newProxyStoreMetrics(nil),
//		responseTimeout: 0,
//	}
//
//	var resps []*storepb.SeriesResponse
//	var expected []storepb.Series
//	lastLabels := storepb.Series{}
//	for _, c := range clients {
//		m := c.(*testClient).StoreClient.(*mockedStoreAPI)
//
//		for _, r := range m.RespSeries {
//			resps = append(resps, r)
//
//			// Proxy will merge all series with same labels without limit (https://github.com/thanos-io/thanos/issues/2332).
//			// Let's do this here as well.
//			x := storepb.Series{Labels: r.GetSeries().Labels}
//			if x.String() == lastLabels.String() {
//				expected[len(expected)-1].Chunks = append(expected[len(expected)-1].Chunks, r.GetSeries().Chunks...)
//				continue
//			}
//			lastLabels = x
//			expected = append(expected, *r.GetSeries())
//		}
//
//	}
//
//	chunkLen := len(resps[len(resps)-1].GetSeries().Chunks)
//	maxTime := resps[len(resps)-1].GetSeries().Chunks[chunkLen-1].MaxTime
//	benchmarkSeries(t, store,
//		&benchSeriesCase{
//			name: fmt.Sprintf("%d of client with %d each, total %d", numOfClients, numberPerClient, number),
//			req: &storepb.SeriesRequest{
//				MinTime: 0,
//				MaxTime: maxTime,
//				Matchers: []storepb.LabelMatcher{
//					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
//				},
//			},
//			expected: expected,
//		},
//	)
//
//	// Change client to just one.
//	store.stores = func() []Client {
//		return []Client{&testClient{
//			StoreClient: &mockedStoreAPI{
//				// All responses.
//				RespSeries: resps,
//			},
//			labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext1", Value: "1"}}}},
//			minTime:   math.MinInt64,
//			maxTime:   math.MaxInt64,
//		}}
//	}
//
//	// In this we expect exactly the same response as input.
//	expected = expected[:0]
//	for _, r := range resps {
//		expected = append(expected, *r.GetSeries())
//	}
//	benchmarkSeries(t, store,
//		&benchSeriesCase{
//			name: fmt.Sprintf("single client with %d", number),
//			req: &storepb.SeriesRequest{
//				MinTime: 0,
//				MaxTime: maxTime,
//				Matchers: []storepb.LabelMatcher{
//					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
//				},
//			},
//			expected: expected,
//		},
//	)
//}
