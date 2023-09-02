// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"testing"
	"time"

	"github.com/cespare/xxhash"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/stringset"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestPrometheusStore_Series_e2e(t *testing.T) {
	testPrometheusStoreSeriesE2e(t, "")
}

// Regression test for https://github.com/thanos-io/thanos/issues/478.
func TestPrometheusStore_Series_promOnPath_e2e(t *testing.T) {
	testPrometheusStoreSeriesE2e(t, "/prometheus/sub/path")
}

func testPrometheusStoreSeriesE2e(t *testing.T, prefix string) {
	defer custom.TolerantVerifyLeak(t)

	p, err := e2eutil.NewPrometheusOnPath(prefix)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	// region is an external label; by adding it as an internal label too we also trigger
	// the resorting code paths
	a := p.Appender()
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "local"), baseT+100, 1)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "local"), baseT+200, 2)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "local"), baseT+300, 3)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	limitMinT := int64(0)
	proxy, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("region", "eu-west") },
		func() (int64, int64) { return limitMinT, -1 },
		func() stringset.Set { return stringset.AllStrings() },
		nil,
	) // MaxTime does not matter.
	testutil.Ok(t, err)

	// Query all three samples except for the first one. Since we round up queried data
	// to seconds, we can test whether the extra sample gets stripped properly.
	{
		srv := newStoreSeriesServer(ctx)
		testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
			MinTime: baseT + 101,
			MaxTime: baseT + 300,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			},
		}, srv))

		testutil.Equals(t, 1, len(srv.SeriesSet))

		testutil.Equals(t, []labelpb.ZLabel{
			{Name: "a", Value: "b"},
			{Name: "region", Value: "eu-west"},
		}, srv.SeriesSet[0].Labels)
		testutil.Equals(t, []string(nil), srv.Warnings)
		testutil.Equals(t, 1, len(srv.SeriesSet[0].Chunks))

		c := srv.SeriesSet[0].Chunks[0]
		testutil.Equals(t, storepb.Chunk_XOR, c.Raw.Type)

		chk, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		testutil.Ok(t, err)

		samples := expandChunk(chk.Iterator(nil))
		testutil.Equals(t, []sample{{baseT + 200, 2}, {baseT + 300, 3}}, samples)

	}
	// Query all samples, but limit mint time to exclude the first one.
	{
		limitMinT = baseT + 101
		srv := newStoreSeriesServer(ctx)
		testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: baseT + 300,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			},
		}, srv))
		// Revert for next cases.
		limitMinT = 0

		testutil.Equals(t, 1, len(srv.SeriesSet))

		testutil.Equals(t, []labelpb.ZLabel{
			{Name: "a", Value: "b"},
			{Name: "region", Value: "eu-west"},
		}, srv.SeriesSet[0].Labels)

		testutil.Equals(t, 1, len(srv.SeriesSet[0].Chunks))

		c := srv.SeriesSet[0].Chunks[0]
		testutil.Equals(t, []string(nil), srv.Warnings)
		testutil.Equals(t, storepb.Chunk_XOR, c.Raw.Type)

		chk, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		testutil.Ok(t, err)

		samples := expandChunk(chk.Iterator(nil))
		testutil.Equals(t, []sample{{baseT + 200, 2}, {baseT + 300, 3}}, samples)
	}
	// Querying by external labels only.
	{
		srv := newStoreSeriesServer(ctx)

		err = proxy.Series(&storepb.SeriesRequest{
			MinTime: baseT + 101,
			MaxTime: baseT + 300,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
			},
		}, srv)
		testutil.NotOk(t, err)
		testutil.Equals(t, []string(nil), srv.Warnings)
		testutil.Equals(t, "rpc error: code = InvalidArgument desc = no matchers specified (excluding external labels)", err.Error())
	}
	// Querying with pushdown.
	{
		srv := newStoreSeriesServer(ctx)
		testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
			MinTime: baseT + 101,
			MaxTime: baseT + 300,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			},
			QueryHints: &storepb.QueryHints{Func: &storepb.Func{Name: "min_over_time"}, Range: &storepb.Range{Millis: 300}},
		}, srv))

		testutil.Equals(t, 1, len(srv.SeriesSet))

		testutil.Equals(t, []labelpb.ZLabel{
			{Name: "a", Value: "b"},
			{Name: "region", Value: "eu-west"},
			{Name: "__thanos_pushed_down", Value: "true"},
		}, srv.SeriesSet[0].Labels)
		testutil.Equals(t, []string(nil), srv.Warnings)
		testutil.Equals(t, 1, len(srv.SeriesSet[0].Chunks))

		c := srv.SeriesSet[0].Chunks[0]
		testutil.Equals(t, storepb.Chunk_XOR, c.Raw.Type)

		chk, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		testutil.Ok(t, err)

		samples := expandChunk(chk.Iterator(nil))
		testutil.Equals(t, []sample{{baseT + 300, 1}}, samples)

	}
}

type sample struct {
	t int64
	v float64
}

func expandChunk(cit chunkenc.Iterator) (res []sample) {
	for cit.Next() != chunkenc.ValNone {
		t, v := cit.At()
		res = append(res, sample{t, v})
	}
	return res
}

func TestPrometheusStore_SeriesLabels_e2e(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	a := p.Appender()
	_, err = a.Append(0, labels.FromStrings("a", "b", "b", "d"), baseT+100, 1)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "c", "b", "d", "job", "test"), baseT+200, 2)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "d", "b", "d", "job", "test"), baseT+300, 3)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("b", "d", "job", "test"), baseT+400, 4)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	promStore, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("region", "eu-west") },
		func() (int64, int64) { return math.MinInt64/1000 + 62135596801, math.MaxInt64/1000 - 62135596801 },
		func() stringset.Set { return stringset.AllStrings() },
		nil,
	)
	testutil.Ok(t, err)

	for _, tcase := range []struct {
		req         *storepb.SeriesRequest
		expected    []storepb.Series
		expectedErr error
	}{
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers:   []storepb.LabelMatcher{},
				MinTime:    baseT - 10000000000,
				MaxTime:    baseT + 10000000000,
			},
			expectedErr: errors.New("rpc error: code = InvalidArgument desc = no matchers specified (excluding external labels)"),
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "wrong-chars-in-label-name(hyphen)", Value: "adsf"},
				},
				MinTime: baseT - 10000000000,
				MaxTime: baseT + 10000000000,
			},
			expectedErr: errors.New("rpc error: code = InvalidArgument desc = expected 2xx response, got 400. Body: {\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid parameter \\\"match[]\\\": 1:7: parse error: unexpected character inside braces: '-'\"}"),
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "non_existing", Value: "something"},
				},
				MinTime: baseT - 10000000000,
				MaxTime: baseT + 10000000000,
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
				},
				MinTime: baseT,
				MaxTime: baseT + 300,
			},
			expected: []storepb.Series{
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "b"}, {Name: "b", Value: "d"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "foo"},
				},
				MinTime: baseT,
				MaxTime: baseT + 300,
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NEQ, Name: "a", Value: "b"},
					{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
				},
				MinTime: baseT,
				MaxTime: baseT + 300,
			},
			expected: []storepb.Series{
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "c"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "d"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
				},
				MinTime: baseT,
				MaxTime: baseT + 300,
			},
			expected: []storepb.Series{
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "c"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "d"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
				},
				MinTime: baseT + 400,
				MaxTime: baseT + 400,
			},
			expected: []storepb.Series{
				{
					Labels: []labelpb.ZLabel{{Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
		{
			req: &storepb.SeriesRequest{
				SkipChunks: true,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "test"},
				},
				MinTime: func() int64 { minTime, _ := promStore.timestamps(); return minTime }(),
				MaxTime: func() int64 { _, maxTime := promStore.timestamps(); return maxTime }(),
			},
			expected: []storepb.Series{
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "c"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
				{
					Labels: []labelpb.ZLabel{{Name: "a", Value: "d"}, {Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
				{
					Labels: []labelpb.ZLabel{{Name: "b", Value: "d"}, {Name: "job", Value: "test"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			srv := newStoreSeriesServer(ctx)
			err = promStore.Series(tcase.req, srv)
			if tcase.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.expectedErr.Error(), err.Error())
				return
			}
			testutil.Ok(t, err)
			testutil.Equals(t, []string(nil), srv.Warnings)
			testutil.Equals(t, tcase.expected, srv.SeriesSet)
		})
	}
}

func TestPrometheusStore_Series_MatchExternalLabel(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	a := p.Appender()
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "eu-west"), baseT+100, 1)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "eu-west"), baseT+200, 2)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b", "region", "eu-west"), baseT+300, 3)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("region", "eu-west") },
		func() (int64, int64) { return 0, math.MaxInt64 },
		func() stringset.Set { return stringset.AllStrings() },
		nil)
	testutil.Ok(t, err)
	srv := newStoreSeriesServer(ctx)

	testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
		MinTime: baseT + 101,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
		},
	}, srv))
	testutil.Equals(t, 1, len(srv.SeriesSet))

	testutil.Equals(t, []labelpb.ZLabel{
		{Name: "a", Value: "b"},
		{Name: "region", Value: "eu-west"},
	}, srv.SeriesSet[0].Labels)

	srv = newStoreSeriesServer(ctx)
	// However, it should not match wrong external label.
	testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
		MinTime: baseT + 101,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west2"}, // Non existing label value.
		},
	}, srv))

	// No series.
	testutil.Equals(t, 0, len(srv.SeriesSet))
}

func TestPrometheusStore_Series_ChunkHashCalculation_Integration(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	a := p.Appender()
	_, err = a.Append(0, labels.FromStrings("a", "b"), baseT+100, 1)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b"), baseT+200, 2)
	testutil.Ok(t, err)
	_, err = a.Append(0, labels.FromStrings("a", "b"), baseT+300, 3)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("region", "eu-west") },
		func() (int64, int64) { return 0, math.MaxInt64 },
		func() stringset.Set { return stringset.AllStrings() },
		nil)
	testutil.Ok(t, err)
	srv := newStoreSeriesServer(ctx)

	testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
		MinTime: baseT + 101,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Name: "a", Value: "b"},
			{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
		},
	}, srv))
	testutil.Equals(t, 1, len(srv.SeriesSet))

	for _, chunk := range srv.SeriesSet[0].Chunks {
		got := chunk.Raw.Hash
		want := xxhash.Sum64(chunk.Raw.Data)
		testutil.Equals(t, want, got)
	}
}

func TestPrometheusStore_Info(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), nil, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("region", "eu-west") },
		func() (int64, int64) { return 123, 456 },
		func() stringset.Set { return stringset.AllStrings() },
		nil)
	testutil.Ok(t, err)

	resp, err := proxy.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, []labelpb.ZLabel{{Name: "region", Value: "eu-west"}}, resp.Labels)
	testutil.Equals(t, storepb.StoreType_SIDECAR, resp.StoreType)
	testutil.Equals(t, int64(123), resp.MinTime)
	testutil.Equals(t, int64(456), resp.MaxTime)
}

func testSeries_SplitSamplesIntoChunksWithMaxSizeOf120(t *testing.T, appender storage.Appender, newStore func() storepb.StoreServer) {
	baseT := timestamp.FromTime(time.Now().AddDate(0, 0, -2)) / 1000 * 1000

	offset := int64(2*math.MaxUint16 + 5)
	for i := int64(0); i < offset; i++ {
		_, err := appender.Append(0, labels.FromStrings("a", "b", "region", "eu-west"), baseT+i, 1)
		testutil.Ok(t, err)
	}

	testutil.Ok(t, appender.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := newStore()
	srv := newStoreSeriesServer(ctx)

	testutil.Ok(t, client.Series(&storepb.SeriesRequest{
		MinTime: baseT,
		MaxTime: baseT + offset,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
		},
	}, srv))

	testutil.Equals(t, 1, len(srv.SeriesSet))

	firstSeries := srv.SeriesSet[0]

	testutil.Equals(t, []labelpb.ZLabel{
		{Name: "a", Value: "b"},
		{Name: "region", Value: "eu-west"},
	}, firstSeries.Labels)

	testutil.Equals(t, 1093, len(firstSeries.Chunks))

	chunk, err := chunkenc.FromData(chunkenc.EncXOR, firstSeries.Chunks[0].Raw.Data)
	testutil.Ok(t, err)
	testutil.Equals(t, 120, chunk.NumSamples())

	chunk, err = chunkenc.FromData(chunkenc.EncXOR, firstSeries.Chunks[1].Raw.Data)
	testutil.Ok(t, err)
	testutil.Equals(t, 120, chunk.NumSamples())

	chunk, err = chunkenc.FromData(chunkenc.EncXOR, firstSeries.Chunks[len(firstSeries.Chunks)-1].Raw.Data)
	testutil.Ok(t, err)
	testutil.Equals(t, 35, chunk.NumSamples())
}

// Regression test for https://github.com/thanos-io/thanos/issues/396.
func TestPrometheusStore_Series_SplitSamplesIntoChunksWithMaxSizeOf120(t *testing.T) {
	defer custom.TolerantVerifyLeak(t)

	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	testSeries_SplitSamplesIntoChunksWithMaxSizeOf120(t, p.Appender(), func() storepb.StoreServer {
		testutil.Ok(t, p.Start())

		u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
		testutil.Ok(t, err)

		proxy, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
			func() labels.Labels { return labels.FromStrings("region", "eu-west") },
			func() (int64, int64) { return 0, math.MaxInt64 },
			func() stringset.Set { return stringset.AllStrings() },
			nil)
		testutil.Ok(t, err)

		// We build chunks only for SAMPLES method. Make sure we ask for SAMPLES only.
		proxy.remoteReadAcceptableResponses = []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES}

		return proxy
	})
}
