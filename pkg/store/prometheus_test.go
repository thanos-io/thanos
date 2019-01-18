package store

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/component"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/labels"
)

func TestPrometheusStore_Series_e2e(t *testing.T) {
	testPrometheusStoreSeriesE2e(t, "")
}

// Regression test for https://github.com/improbable-eng/thanos/issues/478.
func TestPrometheusStore_Series_promOnPath_e2e(t *testing.T) {
	testPrometheusStoreSeriesE2e(t, "/prometheus/sub/path")
}

func testPrometheusStoreSeriesE2e(t *testing.T, prefix string) {
	t.Helper()

	defer leaktest.CheckTimeout(t, 10*time.Second)()

	p, err := testutil.NewPrometheusOnPath(prefix)
	testutil.Ok(t, err)

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	a := p.Appender()
	_, err = a.Add(labels.FromStrings("a", "b"), baseT+100, 1)
	testutil.Ok(t, err)
	_, err = a.Add(labels.FromStrings("a", "b"), baseT+200, 2)
	testutil.Ok(t, err)
	_, err = a.Add(labels.FromStrings("a", "b"), baseT+300, 3)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())
	defer func() { testutil.Ok(t, p.Stop()) }()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, u, component.Sidecar,
		func() labels.Labels {
			return labels.FromStrings("region", "eu-west")
		}, nil)
	testutil.Ok(t, err)

	// Query all three samples except for the first one. Since we round up queried data
	// to seconds, we can test whether the extra sample gets stripped properly.
	srv := newStoreSeriesServer(ctx)

	err = proxy.Series(&storepb.SeriesRequest{
		MinTime: baseT + 101,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
		},
	}, srv)
	testutil.Ok(t, err)

	testutil.Equals(t, 1, len(srv.SeriesSet))

	testutil.Equals(t, []storepb.Label{
		{Name: "a", Value: "b"},
		{Name: "region", Value: "eu-west"},
	}, srv.SeriesSet[0].Labels)

	testutil.Equals(t, 1, len(srv.SeriesSet[0].Chunks))

	c := srv.SeriesSet[0].Chunks[0]
	testutil.Equals(t, storepb.Chunk_XOR, c.Raw.Type)

	chk, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
	testutil.Ok(t, err)

	samples := expandChunk(chk.Iterator())
	testutil.Equals(t, []sample{{baseT + 200, 2}, {baseT + 300, 3}}, samples)
}

type sample struct {
	t int64
	v float64
}

func expandChunk(cit chunkenc.Iterator) (res []sample) {
	for cit.Next() {
		t, v := cit.At()
		res = append(res, sample{t, v})
	}
	return res
}

func getExternalLabels() labels.Labels {
	return labels.Labels{
		{Name: "ext_a", Value: "a"},
		{Name: "ext_b", Value: "a"}}
}

func TestPrometheusStore_LabelValues_e2e(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	a := p.Appender()
	_, err = a.Add(labels.FromStrings("a", "b"), 0, 1)
	testutil.Ok(t, err)
	_, err = a.Add(labels.FromStrings("a", "c"), 0, 1)
	testutil.Ok(t, err)
	_, err = a.Add(labels.FromStrings("a", "a"), 0, 1)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())
	defer func() { testutil.Ok(t, p.Stop()) }()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, u, component.Sidecar, getExternalLabels, nil)
	testutil.Ok(t, err)

	resp, err := proxy.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label: "a",
	})
	testutil.Ok(t, err)

	testutil.Equals(t, []string{"a", "b", "c"}, resp.Values)
}

// Test to check external label values retrieve.
func TestPrometheusStore_ExternalLabelValues_e2e(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	a := p.Appender()
	_, err = a.Add(labels.FromStrings("ext_a", "b"), 0, 1)
	testutil.Ok(t, err)
	_, err = a.Add(labels.FromStrings("a", "b"), 0, 1)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())
	defer func() { testutil.Ok(t, p.Stop()) }()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, u, component.Sidecar, getExternalLabels, nil)
	testutil.Ok(t, err)

	resp, err := proxy.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label: "ext_a",
	})
	testutil.Ok(t, err)

	testutil.Equals(t, []string{"a"}, resp.Values)

	resp, err = proxy.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label: "a",
	})
	testutil.Ok(t, err)

	testutil.Equals(t, []string{"b"}, resp.Values)
}

func TestPrometheusStore_Series_MatchExternalLabel_e2e(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	a := p.Appender()
	_, err = a.Add(labels.FromStrings("a", "b"), baseT+100, 1)
	testutil.Ok(t, err)
	_, err = a.Add(labels.FromStrings("a", "b"), baseT+200, 2)
	testutil.Ok(t, err)
	_, err = a.Add(labels.FromStrings("a", "b"), baseT+300, 3)
	testutil.Ok(t, err)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())
	defer func() { testutil.Ok(t, p.Stop()) }()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, u, component.Sidecar,
		func() labels.Labels {
			return labels.FromStrings("region", "eu-west")
		}, nil)
	testutil.Ok(t, err)
	srv := newStoreSeriesServer(ctx)

	err = proxy.Series(&storepb.SeriesRequest{
		MinTime: baseT + 101,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
		},
	}, srv)
	testutil.Ok(t, err)

	testutil.Equals(t, 1, len(srv.SeriesSet))

	testutil.Equals(t, []storepb.Label{
		{Name: "a", Value: "b"},
		{Name: "region", Value: "eu-west"},
	}, srv.SeriesSet[0].Labels)

	srv = newStoreSeriesServer(ctx)
	// However it should not match wrong external label.
	err = proxy.Series(&storepb.SeriesRequest{
		MinTime: baseT + 101,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west2"}, // Non existing label value.
		},
	}, srv)
	testutil.Ok(t, err)

	// No series.
	testutil.Equals(t, 0, len(srv.SeriesSet))
}

func TestPrometheusStore_Info(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy, err := NewPrometheusStore(nil, nil, nil, component.Sidecar,
		func() labels.Labels {
			return labels.FromStrings("region", "eu-west")
		},
		func() (int64, int64) {
			return 123, 456
		})
	testutil.Ok(t, err)

	resp, err := proxy.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, []storepb.Label{{Name: "region", Value: "eu-west"}}, resp.Labels)
	testutil.Equals(t, storepb.StoreType_SIDECAR, resp.StoreType)
	testutil.Equals(t, int64(123), resp.MinTime)
	testutil.Equals(t, int64(456), resp.MaxTime)
}

// Regression test for https://github.com/improbable-eng/thanos/issues/396.
func TestPrometheusStore_Series_SplitSamplesIntoChunksWithMaxSizeOfUint16_e2e(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	baseT := timestamp.FromTime(time.Now().AddDate(0, 0, -2)) / 1000 * 1000

	a := p.Appender()

	offset := int64(2*math.MaxUint16 + 5)
	for i := int64(0); i < offset; i++ {
		_, err = a.Add(labels.FromStrings("a", "b"), baseT+i, 1)
		testutil.Ok(t, err)
	}

	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())
	defer func() { testutil.Ok(t, p.Stop()) }()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, u, component.Sidecar,
		func() labels.Labels {
			return labels.FromStrings("region", "eu-west")
		}, nil)
	testutil.Ok(t, err)
	srv := newStoreSeriesServer(ctx)

	testutil.Ok(t, proxy.Series(&storepb.SeriesRequest{
		MinTime: baseT,
		MaxTime: baseT + offset,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
			{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
		},
	}, srv))

	testutil.Equals(t, 1, len(srv.SeriesSet))

	firstSeries := srv.SeriesSet[0]

	testutil.Equals(t, []storepb.Label{
		{Name: "a", Value: "b"},
		{Name: "region", Value: "eu-west"},
	}, firstSeries.Labels)

	testutil.Equals(t, 3, len(firstSeries.Chunks))

	chunk, err := chunkenc.FromData(chunkenc.EncXOR, firstSeries.Chunks[0].Raw.Data)
	testutil.Ok(t, err)
	testutil.Equals(t, math.MaxUint16, chunk.NumSamples())

	chunk, err = chunkenc.FromData(chunkenc.EncXOR, firstSeries.Chunks[1].Raw.Data)
	testutil.Ok(t, err)
	testutil.Equals(t, math.MaxUint16, chunk.NumSamples())

	chunk, err = chunkenc.FromData(chunkenc.EncXOR, firstSeries.Chunks[2].Raw.Data)
	testutil.Ok(t, err)
	testutil.Equals(t, 5, chunk.NumSamples())
}
