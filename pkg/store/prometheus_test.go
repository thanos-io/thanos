package store

import (
	"context"
	"net/url"
	"testing"
	"time"

	"fmt"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/labels"
)

func TestPrometheusStore_Series(t *testing.T) {
	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	a := p.Appender()
	a.Add(labels.FromStrings("a", "b"), baseT+100, 1)
	a.Add(labels.FromStrings("a", "b"), baseT+200, 2)
	a.Add(labels.FromStrings("a", "b"), baseT+300, 3)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())
	defer p.Stop()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, nil, u,
		func() labels.Labels {
			return labels.FromStrings("region", "eu-west")
		})
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

func TestPrometheusStore_LabelValues(t *testing.T) {
	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	a := p.Appender()
	a.Add(labels.FromStrings("a", "b"), 0, 1)
	a.Add(labels.FromStrings("a", "c"), 0, 1)
	a.Add(labels.FromStrings("a", "a"), 0, 1)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())
	defer p.Stop()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, nil, u, nil)
	testutil.Ok(t, err)

	resp, err := proxy.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label: "a",
	})
	testutil.Ok(t, err)

	testutil.Equals(t, []string{"a", "b", "c"}, resp.Values)
}

func TestPrometheusStore_Series_MatchExternalLabel(t *testing.T) {
	p, err := testutil.NewPrometheus()
	testutil.Ok(t, err)

	baseT := timestamp.FromTime(time.Now()) / 1000 * 1000

	a := p.Appender()
	a.Add(labels.FromStrings("a", "b"), baseT+100, 1)
	a.Add(labels.FromStrings("a", "b"), baseT+200, 2)
	a.Add(labels.FromStrings("a", "b"), baseT+300, 3)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start())
	defer p.Stop()

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	proxy, err := NewPrometheusStore(nil, nil, nil, u,
		func() labels.Labels {
			return labels.FromStrings("region", "eu-west")
		})
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
