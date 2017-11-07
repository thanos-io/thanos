package store

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/improbable-eng/promlts/pkg/store/storepb"
	"github.com/improbable-eng/promlts/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
)

func TestPrometheusProxy_Series(t *testing.T) {
	p, err := testutil.NewPrometheus(":12345")
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

	u, err := url.Parse("http://localhost:12345")
	testutil.Ok(t, err)

	proxy, err := NewPrometheusProxy(nil, nil, nil, u,
		func() labels.Labels {
			return labels.FromStrings("region", "eu-west")
		})
	testutil.Ok(t, err)

	// Query all three samples except for the first one. Since we round up queried data
	// to seconds, we can test whether the extra sample gets stripped properly.
	resp, err := proxy.Series(ctx, &storepb.SeriesRequest{
		MinTime: baseT + 101,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
		},
	})
	testutil.Ok(t, err)

	testutil.Equals(t, 1, len(resp.Series))

	testutil.Equals(t, []storepb.Label{
		{Name: "a", Value: "b"},
		{Name: "region", Value: "eu-west"},
	}, resp.Series[0].Labels)

	testutil.Equals(t, 1, len(resp.Series[0].Chunks))

	c := resp.Series[0].Chunks[0]
	testutil.Equals(t, storepb.Chunk_XOR, c.Type)

	chk, err := chunks.FromData(chunks.EncXOR, c.Data)
	testutil.Ok(t, err)

	samples := expandChunk(chk.Iterator())
	testutil.Equals(t, []sample{{baseT + 200, 2}, {baseT + 300, 3}}, samples)
}

type sample struct {
	t int64
	v float64
}

func expandChunk(cit chunks.Iterator) (res []sample) {
	for cit.Next() {
		t, v := cit.At()
		res = append(res, sample{t, v})
	}
	return res
}

func TestPrometheusProxy_LabelValues(t *testing.T) {
	p, err := testutil.NewPrometheus(":12346")
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

	u, err := url.Parse("http://localhost:12346/")
	testutil.Ok(t, err)

	proxy, err := NewPrometheusProxy(nil, nil, nil, u, nil)
	testutil.Ok(t, err)

	resp, err := proxy.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label: "a",
	})
	testutil.Ok(t, err)

	testutil.Equals(t, []string{"a", "b", "c"}, resp.Values)
}
