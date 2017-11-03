package store

import (
	"context"
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

	baseT := timestamp.FromTime(time.Now())

	a := p.Appender()
	a.Add(labels.FromStrings("a", "b"), baseT+100, 1)
	a.Add(labels.FromStrings("a", "b"), baseT+200, 2)
	a.Add(labels.FromStrings("a", "b"), baseT+300, 3)
	testutil.Ok(t, a.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testutil.Ok(t, p.Start(ctx))

	proxy, err := NewPrometheusProxy(nil, "http://localhost:12345/")
	testutil.Ok(t, err)

	resp, err := proxy.Series(ctx, &storepb.SeriesRequest{
		MinTime: baseT,
		MaxTime: baseT + 300,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
		},
	})
	testutil.Ok(t, err)

	testutil.Equals(t, 1, len(resp.Series))
	testutil.Equals(t, []storepb.Label{{Name: "a", Value: "b"}}, resp.Series[0].Labels)
	testutil.Equals(t, 1, len(resp.Series[0].Chunks))

	c := resp.Series[0].Chunks[0]
	testutil.Equals(t, storepb.Chunk_XOR, c.Type)

	chk, err := chunks.FromData(chunks.EncXOR, c.Data)
	testutil.Ok(t, err)

	samples := expandChunk(chk.Iterator())

	testutil.Equals(t, []sample{
		{baseT + 100, 1}, {baseT + 200, 2}, {baseT + 300, 3},
	}, samples)

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
