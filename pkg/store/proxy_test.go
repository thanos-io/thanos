package store

import (
	"testing"

	"context"

	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/tsdb/chunkenc"
	tlabels "github.com/prometheus/tsdb/labels"
)

func TestQueryStore_Series(t *testing.T) {
	cls := []*query.StoreInfo{
		{
			Client: &testutil.StoreClient{
				RespSet: []*storepb.SeriesResponse{
					testutil.StoreSeriesResponse(t, labels.FromStrings("a", "a"), []testutil.Sample{{0, 0}, {2, 1}, {3, 2}}),
					storepb.NewWarnSeriesResponse(errors.New("partial error")),
					testutil.StoreSeriesResponse(t, labels.FromStrings("a", "b"), []testutil.Sample{{2, 2}, {3, 3}, {4, 4}}),
				},
			},
			MinTime: 1,
			MaxTime: 300,
		},
		{
			Client: &testutil.StoreClient{
				RespSet: []*storepb.SeriesResponse{
					testutil.StoreSeriesResponse(t, labels.FromStrings("a", "b"), []testutil.Sample{{1, 1}, {2, 2}, {3, 3}}),
				},
			},
			MinTime: 1,
			MaxTime: 300,
		},
		{
			Client: &testutil.StoreClient{
				RespSet: []*storepb.SeriesResponse{
					storepb.NewWarnSeriesResponse(errors.New("partial error")),
				},
			},
			MinTime: 1,
			MaxTime: 300,
		},
		{
			Client: &testutil.StoreClient{
				RespSet: []*storepb.SeriesResponse{
					testutil.StoreSeriesResponse(t, labels.FromStrings("a", "c"), []testutil.Sample{{100, 1}, {300, 3}, {400, 4}}),
				},
			},
			MinTime: 1,
			MaxTime: 300,
		},
		{
			Client: &testutil.StoreClient{
				RespSet: []*storepb.SeriesResponse{
					testutil.StoreSeriesResponse(t, labels.FromStrings("a", "outside"), []testutil.Sample{{1, 1}}),
				},
			},
			// Outside range for store itself.
			MinTime: 301,
			MaxTime: 302,
		},
	}
	q := NewProxyStore(nil, func() []*query.StoreInfo { return cls }, tlabels.FromStrings("fed", "a"))

	ctx := context.Background()
	s1 := testutil.NewStoreSeriesServer(ctx)

	// This should return empty response, since there is external label mismatch.
	err := q.Series(
		&storepb.SeriesRequest{
			MinTime:  1,
			MaxTime:  300,
			Matchers: []storepb.LabelMatcher{{Name: "fed", Value: "not-a", Type: storepb.LabelMatcher_EQ}},
		}, s1,
	)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(s1.SeriesSet))
	testutil.Equals(t, 0, len(s1.Warnings))

	s2 := testutil.NewStoreSeriesServer(ctx)
	err = q.Series(
		&storepb.SeriesRequest{
			MinTime:  1,
			MaxTime:  300,
			Matchers: []storepb.LabelMatcher{{Name: "fed", Value: "a", Type: storepb.LabelMatcher_EQ}},
		}, s2,
	)
	testutil.Ok(t, err)

	expected := []struct {
		lset    []storepb.Label
		samples []testutil.Sample
	}{
		{
			lset:    []storepb.Label{{Name: "a", Value: "a"}},
			samples: []testutil.Sample{{0, 0}, {2, 1}, {3, 2}},
		},
		{
			lset:    []storepb.Label{{Name: "a", Value: "b"}},
			samples: []testutil.Sample{{2, 2}, {3, 3}, {4, 4}, {1, 1}, {2, 2}, {3, 3}},
		},
		{
			lset:    []storepb.Label{{Name: "a", Value: "c"}},
			samples: []testutil.Sample{{100, 1}, {300, 3}, {400, 4}},
		},
	}

	// We should have all series given by all our clients.
	testutil.Equals(t, len(expected), len(s2.SeriesSet))

	for i, series := range s2.SeriesSet {
		testutil.Equals(t, expected[i].lset, series.Labels)

		k := 0
		for _, chk := range series.Chunks {
			c, err := chunkenc.FromData(chunkenc.EncXOR, chk.Data)
			testutil.Ok(t, err)

			iter := c.Iterator()
			for iter.Next() {
				testutil.Assert(t, k < len(expected[i].samples), "more samples than expected")

				tv, v := iter.At()
				testutil.Equals(t, expected[i].samples[k], testutil.Sample{tv, v})
				k++
			}
			testutil.Ok(t, iter.Err())
		}
		testutil.Equals(t, len(expected[i].samples), k)
	}

	// We should have all warnings given by all our clients too.
	testutil.Equals(t, 2, len(s2.Warnings))
}
