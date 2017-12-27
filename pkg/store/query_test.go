package store

import (
	"testing"

	"context"

	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	tlabels "github.com/prometheus/tsdb/labels"
)

func TestQueryStore_Series(t *testing.T) {
	lset := labels.FromStrings("a", "a")
	cls := []*query.StoreInfo{
		{
			Client: &testutil.StoreClient{
				SeriesSet: []storepb.Series{
					testutil.StoreSeries(t, lset, []testutil.Sample{{0, 0}}),
					testutil.WarningSeries, // Trigger partial error response.
					testutil.StoreSeries(t, lset, []testutil.Sample{{1, 1}}),
				},
			},
		},
		{
			Client: &testutil.StoreClient{
				SeriesSet: []storepb.Series{
					testutil.StoreSeries(t, lset, []testutil.Sample{{2, 2}}),
				},
			},
		},
		{
			Client: &testutil.StoreClient{
				SeriesSet: []storepb.Series{
					testutil.WarningSeries, // Trigger partial error response.
				},
			},
		},
	}
	q := NewQueryStore(nil, func() []*query.StoreInfo { return cls }, tlabels.FromStrings("fed", "a"))

	ctx := context.Background()
	s1 := testutil.NewStoreSeriesServer(ctx)

	// This should return empty response, since there is external label mismatch.
	err := q.Series(
		&storepb.SeriesRequest{
			MinTime:  0,
			MaxTime:  0,
			Matchers: []storepb.LabelMatcher{{Name: "fed", Value: "not-a", Type: storepb.LabelMatcher_EQ}},
		}, s1,
	)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(s1.SeriesSet))
	testutil.Equals(t, 0, len(s1.Warnings))

	s2 := testutil.NewStoreSeriesServer(ctx)
	err = q.Series(
		&storepb.SeriesRequest{
			MinTime:  0,
			MaxTime:  0,
			Matchers: []storepb.LabelMatcher{{Name: "fed", Value: "a", Type: storepb.LabelMatcher_EQ}},
		}, s2,
	)
	testutil.Ok(t, err)

	// We should have all series given by all our clients.
	testutil.Equals(t, 3, len(s2.SeriesSet))
	for _, series := range s2.SeriesSet {
		testutil.Equals(t, []storepb.Label{
			{Name: lset[0].Name, Value: lset[0].Value},
			{Name: "fed", Value: "a"},
		}, series.Labels)
	}

	// We should have all warnings given by all our clients too.
	testutil.Equals(t, 2, len(s2.Warnings))
}
