package store

import (
	"testing"

	"context"

	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	tlabels "github.com/prometheus/tsdb/labels"
)

func TestQueryStore_Series(t *testing.T) {
	lset := labels.FromStrings("a", "a")
	cls := []*query.StoreInfo{
		{
			Client: &testutil.StoreClient{
				RespSet: []*storepb.SeriesResponse{
					testutil.StoreSeriesResponse(t, labels.FromStrings("a", "a"), []testutil.Sample{{0, 0}, {2, 1}, {3, 2}}),
					storepb.NewWarnSeriesResponse(errors.New("partial error")),
					testutil.StoreSeriesResponse(t, labels.FromStrings("a", "b"), []testutil.Sample{{2, 2}, {3, 3}, {4, 4}}),
				},
			},
		},
		{
			Client: &testutil.StoreClient{
				RespSet: []*storepb.SeriesResponse{
					testutil.StoreSeriesResponse(t, labels.FromStrings("a", "b"), []testutil.Sample{{1, 1}, {2, 2}, {3, 3}}),
				},
			},
		},
		{
			Client: &testutil.StoreClient{
				RespSet: []*storepb.SeriesResponse{
					storepb.NewWarnSeriesResponse(errors.New("partial error")),
				},
			},
		},
		{
			Client: &testutil.StoreClient{
				RespSet: []*storepb.SeriesResponse{
					testutil.StoreSeriesResponse(t, labels.FromStrings("a", "c"), []testutil.Sample{{100, 1}, {300, 3}, {400, 4}}),
				},
			},
		},
	}
	q := NewProxyStore(nil, func() []*query.StoreInfo { return cls }, tlabels.FromStrings("fed", "a"))

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

	expected := []struct {
		lset    labels.Labels
		samples []testutil.Sample
	}{
		{
			lset:    labels.FromStrings("a", "a"),
			samples: []testutil.Sample{{2, 1}, {3, 2}},
		},
		{
			lset:    labels.FromStrings("a", "b"),
			samples: []testutil.Sample{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
		},
		{
			lset:    labels.FromStrings("a", "c"),
			samples: []testutil.Sample{{100, 1}, {300, 3}},
		},
	}

	// We should have all series given by all our clients.
	testutil.Equals(t, len(expected), len(s2.SeriesSet))

	for i, series := range s2.SeriesSet {
		testutil.Equals(t, expected[i].lset, series.Labels)
		testutil.Equals(t, expected[i].samples, series.Chunks)
	}

	// We should have all warnings given by all our clients too.
	testutil.Equals(t, 2, len(s2.Warnings))
}
