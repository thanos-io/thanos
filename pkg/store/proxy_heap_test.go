package store

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestSortWithoutLabels(t *testing.T) {
	for _, tcase := range []struct {
		input       []*storepb.SeriesResponse
		exp         []*storepb.SeriesResponse
		dedupLabels map[string]struct{}
	}{
		// Single deduplication label.
		{
			input: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-2", "c", "3")),
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "4")),
			},
			dedupLabels: map[string]struct{}{"b": {}},
		},
		// Multi deduplication labels.
		{
			input: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "b1", "replica-1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "b1", "replica-1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "b1", "replica-1", "c", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "b1", "replica-2", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-2", "c", "3")),
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "4")),
			},
			dedupLabels: map[string]struct{}{"b": {}, "b1": {}},
		},
		// Pushdown label at the end.
		{
			input: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "4", dedup.PushdownMarker.Name, dedup.PushdownMarker.Value)),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-2", "c", "3")),
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "4", dedup.PushdownMarker.Name, dedup.PushdownMarker.Value)),
			},
			dedupLabels: map[string]struct{}{"b": {}},
		},
		// Non series responses mixed.
		{
			input: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "3", "d", "4")),
				storepb.NewWarnSeriesResponse(errors.Newf("yolo")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-2", "c", "3")),
			},
			exp: []*storepb.SeriesResponse{
				storepb.NewWarnSeriesResponse(errors.Newf("yolo")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "4")),
			},
			dedupLabels: map[string]struct{}{"b": {}},
		},
	} {
		t.Run("", func(t *testing.T) {
			sortWithoutLabels(tcase.input, tcase.dedupLabels)
			testutil.Equals(t, tcase.exp, tcase.input)
		})
	}
}

// labelsFromStrings is like labels.FromString, but it does not sort the input.
func labelsFromStrings(ss ...string) labels.Labels {
	if len(ss)%2 != 0 {
		panic("invalid number of strings")
	}
	res := make(labels.Labels, 0, len(ss)/2)
	for i := 0; i < len(ss); i += 2 {
		res = append(res, labels.Label{Name: ss[i], Value: ss[i+1]})
	}

	return res
}
