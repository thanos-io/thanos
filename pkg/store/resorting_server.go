package store

import (
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/exp/slices"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type resortingServer struct {
	storepb.Store_SeriesServer
	series []*storepb.Series
}

func (r *resortingServer) Send(response *storepb.SeriesResponse) error {
	if response.GetSeries() == nil {
		return r.Store_SeriesServer.Send(response)
	}

	series := response.GetSeries()
	labelpb.ReAllocZLabelsStrings(&series.Labels, false)
	r.series = append(r.series, series)
	return nil
}

func (r *resortingServer) Flush() error {
	slices.SortFunc(r.series, func(a, b *storepb.Series) bool {
		return labels.Compare(
			labelpb.ZLabelsToPromLabels(a.Labels),
			labelpb.ZLabelsToPromLabels(b.Labels),
		) < 0
	})
	for _, response := range r.series {
		if err := r.Store_SeriesServer.Send(storepb.NewSeriesResponse(response)); err != nil {
			return err
		}
	}
	return nil
}
