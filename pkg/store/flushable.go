// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/exp/slices"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type sortingStrategy uint64

const (
	sortingStrategyStore sortingStrategy = iota + 1
	sortingStrategyNone
)

// flushableServer is an extension of storepb.Store_SeriesServer with a Flush method.
type flushableServer interface {
	storepb.Store_SeriesServer

	Flush() error
}

func newFlushableServer(
	upstream storepb.Store_SeriesServer,
	sortingsortingStrategy sortingStrategy,
) flushableServer {
	switch sortingsortingStrategy {
	case sortingStrategyStore:
		return &resortingServer{Store_SeriesServer: upstream}
	case sortingStrategyNone:
		return &passthroughServer{Store_SeriesServer: upstream}
	default:
		// should not happen.
		panic("unexpected sorting strategy")
	}
}

// passthroughServer is a flushableServer that forwards all data to
// an upstream server without additional processing.
type passthroughServer struct {
	storepb.Store_SeriesServer
}

func (p *passthroughServer) Flush() error { return nil }

// resortingServer is a flushableServer that resorts all series by their labels.
// This is required if replica labels are stored internally in a TSDB.
// Data is resorted and sent to an upstream server upon calling Flush.
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
	slices.SortFunc(r.series, func(a, b *storepb.Series) int {
		return labels.Compare(
			labelpb.ZLabelsToPromLabels(a.Labels),
			labelpb.ZLabelsToPromLabels(b.Labels),
		)
	})
	for _, response := range r.series {
		if err := r.Store_SeriesServer.Send(storepb.NewSeriesResponse(response)); err != nil {
			return err
		}
	}
	return nil
}
