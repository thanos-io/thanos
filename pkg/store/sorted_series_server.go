// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type sortedSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer
	stripLabels map[string]struct{}
}

func newSortedSeriesServer(upstream storepb.Store_SeriesServer, stripLabels map[string]struct{}) *sortedSeriesServer {
	return &sortedSeriesServer{
		Store_SeriesServer: upstream,
		stripLabels:        stripLabels,
	}
}

func (s *sortedSeriesServer) Send(r *storepb.SeriesResponse) error {
	series := r.GetSeries()
	// No need to buffer warnings and hints.
	if series == nil {
		return s.Store_SeriesServer.Send(r)
	}

	series.Labels = stripLabels(series.Labels, s.stripLabels)
	return s.Store_SeriesServer.Send(r)
}

func stripLabels(labelSet []labelpb.ZLabel, labelsToRemove map[string]struct{}) []labelpb.ZLabel {
	if len(labelsToRemove) == 0 {
		return labelSet
	}

	i := 0
	for i < len(labelSet) {
		lbl := labelSet[i]
		if _, ok := labelsToRemove[lbl.Name]; ok {
			labelSet = append(labelSet[:i], labelSet[i+1:]...)
		} else {
			i++
		}
	}
	return labelSet
}
