// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sort"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type sortedSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer

	sortSeriesSet       bool
	sortWithoutLabelSet map[string]struct{}
	responses           []*storepb.SeriesResponse
}

func newSortedSeriesServer(upstream storepb.Store_SeriesServer, sortWithoutLabelSet map[string]struct{}, sortSeriesSet bool) *sortedSeriesServer {
	return &sortedSeriesServer{
		Store_SeriesServer: upstream,

		sortSeriesSet:       sortSeriesSet,
		sortWithoutLabelSet: sortWithoutLabelSet,

		// Buffered responses when sortSeriesSet is true.
		responses: make([]*storepb.SeriesResponse, 0),
	}
}

func (s *sortedSeriesServer) Send(r *storepb.SeriesResponse) error {
	series := r.GetSeries()
	// No need to buffer warnings and hints.
	if series == nil {
		return s.Store_SeriesServer.Send(r)
	}

	series.Labels = stripLabels(series.Labels, s.sortWithoutLabelSet)

	if !s.sortSeriesSet {
		return s.Store_SeriesServer.Send(r)
	}

	s.responses = append(s.responses, r)
	return nil
}

func (s *sortedSeriesServer) Flush() error {
	if !s.sortSeriesSet {
		return nil
	}

	if len(s.sortWithoutLabelSet) > 0 {
		sort.Slice(s.responses, func(i, j int) bool {
			return compareResponses(s.responses[i], s.responses[j])
		})
	}

	for _, r := range s.responses {
		if err := s.Store_SeriesServer.Send(r); err != nil {
			return err
		}
	}
	return nil
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

func sortRequired(sortWithoutLabels map[string]struct{}, extLabelsMap map[string]struct{}) bool {
	for lbl := range sortWithoutLabels {
		if _, isExtLabel := extLabelsMap[lbl]; !isExtLabel {
			return true
		}
	}

	return false
}
