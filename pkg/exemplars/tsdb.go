// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"sync"

	"github.com/gogo/status"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"google.golang.org/grpc/codes"

	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// TSDB allows fetching exemplars from a TSDB instance.
type TSDB struct {
	db storage.ExemplarQueryable

	extLabels labels.Labels
	mtx       sync.RWMutex
}

// NewTSDB creates new exemplars.TSDB.
func NewTSDB(db storage.ExemplarQueryable, extLabels labels.Labels) *TSDB {
	return &TSDB{
		db:        db,
		extLabels: extLabels,
	}
}

func (t *TSDB) SetExtLabels(extLabels labels.Labels) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	t.extLabels = extLabels
}

func (t *TSDB) getExtLabels() labels.Labels {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.extLabels
}

// Exemplars returns all specified exemplars from a TSDB instance.
func (t *TSDB) Exemplars(matchers [][]*labels.Matcher, start, end int64, s exemplarspb.Exemplars_ExemplarsServer) error {
	match, selectors := selectorsMatchesExternalLabels(matchers, t.getExtLabels())

	if !match {
		return nil
	}

	if len(selectors) == 0 {
		return status.Error(codes.InvalidArgument, errors.New("no matchers specified (excluding external labels)").Error())
	}

	eq, err := t.db.ExemplarQuerier(s.Context())
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	exemplars, err := eq.Select(start, end, selectors...)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	for _, e := range exemplars {
		exd := exemplarspb.ExemplarData{
			SeriesLabels: labelpb.ZLabelSet{
				Labels: labelpb.ZLabelsFromPromLabels(labelpb.ExtendSortedLabels(e.SeriesLabels, t.getExtLabels())),
			},
			Exemplars: exemplarspb.ExemplarsFromPromExemplars(e.Exemplars),
		}
		if err := s.Send(exemplarspb.NewExemplarsResponse(&exd)); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}
	return nil
}

// selectorsMatchesExternalLabels returns false if none of the selectors matches the external labels.
// If true, it also returns an array of non-empty Prometheus matchers.
func selectorsMatchesExternalLabels(selectors [][]*labels.Matcher, externalLabels labels.Labels) (bool, [][]*labels.Matcher) {
	matchedOnce := false

	var newSelectors [][]*labels.Matcher
	for _, m := range selectors {
		match, m := matchesExternalLabels(m, externalLabels)

		matchedOnce = matchedOnce || match
		if match && len(m) > 0 {
			newSelectors = append(newSelectors, m)
		}
	}

	return matchedOnce, newSelectors
}
