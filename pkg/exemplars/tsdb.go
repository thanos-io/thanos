// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// TSDB implements exemplarspb.Exemplars that allows to fetch exemplars from a TSDB instance.
type TSDB struct {
	db        storage.ExemplarQueryable
	extLabels labels.Labels
}

// NewTSDB creates new exemplars.TSDB.
func NewTSDB(db storage.ExemplarQueryable, extLabels labels.Labels) *TSDB {
	return &TSDB{
		db:        db,
		extLabels: extLabels,
	}
}

// Exemplars returns all specified exemplars from a TSDB instance.
func (t *TSDB) Exemplars(r *exemplarspb.ExemplarsRequest, s exemplarspb.Exemplars_ExemplarsServer) error {
	eq, err := t.db.ExemplarQuerier(s.Context())
	if err != nil {
		return err
	}

	expr, err := parser.ParseExpr(r.Query)
	if err != nil {
		return err
	}

	matchers := parser.ExtractSelectors(expr)

	exemplars, err := eq.Select(r.Start, r.End, matchers...)
	if err != nil {
		return err
	}

	for _, e := range exemplars {
		exd := exemplarspb.ExemplarData{
			SeriesLabels: labelpb.ZLabelSet{
				Labels: labelpb.ZLabelsFromPromLabels(
					labelpb.ExtendSortedLabels(e.SeriesLabels, t.extLabels),
				),
			},
			Exemplars: exemplarspb.ExemplarsFromPromExemplars(e.Exemplars),
		}
		if err := s.Send(exemplarspb.NewExemplarsResponse(&exd)); err != nil {
			return err
		}
	}
	return nil
}
