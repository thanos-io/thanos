// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplarspb

import (
	"encoding/json"
	"math/big"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func NewExemplarsResponse(e *ExemplarData) *ExemplarsResponse {
	return &ExemplarsResponse{
		Result: &ExemplarsResponse_Data{
			Data: e,
		},
	}
}

func NewWarningExemplarsResponse(warning error) *ExemplarsResponse {
	return &ExemplarsResponse{
		Result: &ExemplarsResponse_Warning{
			Warning: warning.Error(),
		},
	}
}

func (s *ExemplarData) MarshalJSON() ([]byte, error) {
	if s == nil {
		return []byte("[]"), nil
	}
	return json.Marshal(s)
}

func (s1 *ExemplarData) Compare(s2 *ExemplarData) int {
	if d := labels.Compare(s1.SeriesLabels.PromLabels(), s2.SeriesLabels.PromLabels()); d != 0 {
		return d
	}
	return 0
}

func (s *ExemplarData) SetSeriesLabels(ls labels.Labels) {
	var result labelpb.ZLabelSet

	if len(ls) > 0 {
		result = labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(ls)}
	}

	s.SeriesLabels = result
}

func (e *Exemplar) SetLabels(ls labels.Labels) {
	var result labelpb.ZLabelSet

	if len(ls) > 0 {
		result = labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(ls)}
	}

	e.Labels = result
}

func (e1 *Exemplar) Compare(e2 *Exemplar) int {
	if d := labels.Compare(e1.Labels.PromLabels(), e2.Labels.PromLabels()); d != 0 {
		return d
	}

	if e1.Hasts || e2.Hasts {
		if e1.Ts.Before(e2.Ts) {
			return 1
		}
		if e1.Ts.After(e2.Ts) {
			return -1
		}
	}

	if d := big.NewFloat(e1.Value).Cmp(big.NewFloat(e2.Value)); d != 0 {
		return d
	}

	return 0
}
