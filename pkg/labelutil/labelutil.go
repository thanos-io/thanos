package labelutil

import (
	promlabels "github.com/prometheus/prometheus/pkg/labels"
	tsdblabels "github.com/prometheus/tsdb/labels"
)

func TSDBLabelsToPromLabels(lset tsdblabels.Labels) (res promlabels.Labels) {
	for _, l := range lset {
		res = append(res, promlabels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return res
}
