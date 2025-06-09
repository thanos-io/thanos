// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package containing proto and JSON serializable Labels and ZLabels (no copy) structs used to
// identify series. This package expose no-copy converters to Prometheus labels.Labels.

//go:build stringlabels

package labelpb

import (
	"github.com/prometheus/prometheus/model/labels"
)

// ZLabelsFromPromLabels converts Prometheus labels to slice of labelpb.ZLabel.
// It reuses the same memory. Caller should abort using passed labels.Labels.
func ZLabelsFromPromLabels(lset labels.Labels) []ZLabel {
	zlabels := make([]ZLabel, 0, lset.Len())
	lset.Range(func(l labels.Label) {
		zlabels = append(zlabels, ZLabel{
			Name:  l.Name,
			Value: l.Value,
		})
	})
	return zlabels
}

// ZLabelsToPromLabels convert slice of labelpb.ZLabel to Prometheus labels.
func ZLabelsToPromLabels(lset []ZLabel) labels.Labels {
	builder := labels.NewScratchBuilder(len(lset))
	for _, l := range lset {
		builder.Add(l.Name, l.Value)
	}
	builder.Sort()
	return builder.Labels()
}

// ZLabelSetsToPromLabelSets converts slice of labelpb.ZLabelSet to slice of Prometheus labels.
func ZLabelSetsToPromLabelSets(lss ...ZLabelSet) []labels.Labels {
	res := make([]labels.Labels, 0, len(lss))
	for _, ls := range lss {
		res = append(res, ls.PromLabels())
	}
	return res
}

// ZLabelSetsFromPromLabels converts []labels.labels to []labelpb.ZLabelSet.
func ZLabelSetsFromPromLabels(lss ...labels.Labels) []ZLabelSet {
	sets := make([]ZLabelSet, 0, len(lss))
	for _, ls := range lss {
		set := ZLabelSet{
			Labels: make([]ZLabel, 0, ls.Len()),
		}
		ls.Range(func(lbl labels.Label) {
			set.Labels = append(set.Labels, ZLabel{
				Name:  lbl.Name,
				Value: lbl.Value,
			})
		})
		sets = append(sets, set)
	}

	return sets
}
