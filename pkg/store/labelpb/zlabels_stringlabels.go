// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

//go:build !slicelabels

package labelpb

import (
	"github.com/prometheus/prometheus/model/labels"
)

// In the default (stringlabels) and dedupelabels builds, labels.Labels has a
// packed internal representation, so the slice-header conversion used in the
// slicelabels build would produce corrupt labels (and eventually panics, e.g.
// inside labels.decodeString). Build real Labels instead; the conversion
// allocates and does not share memory with the input.

// ZLabelsFromPromLabels converts Prometheus labels to a slice of labelpb.ZLabel.
// The returned slice does not share memory with the passed labels.Labels.
func ZLabelsFromPromLabels(lset labels.Labels) []ZLabel {
	zl := make([]ZLabel, 0, lset.Len())
	lset.Range(func(l labels.Label) {
		zl = append(zl, ZLabel{Name: l.Name, Value: l.Value})
	})
	return zl
}

// ZLabelsToPromLabels converts a slice of labelpb.ZLabel to Prometheus labels.
// The returned Labels does not share memory with the passed slice. Labels are
// expected to be sorted, matching the other conversion direction.
func ZLabelsToPromLabels(lset []ZLabel) labels.Labels {
	b := labels.NewScratchBuilder(len(lset))
	for _, l := range lset {
		b.Add(l.Name, l.Value)
	}
	return b.Labels()
}
