// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

//go:build slicelabels

package labelpb

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
)

// ZLabelsFromPromLabels converts Prometheus labels to slice of labelpb.ZLabel in type unsafe manner.
// It reuses the same memory. Caller should abort using passed labels.Labels.
func ZLabelsFromPromLabels(lset labels.Labels) []ZLabel {
	return *(*[]ZLabel)(unsafe.Pointer(&lset))
}

// ZLabelsToPromLabels convert slice of labelpb.ZLabel to Prometheus labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed []ZLabel.
// NOTE: Use with care. ZLabels holds memory from the whole protobuf unmarshal, so the returned
// Prometheus Labels will hold this memory as well.
func ZLabelsToPromLabels(lset []ZLabel) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&lset))
}
