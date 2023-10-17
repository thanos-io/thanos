// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package labelpb

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
)

// TODO: something good
func StringLabelsFromPromLabels(lset labels.Labels) StringLabels {
	return *(*StringLabels)(unsafe.Pointer(&lset))
}

// TODO: something good
func StringLabelsToPromLabels(lset StringLabels) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&lset))
}
