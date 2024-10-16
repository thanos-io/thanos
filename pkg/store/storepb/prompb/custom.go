// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prompb

import (
	"github.com/prometheus/prometheus/model/histogram"
)

func (h Histogram) IsFloatHistogram() bool {
	_, ok := h.GetCount().(*Histogram_CountFloat)
	return ok
}

func FromProtoHistogram(h Histogram) *histogram.FloatHistogram {
	if h.IsFloatHistogram() {
		return FloatHistogramProtoToFloatHistogram(h)
	} else {
		return HistogramProtoToFloatHistogram(h)
	}
}
