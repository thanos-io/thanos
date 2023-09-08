// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prompb

func (h Histogram) IsFloatHistogram() bool {
	_, ok := h.GetCount().(*Histogram_CountFloat)
	return ok
}
