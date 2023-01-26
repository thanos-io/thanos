// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"github.com/prometheus/common/model"
)

// The following code will allow us to use JSON marshal and unmarshal functions from the Prometheus common package in
// query_range.go: https://github.com/prometheus/common/blob/846591a166358c7048ef197e84501ca688dda920/model/value.go
// Please see the link above for more details on Sample, SampleStream, HistogramPair and SampleHistogramPair.

func toModelSampleHistogramPair(s SampleHistogramPair) model.SampleHistogramPair {
	return model.SampleHistogramPair{
		Timestamp: model.Time(s.Timestamp),
		Histogram: toModelSampleHistogram(s.Histogram),
	}
}

func fromModelSampleHistogramPair(modelSampleHistogram model.SampleHistogramPair) (s SampleHistogramPair) {
	return SampleHistogramPair{
		Timestamp: int64(modelSampleHistogram.Timestamp),
		Histogram: fromModelSampleHistogram(&modelSampleHistogram.Histogram),
	}
}

func fromModelSampleHistogram(modelSampleHistogram *model.SampleHistogram) (s SampleHistogram) {
	buckets := make([]*HistogramBucket, len(modelSampleHistogram.Buckets))

	for i, b := range modelSampleHistogram.Buckets {
		buckets[i] = &HistogramBucket{
			Lower:      float64(b.Lower),
			Upper:      float64(b.Upper),
			Count:      float64(b.Count),
			Boundaries: int64(b.Boundaries),
		}
	}

	return SampleHistogram{
		Count:   float64(modelSampleHistogram.Count),
		Sum:     float64(modelSampleHistogram.Sum),
		Buckets: buckets,
	}
}

func toModelSampleHistogram(s SampleHistogram) model.SampleHistogram {
	modelBuckets := make([]*model.HistogramBucket, len(s.Buckets))

	for i, b := range s.Buckets {
		modelBuckets[i] = &model.HistogramBucket{
			Lower:      model.FloatString(b.Lower),
			Upper:      model.FloatString(b.Upper),
			Count:      model.FloatString(b.Count),
			Boundaries: int(b.Boundaries),
		}
	}

	return model.SampleHistogram{
		Count:   model.FloatString(s.Count),
		Sum:     model.FloatString(s.Sum),
		Buckets: modelBuckets,
	}
}
