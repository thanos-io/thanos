// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prompb

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
)

// SamplesFromSamplePairs converts a slice of model.SamplePair
// to a slice of Sample.
func SamplesFromSamplePairs(samples []model.SamplePair) []Sample {
	result := make([]Sample, 0, len(samples))
	for _, s := range samples {
		result = append(result, Sample{
			Value:     float64(s.Value),
			Timestamp: int64(s.Timestamp),
		})
	}

	return result
}

// SamplesFromPromqlPoints converts a slice of promql.Point
// to a slice of Sample.
func SamplesFromPromqlPoints(samples []promql.Point) []Sample {
	result := make([]Sample, 0, len(samples))
	for _, s := range samples {
		result = append(result, Sample{
			Value:     s.V,
			Timestamp: s.T,
		})
	}

	return result
}
