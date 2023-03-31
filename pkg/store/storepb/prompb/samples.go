// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prompb

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
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
func SamplesFromPromqlPoints(samples ...promql.Point) ([]Sample, []Histogram) {
	floats := make([]Sample, 0, len(samples))
	histograms := make([]Histogram, 0, len(samples))
	for _, s := range samples {
		if s.H == nil {
			floats = append(floats, Sample{
				Value:     s.V,
				Timestamp: s.T,
			})
		} else {
			histograms = append(histograms, FloatHistogramToHistogramProto(s.T, s.H))
		}
	}

	return floats, histograms
}

// HistogramProtoToHistogram extracts a (normal integer) Histogram from the
// provided proto message. The caller has to make sure that the proto message
// represents an interger histogram and not a float histogram.
// Taken from https://github.com/prometheus/prometheus/blob/d33eb3ab17616a54b97d9f7791c791a79823f279/storage/remote/codec.go#L529-L542.
func HistogramProtoToHistogram(hp Histogram) *histogram.Histogram {
	return &histogram.Histogram{
		Schema:          hp.Schema,
		ZeroThreshold:   hp.ZeroThreshold,
		ZeroCount:       hp.GetZeroCountInt(),
		Count:           hp.GetCountInt(),
		Sum:             hp.Sum,
		PositiveSpans:   spansProtoToSpans(hp.GetPositiveSpans()),
		PositiveBuckets: hp.GetPositiveDeltas(),
		NegativeSpans:   spansProtoToSpans(hp.GetNegativeSpans()),
		NegativeBuckets: hp.GetNegativeDeltas(),
	}
}

// FloatHistogramToHistogramProto converts a float histogram to a protobuf type.
// Taken from https://github.com/prometheus/prometheus/blob/d33eb3ab17616a54b97d9f7791c791a79823f279/storage/remote/codec.go#L587-L601.
func FloatHistogramToHistogramProto(timestamp int64, fh *histogram.FloatHistogram) Histogram {
	return Histogram{
		Count:          &Histogram_CountFloat{CountFloat: fh.Count},
		Sum:            fh.Sum,
		Schema:         fh.Schema,
		ZeroThreshold:  fh.ZeroThreshold,
		ZeroCount:      &Histogram_ZeroCountFloat{ZeroCountFloat: fh.ZeroCount},
		NegativeSpans:  spansToSpansProto(fh.NegativeSpans),
		NegativeCounts: fh.NegativeBuckets,
		PositiveSpans:  spansToSpansProto(fh.PositiveSpans),
		PositiveCounts: fh.PositiveBuckets,
		ResetHint:      Histogram_ResetHint(fh.CounterResetHint),
		Timestamp:      timestamp,
	}
}

// HistogramProtoToFloatHistogram extracts a (normal integer) Histogram from the
// provided proto message to a Float Histogram. The caller has to make sure that
// the proto message represents an float histogram and not a integer histogram.
// Taken from https://github.com/prometheus/prometheus/blob/d33eb3ab17616a54b97d9f7791c791a79823f279/storage/remote/codec.go#L547-L560.
func HistogramProtoToFloatHistogram(hp Histogram) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		CounterResetHint: histogram.CounterResetHint(hp.ResetHint),
		Schema:           hp.Schema,
		ZeroThreshold:    hp.ZeroThreshold,
		ZeroCount:        hp.GetZeroCountFloat(),
		Count:            hp.GetCountFloat(),
		Sum:              hp.Sum,
		PositiveSpans:    spansProtoToSpans(hp.GetPositiveSpans()),
		PositiveBuckets:  hp.GetPositiveCounts(),
		NegativeSpans:    spansProtoToSpans(hp.GetNegativeSpans()),
		NegativeBuckets:  hp.GetNegativeCounts(),
	}
}

func spansToSpansProto(s []histogram.Span) []*BucketSpan {
	spans := make([]*BucketSpan, len(s))
	for i := 0; i < len(s); i++ {
		spans[i] = &BucketSpan{Offset: s[i].Offset, Length: s[i].Length}
	}

	return spans
}

func spansProtoToSpans(s []*BucketSpan) []histogram.Span {
	spans := make([]histogram.Span, len(s))
	for i := 0; i < len(s); i++ {
		spans[i] = histogram.Span{Offset: s[i].Offset, Length: s[i].Length}
	}

	return spans
}
