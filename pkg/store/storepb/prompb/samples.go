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

// SamplesFromPromqlSamples converts a slice of promql.Sample
// to a slice of Sample.
func SamplesFromPromqlSamples(samples ...promql.Sample) ([]Sample, []Histogram) {
	floats := make([]Sample, 0, len(samples))
	histograms := make([]Histogram, 0, len(samples))
	for _, s := range samples {
		if s.H == nil {
			floats = append(floats, Sample{
				Value:     s.F,
				Timestamp: s.T,
			})
		} else {
			histograms = append(histograms, FloatHistogramToHistogramProto(s.T, s.H))
		}
	}

	return floats, histograms
}

// SamplesFromPromqlSeries converts promql.Series to a slice of Sample and a slice of Histogram.
func SamplesFromPromqlSeries(series promql.Series) ([]Sample, []Histogram) {
	floats := make([]Sample, 0, len(series.Floats))
	for _, f := range series.Floats {
		floats = append(floats, Sample{
			Value:     f.F,
			Timestamp: f.T,
		})
	}
	histograms := make([]Histogram, 0, len(series.Histograms))
	for _, h := range series.Histograms {
		histograms = append(histograms, FloatHistogramToHistogramProto(h.T, h.H))
	}

	return floats, histograms
}

// HistogramProtoToHistogram extracts a (normal integer) Histogram from the
// provided proto message. The caller has to make sure that the proto message
// represents an integer histogram and not a float histogram.
// Copied from https://github.com/prometheus/prometheus/blob/0ab95536115adfe50af249d36d73674be694ca3f/storage/remote/codec.go#L626-L645
func HistogramProtoToHistogram(hp Histogram) *histogram.Histogram {
	if hp.IsFloatHistogram() {
		panic("HistogramProtoToHistogram called with a float histogram")
	}
	return &histogram.Histogram{
		CounterResetHint: histogram.CounterResetHint(hp.ResetHint),
		Schema:           hp.Schema,
		ZeroThreshold:    hp.ZeroThreshold,
		ZeroCount:        hp.GetZeroCountInt(),
		Count:            hp.GetCountInt(),
		Sum:              hp.Sum,
		PositiveSpans:    spansProtoToSpans(hp.GetPositiveSpans()),
		PositiveBuckets:  hp.GetPositiveDeltas(),
		NegativeSpans:    spansProtoToSpans(hp.GetNegativeSpans()),
		NegativeBuckets:  hp.GetNegativeDeltas(),
	}
}

// FloatHistogramToHistogramProto converts a float histogram to a protobuf type.
// Copied from https://github.com/prometheus/prometheus/blob/0ab95536115adfe50af249d36d73674be694ca3f/storage/remote/codec.go#L647-L667
func FloatHistogramProtoToFloatHistogram(hp Histogram) *histogram.FloatHistogram {
	if !hp.IsFloatHistogram() {
		panic("FloatHistogramProtoToFloatHistogram called with an integer histogram")
	}
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

// HistogramProtoToFloatHistogram extracts a (normal integer) Histogram from the
// provided proto message to a Float Histogram. The caller has to make sure that
// the proto message represents an float histogram and not a integer histogram.
// Copied from https://github.com/prometheus/prometheus/blob/0ab95536115adfe50af249d36d73674be694ca3f/storage/remote/codec.go#L669-L688
func HistogramProtoToFloatHistogram(hp Histogram) *histogram.FloatHistogram {
	if hp.IsFloatHistogram() {
		panic("HistogramProtoToFloatHistogram called with a float histogram")
	}
	return &histogram.FloatHistogram{
		CounterResetHint: histogram.CounterResetHint(hp.ResetHint),
		Schema:           hp.Schema,
		ZeroThreshold:    hp.ZeroThreshold,
		ZeroCount:        float64(hp.GetZeroCountInt()),
		Count:            float64(hp.GetCountInt()),
		Sum:              hp.Sum,
		PositiveSpans:    spansProtoToSpans(hp.GetPositiveSpans()),
		PositiveBuckets:  deltasToCounts(hp.GetPositiveDeltas()),
		NegativeSpans:    spansProtoToSpans(hp.GetNegativeSpans()),
		NegativeBuckets:  deltasToCounts(hp.GetNegativeDeltas()),
	}
}

func spansProtoToSpans(s []BucketSpan) []histogram.Span {
	spans := make([]histogram.Span, len(s))
	for i := 0; i < len(s); i++ {
		spans[i] = histogram.Span{Offset: s[i].Offset, Length: s[i].Length}
	}

	return spans
}

func deltasToCounts(deltas []int64) []float64 {
	counts := make([]float64, len(deltas))
	var cur float64
	for i, d := range deltas {
		cur += float64(d)
		counts[i] = cur
	}
	return counts
}

// Copied from https://github.com/prometheus/prometheus/blob/0ab95536115adfe50af249d36d73674be694ca3f/storage/remote/codec.go#L709-L723
func HistogramToHistogramProto(timestamp int64, h *histogram.Histogram) Histogram {
	return Histogram{
		Count:          &Histogram_CountInt{CountInt: h.Count},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &Histogram_ZeroCountInt{ZeroCountInt: h.ZeroCount},
		NegativeSpans:  spansToSpansProto(h.NegativeSpans),
		NegativeDeltas: h.NegativeBuckets,
		PositiveSpans:  spansToSpansProto(h.PositiveSpans),
		PositiveDeltas: h.PositiveBuckets,
		ResetHint:      Histogram_ResetHint(h.CounterResetHint),
		Timestamp:      timestamp,
	}
}

// Copied from https://github.com/prometheus/prometheus/blob/0ab95536115adfe50af249d36d73674be694ca3f/storage/remote/codec.go#L725-L739
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

func spansToSpansProto(s []histogram.Span) []BucketSpan {
	spans := make([]BucketSpan, len(s))
	for i := 0; i < len(s); i++ {
		spans[i] = BucketSpan{Offset: s[i].Offset, Length: s[i].Length}
	}

	return spans
}
