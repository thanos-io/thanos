// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"

	"capnproto.org/go/capnp/v3"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestMarshalWriteRequest(t *testing.T) {
	testHistogram := &histogram.Histogram{
		Count:         12,
		ZeroCount:     2,
		ZeroThreshold: 0.001,
		Sum:           18.4,
		Schema:        1,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		PositiveBuckets: []int64{1, 1, -1, 0},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		NegativeBuckets: []int64{1, 1, -1, 0},
	}

	wreq := storepb.WriteRequest{
		Tenant: "example-tenant",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "up"},
					{Name: "job", Value: "prometheus"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 1},
					{Timestamp: 2, Value: 2},
				},
				Histograms: []prompb.Histogram{
					prompb.HistogramToHistogramProto(1, testHistogram),
					prompb.FloatHistogramToHistogramProto(2, tsdbutil.GenerateTestFloatHistogram(2)),
				},
				Exemplars: []prompb.Exemplar{
					{
						Labels:    []labelpb.ZLabel{{Name: "traceID", Value: "1234"}},
						Value:     10,
						Timestamp: 14,
					},
				},
			},
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "up"},
					{Name: "job", Value: "thanos"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 3, Value: 3},
					{Timestamp: 4, Value: 4},
				},
			},
		},
	}
	b, err := Marshal(wreq.Tenant, wreq.Timeseries)
	require.NoError(t, err)

	msg, err := capnp.Unmarshal(b)
	require.NoError(t, err)

	wr, err := ReadRootWriteRequest(msg)
	require.NoError(t, err)

	tenant, err := wr.Tenant()
	require.NoError(t, err)
	require.Equal(t, wreq.Tenant, tenant)

	series, err := wr.TimeSeries()
	require.NoError(t, err)
	require.Equal(t, len(wreq.Timeseries), series.Len())

	var (
		i      int
		actual Series
	)
	request, err := NewRequest(wr)
	require.NoError(t, err)
	for request.Next() {
		require.NoError(t, request.At(&actual))
		expected := wreq.Timeseries[i]

		t.Run("test_labels", func(t *testing.T) {
			builder := labels.ScratchBuilder{}
			for _, lbl := range expected.Labels {
				builder.Add(lbl.Name, lbl.Value)
			}
			builder.Sort()
			require.Equal(t, builder.Labels(), actual.Labels, fmt.Sprintf("incorrect series labels at %d", i))
		})
		t.Run("test_float_samples", func(t *testing.T) {
			expectedSamples := make([]FloatSample, 0)
			for _, s := range expected.Samples {
				expectedSamples = append(expectedSamples, FloatSample{
					Value:     s.Value,
					Timestamp: s.Timestamp,
				})
			}
			require.Equal(t, expectedSamples, actual.Samples, fmt.Sprintf("incorrect series samples at %d", i))
		})
		t.Run("test_histogram_samples", func(t *testing.T) {
			for i, hs := range expected.Histograms {
				require.Equal(t, hs.Timestamp, actual.Histograms[i].Timestamp)
				if hs.IsFloatHistogram() {
					fh := prompb.FloatHistogramProtoToFloatHistogram(hs)
					require.Equal(t, fh, actual.Histograms[i].FloatHistogram)
				} else {
					h := prompb.HistogramProtoToHistogram(hs)
					require.Equal(t, h, actual.Histograms[i].Histogram)
				}
			}
		})
		t.Run("test_exemplars", func(t *testing.T) {
			for i, ex := range expected.Exemplars {
				require.Equal(t, labelpb.ZLabelsToPromLabels(ex.Labels), actual.Exemplars[i].Labels)
				require.Equal(t, ex.Timestamp, actual.Exemplars[i].Ts)
				require.Equal(t, ex.Value, actual.Exemplars[i].Value)
			}
		})

		i++
	}
}

func TestMarshalWithMultipleHistogramSeries(t *testing.T) {
	wreq := storepb.WriteRequest{
		Tenant: "example-tenant",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "job", Value: "prometheus-1"},
				},
				Histograms: []prompb.Histogram{
					prompb.HistogramToHistogramProto(1, &histogram.Histogram{}),
					prompb.HistogramToHistogramProto(1, tsdbutil.GenerateTestHistogram(1)),
					prompb.FloatHistogramToHistogramProto(2, tsdbutil.GenerateTestFloatHistogram(2)),
				},
			},
			{
				Labels: []labelpb.ZLabel{
					{Name: "job", Value: "prometheus-2"},
				},
				Histograms: []prompb.Histogram{
					prompb.HistogramToHistogramProto(1, tsdbutil.GenerateTestHistogram(1)),
					prompb.FloatHistogramToHistogramProto(2, tsdbutil.GenerateTestFloatHistogram(2)),
					prompb.HistogramToHistogramProto(1, &histogram.Histogram{}),
				},
			},
		},
	}
	b, err := Marshal(wreq.Tenant, wreq.Timeseries)
	require.NoError(t, err)

	msg, err := capnp.Unmarshal(b)
	require.NoError(t, err)

	wr, err := ReadRootWriteRequest(msg)
	require.NoError(t, err)

	tenant, err := wr.Tenant()
	require.NoError(t, err)
	require.Equal(t, wreq.Tenant, tenant)

	series, err := wr.TimeSeries()
	require.NoError(t, err)
	require.Equal(t, len(wreq.Timeseries), series.Len())
	var (
		current Series

		readHistograms      []*histogram.Histogram
		readFloatHistograms []*histogram.FloatHistogram
	)
	request, err := NewRequest(wr)
	require.NoError(t, err)

	for request.Next() {
		require.NoError(t, request.At(&current))
		for _, h := range current.Histograms {
			if h.FloatHistogram != nil {
				readFloatHistograms = append(readFloatHistograms, h.FloatHistogram)
			} else {
				readHistograms = append(readHistograms, h.Histogram)
			}
		}
	}
	var (
		histograms      []*histogram.Histogram
		floatHistograms []*histogram.FloatHistogram
	)
	for _, ts := range wreq.Timeseries {
		for _, h := range ts.Histograms {
			if h.IsFloatHistogram() {
				floatHistograms = append(floatHistograms, prompb.FloatHistogramProtoToFloatHistogram(h))
			} else {
				histograms = append(histograms, prompb.HistogramProtoToHistogram(h))
			}
		}
	}
	require.Equal(t, histograms, readHistograms)
	require.Equal(t, floatHistograms, readFloatHistograms)
}
