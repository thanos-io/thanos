// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestWriter(t *testing.T) {
	lbls := []labelpb.ZLabel{{Name: "__name__", Value: "test"}}
	tests := map[string]struct {
		reqs             []*prompb.WriteRequest
		expectedErr      error
		expectedIngested []prompb.TimeSeries
		maxExemplars     int64
	}{
		"should error out on series with no labels": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						},
						{
							Labels:  []labelpb.ZLabel{{Name: "__name__", Value: ""}},
							Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						},
					},
				},
			},
			expectedErr: errors.Wrapf(labelpb.ErrEmptyLabels, "add 2 series"),
		},
		"should succeed on series with valid labels": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
							Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						},
					},
				},
			},
			expectedErr: nil,
			expectedIngested: []prompb.TimeSeries{
				{
					Labels:  append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
					Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
				},
			},
		},
		"should error out and skip series with out-of-order labels": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "1"}, labelpb.ZLabel{Name: "Z", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
							Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						},
					},
				},
			},
			expectedErr: errors.Wrapf(labelpb.ErrOutOfOrderLabels, "add 1 series"),
		},
		"should error out and skip series with duplicate labels": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}, labelpb.ZLabel{Name: "z", Value: "1"}),
							Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						},
					},
				},
			},
			expectedErr: errors.Wrapf(labelpb.ErrDuplicateLabels, "add 1 series"),
		},
		"should error out and skip series with out-of-order labels; accept series with valid labels": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  append(lbls, labelpb.ZLabel{Name: "A", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
							Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						},
						{
							Labels:  append(lbls, labelpb.ZLabel{Name: "c", Value: "1"}, labelpb.ZLabel{Name: "d", Value: "2"}),
							Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						},
						{
							Labels:  append(lbls, labelpb.ZLabel{Name: "E", Value: "1"}, labelpb.ZLabel{Name: "f", Value: "2"}),
							Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						},
					},
				},
			},
			expectedErr: errors.Wrapf(labelpb.ErrOutOfOrderLabels, "add 2 series"),
			expectedIngested: []prompb.TimeSeries{
				{
					Labels:  append(lbls, labelpb.ZLabel{Name: "c", Value: "1"}, labelpb.ZLabel{Name: "d", Value: "2"}),
					Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
				},
			},
		},
		"should succeed on valid series with exemplars": {
			reqs: []*prompb.WriteRequest{{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: lbls,
						// Ingesting an exemplar requires a sample to create the series first.
						Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						Exemplars: []prompb.Exemplar{
							{
								Labels:    []labelpb.ZLabel{{Name: "traceID", Value: "123"}},
								Value:     111,
								Timestamp: 11,
							},
							{
								Labels:    []labelpb.ZLabel{{Name: "traceID", Value: "234"}},
								Value:     112,
								Timestamp: 12,
							},
						},
					},
				},
			}},
			expectedErr:  nil,
			maxExemplars: 2,
		},
		"should error out on valid series with out of order exemplars": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: lbls,
							// Ingesting an exemplar requires a sample to create the series first.
							Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							Exemplars: []prompb.Exemplar{
								{
									Labels:    []labelpb.ZLabel{{Name: "traceID", Value: "123"}},
									Value:     111,
									Timestamp: 11,
								},
							},
						},
					},
				},
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: lbls,
							Exemplars: []prompb.Exemplar{
								{
									Labels:    []labelpb.ZLabel{{Name: "traceID", Value: "1234"}},
									Value:     111,
									Timestamp: 10,
								},
							},
						},
					},
				},
			},
			expectedErr:  errors.Wrapf(storage.ErrOutOfOrderExemplar, "add 1 exemplars"),
			maxExemplars: 2,
		},
		"should error out when exemplar label length exceeds the limit": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: lbls,
							// Ingesting an exemplar requires a sample to create the series first.
							Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							Exemplars: []prompb.Exemplar{
								{
									Labels:    []labelpb.ZLabel{{Name: strings.Repeat("a", exemplar.ExemplarMaxLabelSetLength), Value: "1"}},
									Value:     111,
									Timestamp: 11,
								},
							},
						},
					},
				},
			},
			expectedErr:  errors.Wrapf(storage.ErrExemplarLabelLength, "add 1 exemplars"),
			maxExemplars: 2,
		},
		"should succeed on histogram with valid labels": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
							Histograms: []prompb.Histogram{
								histogramToHistogramProto(9, testHistogram()),
							},
						},
					},
				},
			},
			expectedErr: nil,
			expectedIngested: []prompb.TimeSeries{
				{
					Labels: append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
					Histograms: []prompb.Histogram{
						histogramToHistogramProto(10, testHistogram()),
					},
				},
			},
		},
		"should error out on valid histograms with out of order histogram": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
							Histograms: []prompb.Histogram{
								histogramToHistogramProto(10, testHistogram()),
							},
						},
					},
				},
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
							Histograms: []prompb.Histogram{
								histogramToHistogramProto(9, testHistogram()),
							},
						},
					},
				},
			},
			expectedErr: errors.Wrapf(storage.ErrOutOfOrderSample, "add 1 samples"),
			expectedIngested: []prompb.TimeSeries{
				{
					Labels: append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
					Histograms: []prompb.Histogram{
						histogramToHistogramProto(10, testHistogram()),
					},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			dir := t.TempDir()
			logger := log.NewNopLogger()

			m := NewMultiTSDB(dir, logger, prometheus.NewRegistry(), &tsdb.Options{
				MinBlockDuration:       (2 * time.Hour).Milliseconds(),
				MaxBlockDuration:       (2 * time.Hour).Milliseconds(),
				RetentionDuration:      (6 * time.Hour).Milliseconds(),
				NoLockfile:             true,
				MaxExemplars:           testData.maxExemplars,
				EnableExemplarStorage:  true,
				EnableNativeHistograms: true,
			},
				labels.FromStrings("replica", "01"),
				"tenant_id",
				nil,
				false,
				metadata.NoneFunc,
			)
			t.Cleanup(func() { testutil.Ok(t, m.Close()) })

			testutil.Ok(t, m.Flush())
			testutil.Ok(t, m.Open())

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			app, err := m.TenantAppendable(DefaultTenant)
			testutil.Ok(t, err)

			testutil.Ok(t, runutil.Retry(1*time.Second, ctx.Done(), func() error {
				_, err = app.Appender(context.Background())
				return err
			}))

			w := NewWriter(logger, m)

			for idx, req := range testData.reqs {
				err = w.Write(context.Background(), DefaultTenant, req)

				// We expect no error on any request except the last one
				// which may error (and in that case we assert on it).
				if testData.expectedErr == nil || idx < len(testData.reqs)-1 {
					testutil.Ok(t, err)
				} else {
					testutil.NotOk(t, err)
					testutil.Equals(t, testData.expectedErr.Error(), err.Error())
				}
			}

			// On each expected series, assert we have a ref available.
			a, err := app.Appender(context.Background())
			testutil.Ok(t, err)
			gr := a.(storage.GetRef)

			for _, ts := range testData.expectedIngested {
				l := labelpb.ZLabelsToPromLabels(ts.Labels)
				ref, _ := gr.GetRef(l, l.Hash())
				testutil.Assert(t, ref != 0, fmt.Sprintf("appender should have reference to series %v", ts))
			}
		})
	}
}

func BenchmarkWriterTimeSeriesWithSingleLabel_10(b *testing.B)   { benchmarkWriter(b, 1, 10, false) }
func BenchmarkWriterTimeSeriesWithSingleLabel_100(b *testing.B)  { benchmarkWriter(b, 1, 100, false) }
func BenchmarkWriterTimeSeriesWithSingleLabel_1000(b *testing.B) { benchmarkWriter(b, 1, 1000, false) }

func BenchmarkWriterTimeSeriesWith10Labels_10(b *testing.B)   { benchmarkWriter(b, 10, 10, false) }
func BenchmarkWriterTimeSeriesWith10Labels_100(b *testing.B)  { benchmarkWriter(b, 10, 100, false) }
func BenchmarkWriterTimeSeriesWith10Labels_1000(b *testing.B) { benchmarkWriter(b, 10, 1000, false) }

func BenchmarkWriterTimeSeriesWithHistogramsWithSingleLabel_10(b *testing.B) {
	benchmarkWriter(b, 1, 10, true)
}
func BenchmarkWriterTimeSeriesWithHistogramsWithSingleLabel_100(b *testing.B) {
	benchmarkWriter(b, 1, 100, true)
}
func BenchmarkWriterTimeSeriesWithHistogramsWithSingleLabel_1000(b *testing.B) {
	benchmarkWriter(b, 1, 1000, true)
}

func BenchmarkWriterTimeSeriesWithHistogramsWith10Labels_10(b *testing.B) {
	benchmarkWriter(b, 10, 10, true)
}
func BenchmarkWriterTimeSeriesWithHistogramsWith10Labels_100(b *testing.B) {
	benchmarkWriter(b, 10, 100, true)
}
func BenchmarkWriterTimeSeriesWithHistogramsWith10Labels_1000(b *testing.B) {
	benchmarkWriter(b, 10, 1000, true)
}

func benchmarkWriter(b *testing.B, labelsNum int, seriesNum int, generateHistograms bool) {
	dir := b.TempDir()
	logger := log.NewNopLogger()

	m := NewMultiTSDB(dir, logger, prometheus.NewRegistry(), &tsdb.Options{
		MinBlockDuration:       (2 * time.Hour).Milliseconds(),
		MaxBlockDuration:       (2 * time.Hour).Milliseconds(),
		RetentionDuration:      (6 * time.Hour).Milliseconds(),
		NoLockfile:             true,
		MaxExemplars:           0,
		EnableExemplarStorage:  true,
		EnableNativeHistograms: generateHistograms,
	},
		labels.FromStrings("replica", "01"),
		"tenant_id",
		nil,
		false,
		metadata.NoneFunc,
	)
	b.Cleanup(func() { testutil.Ok(b, m.Close()) })

	testutil.Ok(b, m.Flush())
	testutil.Ok(b, m.Open())

	app, err := m.TenantAppendable("foo")
	testutil.Ok(b, err)

	w := NewWriter(logger, m)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testutil.Ok(b, runutil.Retry(1*time.Second, ctx.Done(), func() error {
		_, err = app.Appender(context.Background())
		return err
	}))

	timeSeries := generateLabelsAndSeries(labelsNum, seriesNum, generateHistograms)

	wreq := &prompb.WriteRequest{
		Timeseries: timeSeries,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		testutil.Ok(b, w.Write(ctx, "foo", wreq))
	}
}

// generateLabelsAndSeries generates time series for benchmark with specified number of labels.
// Although in this method we're reusing samples with same value and timestamp, Prometheus actually allows us to provide exact
// duplicates without error (see comment https://github.com/prometheus/prometheus/blob/release-2.37/tsdb/head_append.go#L316).
// This also means the sample won't be appended, which means the overhead of appending additional samples to head is not
// reflected in the benchmark, but should still capture the performance of receive writer.
func generateLabelsAndSeries(numLabels int, numSeries int, generateHistograms bool) []prompb.TimeSeries {
	// Generate some labels first.
	l := make([]labelpb.ZLabel, 0, numLabels)
	l = append(l, labelpb.ZLabel{Name: "__name__", Value: "test"})
	for i := 0; i < numLabels; i++ {
		l = append(l, labelpb.ZLabel{Name: fmt.Sprintf("label_%s", string(rune('a'+i))), Value: fmt.Sprintf("%d", i)})
	}

	ts := make([]prompb.TimeSeries, numSeries)
	for j := 0; j < numSeries; j++ {
		ts[j] = prompb.TimeSeries{
			Labels: l,
		}

		if generateHistograms {
			ts[j].Histograms = []prompb.Histogram{histogramToHistogramProto(10, testHistogram())}
			continue
		}

		ts[j].Samples = []prompb.Sample{{Value: 1, Timestamp: 10}}
	}

	return ts
}

func testHistogram() *histogram.Histogram {
	return &histogram.Histogram{
		Count:         5,
		ZeroCount:     2,
		Sum:           18.4,
		ZeroThreshold: 0.1,
		Schema:        1,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		PositiveBuckets: []int64{1, 1, -1, 0}, // counts: 1, 2, 1, 1 (total 5)
	}
}

func histogramToHistogramProto(timestamp int64, h *histogram.Histogram) prompb.Histogram {
	return prompb.Histogram{
		Count:          &prompb.Histogram_CountInt{CountInt: h.Count},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &prompb.Histogram_ZeroCountInt{ZeroCountInt: h.ZeroCount},
		NegativeSpans:  spansToSpansProto(h.NegativeSpans),
		NegativeDeltas: h.NegativeBuckets,
		PositiveSpans:  spansToSpansProto(h.PositiveSpans),
		PositiveDeltas: h.PositiveBuckets,
		Timestamp:      timestamp,
	}
}

func spansToSpansProto(s []histogram.Span) []*prompb.BucketSpan {
	spans := make([]*prompb.BucketSpan, len(s))
	for i := 0; i < len(s); i++ {
		spans[i] = &prompb.BucketSpan{Offset: s[i].Offset, Length: s[i].Length}
	}

	return spans
}
