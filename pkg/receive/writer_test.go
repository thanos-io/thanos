// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

func TestWriter(t *testing.T) {
	now := model.Now()
	lbls := []labelpb.ZLabel{{Name: "__name__", Value: "test"}}
	tests := map[string]struct {
		reqs             []*prompb.WriteRequest
		expectedErr      error
		expectedIngested []prompb.TimeSeries
		maxExemplars     int64
		opts             *WriterOptions
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
		"should succeed when sample timestamp is NOT too far in the future": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels:  lbls,
							Samples: []prompb.Sample{{Value: 1, Timestamp: int64(now)}},
						},
					},
				},
			},
			expectedErr: nil,
			expectedIngested: []prompb.TimeSeries{
				{
					Labels:  lbls,
					Samples: []prompb.Sample{{Value: 1, Timestamp: int64(now)}},
				},
			},
			opts: &WriterOptions{TooFarInFutureTimeWindow: 30 * int64(time.Second)},
		},
		"should error out when sample timestamp is too far in the future": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: lbls,
							// A sample with a very large timestamp in year 5138 (milliseconds)
							Samples: []prompb.Sample{{Value: 1, Timestamp: 99999999999999}},
						},
					},
				},
			},
			expectedErr: errors.Wrapf(storage.ErrOutOfBounds, "add 1 samples"),
			opts:        &WriterOptions{TooFarInFutureTimeWindow: 10000},
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
								prompb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(0)),
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
						prompb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(0)),
					},
				},
			},
		},
		"should succeed on float histogram with valid labels": {
			reqs: []*prompb.WriteRequest{
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
							Histograms: []prompb.Histogram{
								prompb.FloatHistogramToHistogramProto(10, tsdbutil.GenerateTestFloatHistogram(1)),
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
						prompb.FloatHistogramToHistogramProto(10, tsdbutil.GenerateTestFloatHistogram(1)),
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
								prompb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(0)),
							},
						},
					},
				},
				{
					Timeseries: []prompb.TimeSeries{
						{
							Labels: append(lbls, labelpb.ZLabel{Name: "a", Value: "1"}, labelpb.ZLabel{Name: "b", Value: "2"}),
							Histograms: []prompb.Histogram{
								prompb.HistogramToHistogramProto(9, tsdbutil.GenerateTestHistogram(0)),
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
						prompb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(0)),
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

			app, err := m.TenantAppendable(tenancy.DefaultTenant)
			testutil.Ok(t, err)

			testutil.Ok(t, runutil.Retry(1*time.Second, ctx.Done(), func() error {
				_, err = app.Appender(context.Background())
				return err
			}))

			w := NewWriter(logger, m, testData.opts)

			for idx, req := range testData.reqs {
				err = w.Write(context.Background(), tenancy.DefaultTenant, req)

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

	b.Run("without interning", func(b *testing.B) {
		w := NewWriter(logger, m, &WriterOptions{Intern: false})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			testutil.Ok(b, w.Write(ctx, "foo", wreq))
		}
	})

	b.Run("with interning", func(b *testing.B) {
		w := NewWriter(logger, m, &WriterOptions{Intern: true})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			testutil.Ok(b, w.Write(ctx, "foo", wreq))
		}
	})

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
			ts[j].Histograms = []prompb.Histogram{prompb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(0))}
			continue
		}

		ts[j].Samples = []prompb.Sample{{Value: 1, Timestamp: 10}}
	}

	return ts
}
