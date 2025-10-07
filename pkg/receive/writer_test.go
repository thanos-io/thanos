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
	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

func TestWriter(t *testing.T) {
	t.Parallel()

	now := model.Now()

	tests := map[string]struct {
		reqs             func() []*prompb.WriteRequest
		expectedErr      error
		expectedIngested []prompb.TimeSeries
		maxExemplars     int64
		opts             *WriterOptions
	}{
		"should error out on series with no labels": {
			reqs: func() []*prompb.WriteRequest {
				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							},
							{
								Labels:  labels.FromStrings("__name__", ""),
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							},
						},
					},
				}
			},
			expectedErr: errors.Wrapf(labelpb.ErrEmptyLabels, "add 2 series"),
		},
		"should succeed on series with valid labels": {
			reqs: func() []*prompb.WriteRequest {
				base := labels.NewScratchBuilder(0)
				base.Add("__name__", "test_metric")
				base.Add("a", "1")
				base.Add("b", "2")

				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels:  base.Labels(),
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							},
						},
					},
				}
			},
			expectedErr: nil,
			expectedIngested: []prompb.TimeSeries{
				{
					Labels:  labels.FromStrings("__name__", "test_metric", "a", "1", "b", "2"),
					Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
				},
			},
		},
		"should error out and skip series with out-of-order labels": {
			reqs: func() []*prompb.WriteRequest {
				base := labels.NewScratchBuilder(0)
				base.Add("__name__", "test_metric")
				base.Add("a", "1")
				base.Add("b", "2")
				base.Add("Z", "1")
				base.Add("b", "2")

				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels:  base.Labels(),
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							},
						},
					},
				}
			},
			expectedErr: errors.Wrapf(labelpb.ErrOutOfOrderLabels, "add 1 series"),
		},
		"should error out and skip series with duplicate labels": {
			reqs: func() []*prompb.WriteRequest {
				base := labels.NewScratchBuilder(0)
				base.Add("__name__", "test_metric")
				base.Add("a", "1")
				base.Add("b", "1")
				base.Add("b", "2")
				base.Add("z", "1")

				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels:  base.Labels(),
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							},
						},
					},
				}
			},
			expectedErr: errors.Wrapf(labelpb.ErrDuplicateLabels, "add 1 series"),
		},
		"should error out and skip series with out-of-order labels; accept series with valid labels": {
			reqs: func() []*prompb.WriteRequest {
				base := labels.NewScratchBuilder(0)
				base.Add("__name__", "test_metric")
				baseLabels := base.Labels()

				l0 := labels.NewBuilder(baseLabels)
				l0.Set("A", "1")
				l0.Set("b", "2")

				l1 := labels.NewBuilder(baseLabels)
				l1.Set("c", "1")
				l1.Set("d", "2")

				l2 := labels.NewBuilder(baseLabels)
				l2.Set("E", "1")
				l2.Set("f", "2")

				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels:  l0.Labels(),
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							},
							{
								Labels:  l1.Labels(),
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							},
							{
								Labels:  l2.Labels(),
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							},
						},
					},
				}
			},
			expectedErr: errors.Wrapf(labelpb.ErrOutOfOrderLabels, "add 2 series"),
			expectedIngested: []prompb.TimeSeries{
				{
					Labels:  labels.FromStrings("__name__", "test_metric", "c", "1", "d", "2"),
					Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
				},
			},
		},
		"should succeed when sample timestamp is NOT too far in the future": {
			reqs: func() []*prompb.WriteRequest {
				base := labels.NewScratchBuilder(0)
				base.Add("__name__", "test_metric")

				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels:  base.Labels(),
								Samples: []prompb.Sample{{Value: 1, Timestamp: int64(now)}},
							},
						},
					},
				}
			},
			expectedErr: nil,
			expectedIngested: []prompb.TimeSeries{
				{
					Labels:  labels.FromStrings("__name__", "test_metric"),
					Samples: []prompb.Sample{{Value: 1, Timestamp: int64(now)}},
				},
			},
			opts: &WriterOptions{TooFarInFutureTimeWindow: 30 * int64(time.Second)},
		},
		"should error out when sample timestamp is too far in the future": {
			reqs: func() []*prompb.WriteRequest {
				base := labels.NewScratchBuilder(0)
				base.Add("__name__", "test_metric")

				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels: base.Labels(),
								// A sample with a very large timestamp in year 5138 (milliseconds)
								Samples: []prompb.Sample{{Value: 1, Timestamp: 99999999999999}},
							},
						},
					},
				}
			},
			expectedErr: errors.Wrapf(storage.ErrOutOfBounds, "add 1 samples"),
			opts:        &WriterOptions{TooFarInFutureTimeWindow: 10000},
		},
		"should succeed on valid series with exemplars": {
			reqs: func() []*prompb.WriteRequest {
				base := labels.NewScratchBuilder(0)
				base.Add("__name__", "test_metric")

				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels: base.Labels(),
								// Ingesting an exemplar requires a sample to create the series first.
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
								Exemplars: []prompb.Exemplar{
									{
										Labels:    labels.FromStrings("traceID", "123"),
										Value:     111,
										Timestamp: 11,
									},
									{
										Labels:    labels.FromStrings("traceID", "234"),
										Value:     112,
										Timestamp: 12,
									},
								},
							},
						},
					},
				}
			},
			expectedErr:  nil,
			maxExemplars: 2,
		},
		"should error out on valid series with out of order exemplars": {
			reqs: func() []*prompb.WriteRequest {
				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels: labels.FromStrings("__name__", "test_metric"),
								// Ingesting an exemplar requires a sample to create the series first.
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
								Exemplars: []prompb.Exemplar{
									{
										Labels:    labels.FromStrings("traceID", "123"),
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
								Labels: labels.FromStrings("__name__", "test_metric"),
								Exemplars: []prompb.Exemplar{
									{
										Labels:    labels.FromStrings("traceID", "1234"),
										Value:     111,
										Timestamp: 10,
									},
								},
							},
						},
					},
				}
			},
			expectedErr:  errors.Wrapf(storage.ErrOutOfOrderExemplar, "add 1 exemplars"),
			maxExemplars: 2,
		},
		"should error out when exemplar label length exceeds the limit": {
			reqs: func() []*prompb.WriteRequest {
				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels: labels.FromStrings("__name__", "test_metric"),
								// Ingesting an exemplar requires a sample to create the series first.
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
								Exemplars: []prompb.Exemplar{
									{
										Labels:    labels.FromStrings(strings.Repeat("a", exemplar.ExemplarMaxLabelSetLength), "1"),
										Value:     111,
										Timestamp: 11,
									},
								},
							},
						},
					},
				}
			},
			expectedErr:  errors.Wrapf(storage.ErrExemplarLabelLength, "add 1 exemplars"),
			maxExemplars: 2,
		},
		"should succeed on histogram with valid labels": {
			reqs: func() []*prompb.WriteRequest {
				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels: labels.FromStrings("__name__", "test_metric", "a", "1", "b", "2"),
								Histograms: []prompb.Histogram{
									prompb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(0)),
								},
							},
						},
					},
				}
			},
			expectedErr: nil,
			expectedIngested: []prompb.TimeSeries{
				{
					Labels: labels.FromStrings("__name__", "test_metric", "a", "1", "b", "2"),
					Histograms: []prompb.Histogram{
						prompb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(0)),
					},
				},
			},
		},
		"should succeed on float histogram with valid labels": {
			reqs: func() []*prompb.WriteRequest {
				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels: labels.FromStrings("__name__", "test_metric", "a", "1", "b", "2"),
								Histograms: []prompb.Histogram{
									prompb.FloatHistogramToHistogramProto(10, tsdbutil.GenerateTestFloatHistogram(1)),
								},
							},
						},
					},
				}
			},
			expectedErr: nil,
			expectedIngested: []prompb.TimeSeries{
				{
					Labels: labels.FromStrings("__name__", "test_metric", "a", "1", "b", "2"),
					Histograms: []prompb.Histogram{
						prompb.FloatHistogramToHistogramProto(10, tsdbutil.GenerateTestFloatHistogram(1)),
					},
				},
			},
		},
		"should error out on valid histograms with out of order histogram": {
			reqs: func() []*prompb.WriteRequest {
				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels: labels.FromStrings("__name__", "test_metric", "a", "1", "b", "2"),
								Histograms: []prompb.Histogram{
									prompb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(0)),
								},
							},
						},
					},
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels: labels.FromStrings("__name__", "test_metric", "a", "1", "b", "2"),
								Histograms: []prompb.Histogram{
									prompb.HistogramToHistogramProto(9, tsdbutil.GenerateTestHistogram(0)),
								},
							},
						},
					},
				}
			},
			expectedErr: errors.Wrapf(storage.ErrOutOfOrderSample, "add 1 samples"),
			expectedIngested: []prompb.TimeSeries{
				{
					Labels: labels.FromStrings("__name__", "test_metric", "a", "1", "b", "2"),
					Histograms: []prompb.Histogram{
						prompb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(0)),
					},
				},
			},
		},
		"should succeed on series with utf-8 labels": {
			reqs: func() []*prompb.WriteRequest {
				return []*prompb.WriteRequest{
					{
						Timeseries: []prompb.TimeSeries{
							{
								Labels:  labels.FromStrings("__name__", "test_metric", "label:name", "label:value", "region:name", "region:value"),
								Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
							},
						},
					},
				}
			},
			expectedErr: nil,
			expectedIngested: []prompb.TimeSeries{
				{
					Labels:  labels.FromStrings("__name__", "test_metric", "label:name", "label:value", "region:name", "region:value"),
					Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Run("proto_writer", func(t *testing.T) {
				logger, m, app := setupMultitsdb(t, testData.maxExemplars)

				w := NewWriter(logger, m, testData.opts)

				reqs := testData.reqs()
				for idx, req := range reqs {
					err := w.Write(context.Background(), tenancy.DefaultTenant, req.Timeseries)

					// We expect no error on any request except the last one
					// which may error (and in that case we assert on it).
					if testData.expectedErr == nil || idx < len(reqs)-1 {
						testutil.Ok(t, err)
					} else {
						testutil.NotOk(t, err)
						testutil.Equals(t, testData.expectedErr.Error(), err.Error())
					}
				}

				assertWrittenData(t, app, testData.expectedIngested)
			})

			t.Run("capnproto_writer", func(t *testing.T) {
				logger, m, app := setupMultitsdb(t, testData.maxExemplars)

				opts := &CapNProtoWriterOptions{}
				if testData.opts != nil {
					opts.TooFarInFutureTimeWindow = testData.opts.TooFarInFutureTimeWindow
				}
				w := NewCapNProtoWriter(logger, m, opts)

				reqs := testData.reqs()
				for idx, req := range reqs {
					capnpReq, err := writecapnp.Build(tenancy.DefaultTenant, req.Timeseries)
					testutil.Ok(t, err)

					wr, err := writecapnp.NewRequest(capnpReq)
					testutil.Ok(t, err)
					err = w.Write(context.Background(), tenancy.DefaultTenant, wr)

					// We expect no error on any request except the last one
					// which may error (and in that case we assert on it).
					if testData.expectedErr == nil || idx < len(reqs)-1 {
						testutil.Ok(t, err)
					} else {
						testutil.NotOk(t, err)
						testutil.Equals(t, testData.expectedErr.Error(), err.Error())
					}
				}

				assertWrittenData(t, app, testData.expectedIngested)
			})
		})
	}
}

func assertWrittenData(t *testing.T, app Appendable, expectedIngested []prompb.TimeSeries) {
	// On each expected series, assert we have a ref available.
	a, err := app.Appender(context.Background())
	testutil.Ok(t, err)
	gr := a.(storage.GetRef)

	for _, ts := range expectedIngested {
		l := labels.Labels(ts.Labels)
		ref, _ := gr.GetRef(l, l.Hash())
		testutil.Assert(t, ref != 0, fmt.Sprintf("appender should have reference to series %v", ts))
	}
}

func setupMultitsdb(t *testing.T, maxExemplars int64) (log.Logger, *MultiTSDB, Appendable) {
	dir := t.TempDir()
	logger := log.NewNopLogger()

	m := NewMultiTSDB(dir, logger, prometheus.NewRegistry(), &tsdb.Options{
		MinBlockDuration:       (2 * time.Hour).Milliseconds(),
		MaxBlockDuration:       (2 * time.Hour).Milliseconds(),
		RetentionDuration:      (6 * time.Hour).Milliseconds(),
		NoLockfile:             true,
		MaxExemplars:           maxExemplars,
		EnableExemplarStorage:  true,
		EnableNativeHistograms: true,
	},
		labels.FromStrings("replica", "01"),
		"tenant_id",
		nil,
		false,
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
	return logger, m, app
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

		for b.Loop() {
			testutil.Ok(b, w.Write(ctx, "foo", wreq.Timeseries))
		}
	})

	b.Run("with interning", func(b *testing.B) {
		w := NewWriter(logger, m, &WriterOptions{Intern: true})

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			testutil.Ok(b, w.Write(ctx, "foo", wreq.Timeseries))
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
	seriesLabels := labels.NewScratchBuilder(numLabels)
	seriesLabels.Add("__name__", "test")
	for i := range numLabels {
		seriesLabels.Add(fmt.Sprintf("label_%s", string(rune('a'+i))), fmt.Sprintf("%d", i))
	}

	ts := make([]prompb.TimeSeries, numSeries)
	for j := range numSeries {
		ts[j] = prompb.TimeSeries{
			Labels: seriesLabels.Labels(),
		}

		if generateHistograms {
			ts[j].Histograms = []prompb.Histogram{prompb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(0))}
			continue
		}

		ts[j].Samples = []prompb.Sample{{Value: 1, Timestamp: 10}}
	}

	return ts
}
