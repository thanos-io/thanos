// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestWriter(t *testing.T) {
	lbls := []labelpb.ZLabel{{Name: "__name__", Value: "test"}}
	tests := map[string]struct {
		reqs             []*prompb.WriteRequest
		expectedErr      error
		expectedIngested []prompb.TimeSeries
		maxExemplars     int64
	}{
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
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "test")
			testutil.Ok(t, err)
			defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

			logger := log.NewNopLogger()

			m := NewMultiTSDB(dir, logger, prometheus.NewRegistry(), &tsdb.Options{
				MinBlockDuration:      (2 * time.Hour).Milliseconds(),
				MaxBlockDuration:      (2 * time.Hour).Milliseconds(),
				RetentionDuration:     (6 * time.Hour).Milliseconds(),
				NoLockfile:            true,
				MaxExemplars:          testData.maxExemplars,
				EnableExemplarStorage: true,
			},
				labels.FromStrings("replica", "01"),
				"tenant_id",
				nil,
				false,
				metadata.NoneFunc,
			)
			defer func() { testutil.Ok(t, m.Close()) }()

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
		})
	}
}

func BenchmarkWriterTimeSeriesWithSingleLabel_10(b *testing.B)   { benchmarkWriter(b, 1, 10) }
func BenchmarkWriterTimeSeriesWithSingleLabel_100(b *testing.B)  { benchmarkWriter(b, 1, 100) }
func BenchmarkWriterTimeSeriesWithSingleLabel_1000(b *testing.B) { benchmarkWriter(b, 1, 1000) }

func BenchmarkWriterTimeSeriesWith10Labels_10(b *testing.B)   { benchmarkWriter(b, 10, 10) }
func BenchmarkWriterTimeSeriesWith10Labels_100(b *testing.B)  { benchmarkWriter(b, 10, 100) }
func BenchmarkWriterTimeSeriesWith10Labels_1000(b *testing.B) { benchmarkWriter(b, 10, 1000) }

func benchmarkWriter(b *testing.B, labelsNum int, seriesNum int) {
	dir, err := ioutil.TempDir("", "bench-writer")
	testutil.Ok(b, err)
	defer func() { testutil.Ok(b, os.RemoveAll(dir)) }()

	logger := log.NewNopLogger()

	m := NewMultiTSDB(dir, logger, prometheus.NewRegistry(), &tsdb.Options{
		MinBlockDuration:      (2 * time.Hour).Milliseconds(),
		MaxBlockDuration:      (2 * time.Hour).Milliseconds(),
		RetentionDuration:     (6 * time.Hour).Milliseconds(),
		NoLockfile:            true,
		MaxExemplars:          0,
		EnableExemplarStorage: true,
	},
		labels.FromStrings("replica", "01"),
		"tenant_id",
		nil,
		false,
		metadata.NoneFunc,
	)
	defer func() { testutil.Ok(b, m.Close()) }()

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

	timeSeries := generateLabelsAndSeries(labelsNum, seriesNum)

	wreq := &prompb.WriteRequest{
		Timeseries: timeSeries,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = w.Write(ctx, "foo", wreq)
	}
}

func generateLabelsAndSeries(numLabels int, numSeries int) []prompb.TimeSeries {
	// Generate some labels first.
	l := make([]labelpb.ZLabel, 0, numLabels)
	l = append(l, labelpb.ZLabel{Name: "__name__", Value: "test"})
	for i := 0; i < numLabels; i++ {
		l = append(l, labelpb.ZLabel{Name: fmt.Sprintf("label_%q", rune('a'-1+i)), Value: fmt.Sprintf("%d", i)})
	}

	ts := make([]prompb.TimeSeries, 0, numSeries)
	for j := 0; j < numSeries; j++ {
		ts = append(ts, prompb.TimeSeries{Labels: l, Samples: []prompb.Sample{{Value: 1, Timestamp: 10}}})
	}

	return ts
}
