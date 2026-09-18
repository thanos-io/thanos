// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
)

func TestNativeHistogramDedup(t *testing.T) {
	cases := []struct {
		samples []sample
		tcase   string
	}{
		{
			tcase: "unknown counter reset chunk",
			samples: []sample{
				{t: 10000, h: makeHistWithHint(1, histogram.UnknownCounterReset)},
				{t: 20000, h: makeHistWithHint(2, histogram.UnknownCounterReset)},
			},
		},
		{
			tcase: "not counter reset chunk",
			samples: []sample{
				{t: 10000, h: makeHistWithHint(1, histogram.NotCounterReset)},
				{t: 20000, h: makeHistWithHint(2, histogram.NotCounterReset)},
			},
		},
		{
			tcase: "counter reset chunk",
			samples: []sample{
				{t: 10000, h: makeHistWithHint(1, histogram.CounterReset)},
				{t: 20000, h: makeHistWithHint(2, histogram.NotCounterReset)},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.tcase, func(t *testing.T) {
			// When the first sample is read, the counter reset hint for the second sample
			// does not need to be reset.
			t.Run("read_first_sample", func(t *testing.T) {
				it := newHistogramResetDetector(newMockedSeriesIterator(c.samples))
				it.Next()
				_, h := it.AtHistogram(nil)
				testutil.Equals(t, c.samples[0].h.CounterResetHint, h.CounterResetHint)

				it.Next()
				_, h = it.AtHistogram(nil)
				testutil.Equals(t, c.samples[1].h.CounterResetHint, h.CounterResetHint)
			})

			// When the first sample is not read, the counter reset hint for the second
			// sample should be reset.
			t.Run("skip_first_sample", func(t *testing.T) {
				it := newHistogramResetDetector(newMockedSeriesIterator(c.samples))
				it.Next()
				it.Next()
				_, h := it.AtHistogram(nil)
				testutil.Equals(t, h.CounterResetHint, histogram.UnknownCounterReset)
			})
		})
	}
}

func makeHistWithHint(i int, hint histogram.CounterResetHint) *histogram.Histogram {
	h := tsdbutil.GenerateTestHistogram(i)
	h.CounterResetHint = hint
	return h
}
