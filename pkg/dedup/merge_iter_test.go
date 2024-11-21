// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dedup

import (
	"math"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestIteratorEdgeCases(t *testing.T) {
	ms := NewMergedSeries(labels.Labels{}, []storage.Series{}, "")
	it := ms.Iterator(nil)
	testutil.Ok(t, it.Err())
	testutil.Equals(t, int64(math.MinInt64), it.AtT())
	testutil.Equals(t, chunkenc.ValNone, it.Next())
	testutil.Ok(t, it.Err())
}

func TestMergedSeriesIterator(t *testing.T) {
	for _, tcase := range []struct {
		name      string
		input     []series
		exp       []series
		isCounter bool
	}{
		// copied from dedup_test.go to make sure the result is correct if no overlaps
		{
			name: "Single dedup label",
			input: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{200000, 5}, {210000, 6}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
		},
		{
			name: "Multi dedup label",
			input: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{200000, 5}, {210000, 6}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset:    labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
		},
		{
			name: "Multi dedup label - some series don't have all dedup labels",
			input: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					samples: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
		},
		// additional tests
		{
			name:  "empty",
			input: []series{},
			exp:   []series{},
		},
		{
			name: "Multi dedup labels - data points absent",
			input: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {30000, 3}, {40000, 4}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {20000, 2}, {30000, 3}, {50000, 5}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {80000, 10}},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {20000, 2}, {30000, 3}, {40000, 4}, {50000, 5}, {80000, 10}},
				},
			},
		},
		{
			name: "Avoid corrupt Values",
			input: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {20000, 23492}, {30000, 3}, {50000, 5}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {20000, 2}, {30000, 3}, {50000, 5}},
				}, {
					lset:    labels.Labels{{Name: "a", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {20000, 2}, {30000, 3}, {50000, 5}},
				}, {
					lset:    labels.Labels{{Name: "b", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {20000, 2}, {30000, 3}, {50000, 5}},
				}, {
					lset:    labels.Labels{{Name: "b", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {20000, 2}, {30000, 3}, {50000, 5}},
				}, {
					lset:    labels.Labels{{Name: "b", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {20000, 1234}, {30000, 3}, {50000, 5}},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {20000, 2}, {30000, 3}, {50000, 5}},
				},
				{
					lset:    labels.Labels{{Name: "b", Value: "5"}, {Name: "c", Value: "6"}},
					samples: []sample{{10000, 1}, {20000, 2}, {30000, 3}, {50000, 5}},
				},
			},
		},
		{
			name: "ignore sampling interval too small",
			input: []series{
				{
					lset: labels.Labels{{Name: "a", Value: "1"}},
					samples: []sample{
						{10000, 8.0},
						{20000, 9.0},
						{50001, 9 + 1.0},
						{60000, 9 + 2.0},
						{70000, 9 + 3.0},
						{80000, 9 + 4.0},
						{90000, 9 + 5.0},
						{100000, 9 + 6.0},
					},
				}, {
					lset: labels.Labels{{Name: "a", Value: "1"}},
					samples: []sample{
						{10001, 8.0}, // Penalty 5000 will be added.
						// 20001 was app reset. No sample, because stale marker but removed by downsample.CounterSeriesIterator. Penalty 2 * (20000 - 10000) will be added.
						// 30001 no sample. Within penalty, ignored.
						{45001, 8 + 1.0}, // Smaller timestamp, this will be chosen. CurrValue = 8.5 which is smaller than last chosen value.
						{55001, 8 + 2.0},
						{65001, 8 + 3.0},
						// {Gap} app reset. No sample, because stale marker but removed by downsample.CounterSeriesIterator.
					},
				},
			},
			exp: []series{
				{
					lset:    labels.Labels{{Name: "a", Value: "1"}},
					samples: []sample{{10000, 8}, {20000, 9}, {45001, 9}, {50001, 10}, {55001, 10}, {65001, 11}, {80000, 13}, {90000, 14}, {100000, 15}},
				},
			},
		},
		{
			// Regression test against https://github.com/thanos-io/thanos/issues/2401.
			// Two counter series, when one (initially chosen) series is having hiccup (few dropped samples), while second is live.
			// This also happens when 2 replicas scrape in different time (they usually do) and one sees later counter value then the other.
			// Now, depending on what replica we look, we can see totally different counter value in total where total means
			// after accounting for counter resets. We account for that in downsample.CounterSeriesIterator, mainly because
			// we handle downsample Counter Aggregations specially (for detecting resets between chunks).
			name:      "Regression test against 2401",
			isCounter: true,
			input: []series{
				{
					lset: labels.FromStrings("a", "1"),
					samples: []sample{
						{10000, 8.0}, // Smaller timestamp, this will be chosen. CurrValue = 8.0.
						{20000, 9.0}, // Same. CurrValue = 9.0.
						// {Gap} app reset. No sample, because stale marker but removed by downsample.CounterSeriesIterator.
						{50001, 9 + 1.0}, // Next after 20000+1 has a bit higher than timestamp then in second series. Penalty 5000 will be added.
						{60000, 9 + 2.0},
						{70000, 9 + 3.0},
						{80000, 9 + 4.0},
						{90000, 9 + 5.0}, // This should be now taken, and we expect 14 to be correct value now.
						{100000, 9 + 6.0},
					},
				}, {
					lset: labels.FromStrings("a", "1"),
					samples: []sample{
						{10001, 8.0}, // Penalty 5000 will be added.
						// 20001 was app reset. No sample, because stale marker but removed by downsample.CounterSeriesIterator. Penalty 2 * (20000 - 10000) will be added.
						// 30001 no sample. Within penalty, ignored.
						{45001, 8 + 0.5}, // Smaller timestamp, this will be chosen. CurrValue = 8.5 which is smaller than last chosen value.
						{55001, 8 + 1.5},
						{65001, 8 + 2.5},
						// {Gap} app reset. No sample, because stale marker but removed by downsample.CounterSeriesIterator.
					},
				},
			},
			exp: []series{
				{
					lset:    labels.FromStrings("a", "1"),
					samples: []sample{{10000, 8}, {20000, 9}, {45001, 9}, {t: 50001, f: 10}, {55001, 10}, {65001, 11}, {t: 80000, f: 13}, {90000, 14}, {100000, 15}},
				},
			},
		},
		{
			// Same thing but not for counter should not adjust anything.
			name:      "Regression test with no counter adjustment",
			isCounter: false,
			input: []series{
				{
					lset: labels.FromStrings("a", "1"),
					samples: []sample{
						{10000, 8.0}, {20000, 9.0}, {50001, 9 + 1.0}, {60000, 9 + 2.0}, {70000, 9 + 3.0}, {80000, 9 + 4.0}, {90000, 9 + 5.0}, {100000, 9 + 6.0},
					},
				}, {
					lset: labels.FromStrings("a", "1"),
					samples: []sample{
						{10001, 8.0}, {45001, 8 + 0.5}, {55001, 8 + 1.5}, {65001, 8 + 2.5},
					},
				},
			},
			exp: []series{
				{
					lset:    labels.FromStrings("a", "1"),
					samples: []sample{{10000, 8}, {20000, 9}, {45001, 8.5}, {t: 50001, f: 10}, {55001, 9.5}, {65001, 10.5}, {t: 80000, f: 13}, {90000, 14}, {100000, 15}},
				},
			},
		},
		//{
		//	name:      "Reusable counter with resets and large gaps",
		//	isCounter: true,
		//	input: []series{
		//		{
		//			lset: labels.FromStrings("a", "1"),
		//			samples: []sample{
		//				{10000, 8.0}, {20000, 9.0}, {1050001, 1.0}, {1060001, 5.0}, {2060001, 3.0},
		//			},
		//		},
		//		{
		//			lset: labels.FromStrings("a", "1"),
		//			samples: []sample{
		//				{10000, 8.0}, {20000, 9.0}, {1050001, 1.0}, {1060001, 5.0}, {2060001, 3.0},
		//			},
		//		},
		//	},
		//	exp: []series{
		//		{
		//			lset:    labels.FromStrings("a", "1"),
		//			samples: []sample{{10000, 8.0}, {20000, 9.0}, {1050001, 10.0}, {1060001, 16.0}, {2060001, 19.0}},
		//		},
		//	},
		//},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			// If it is a counter then pass a function which expects a counter.
			// If it is a counter then pass a function which expects a counter.
			f := ""
			if tcase.isCounter {
				f = "rate"
			}
			dedupSet := NewSeriesSet(&mockedSeriesSet{series: tcase.input}, f, AlgorithmQuorum)
			var ats []storage.Series
			for dedupSet.Next() {
				ats = append(ats, dedupSet.At())
			}
			testutil.Ok(t, dedupSet.Err())
			testutil.Equals(t, len(tcase.exp), len(ats))

			for i, s := range ats {
				testutil.Equals(t, tcase.exp[i].lset, s.Labels(), "labels mismatch for series %v", i)
				res := expandSeries(t, s.Iterator(nil))
				testutil.Equals(t, tcase.exp[i].samples, res, "values mismatch for series :%v", i)
			}
		})
	}
}
