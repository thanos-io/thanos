// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package statuspb

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestMergeTSDBStatisticsEntry(t *testing.T) {
	for _, tc := range []struct {
		a, b *TSDBStatisticsEntry
		exp  *TSDBStatisticsEntry
	}{
		{
			a:   &TSDBStatisticsEntry{},
			b:   &TSDBStatisticsEntry{},
			exp: &TSDBStatisticsEntry{},
		},
		{
			a: &TSDBStatisticsEntry{
				HeadStatistics: HeadStatistics{
					NumSeries:     20,
					NumLabelPairs: 10,
					MinTime:       5,
					MaxTime:       100,
				},
				SeriesCountByMetricName: []Statistic{
					{
						Name:  "__name__",
						Value: 40,
					},
					{
						Name:  "job",
						Value: 30,
					},
					{
						Name:  "instance",
						Value: 20,
					},
					{
						Name:  "service",
						Value: 5,
					},
				},
				LabelValueCountByLabelName: []Statistic{
					{
						Name:  "__name__",
						Value: 40,
					},
					{
						Name:  "job",
						Value: 30,
					},
					{
						Name:  "instance",
						Value: 20,
					},
					{
						Name:  "service",
						Value: 5,
					},
				},
				MemoryInBytesByLabelName: []Statistic{
					{
						Name:  "__name__",
						Value: 4000,
					},
					{
						Name:  "job",
						Value: 3000,
					},
					{
						Name:  "instance",
						Value: 2000,
					},
					{
						Name:  "service",
						Value: 500,
					},
				},
				SeriesCountByLabelValuePair: []Statistic{
					{
						Name:  "job=test",
						Value: 40,
					},
					{
						Name:  "instance=localhost:9090",
						Value: 30,
					},
					{
						Name:  "service=web",
						Value: 20,
					},
					{
						Name:  "__name__=up",
						Value: 5,
					},
				},
			},
			b: &TSDBStatisticsEntry{
				HeadStatistics: HeadStatistics{
					NumSeries:     20,
					NumLabelPairs: 10,
					MinTime:       10,
					MaxTime:       200,
				},
				SeriesCountByMetricName: []Statistic{
					{
						Name:  "instance",
						Value: 30,
					},
					{
						Name:  "__name__",
						Value: 20,
					},
					{
						Name:  "job",
						Value: 15,
					},
					{
						Name:  "role",
						Value: 1,
					},
				},
				LabelValueCountByLabelName: []Statistic{
					{
						Name:  "instance",
						Value: 30,
					},
					{
						Name:  "__name__",
						Value: 20,
					},
					{
						Name:  "job",
						Value: 15,
					},
					{
						Name:  "role",
						Value: 1,
					},
				},
				MemoryInBytesByLabelName: []Statistic{
					{
						Name:  "instance",
						Value: 3000,
					},
					{
						Name:  "__name__",
						Value: 2000,
					},
					{
						Name:  "job",
						Value: 1500,
					},
					{
						Name:  "role",
						Value: 100,
					},
				},
				SeriesCountByLabelValuePair: []Statistic{
					{
						Name:  "job=test",
						Value: 30,
					},
					{
						Name:  "instance=localhost:9091",
						Value: 20,
					},
					{
						Name:  "service=web",
						Value: 15,
					},
					{
						Name:  "role=receiver",
						Value: 1,
					},
				},
			},
			exp: &TSDBStatisticsEntry{
				HeadStatistics: HeadStatistics{
					NumSeries:     40,
					NumLabelPairs: 20,
					MinTime:       5,
					MaxTime:       200,
				},
				SeriesCountByMetricName: []Statistic{
					{
						Name:  "__name__",
						Value: 60,
					},
					{
						Name:  "instance",
						Value: 50,
					},
					{
						Name:  "job",
						Value: 45,
					},
					{
						Name:  "service",
						Value: 5,
					},
					{
						Name:  "role",
						Value: 1,
					},
				},
				LabelValueCountByLabelName: []Statistic{
					{
						Name:  "__name__",
						Value: 40,
					},
					{
						Name:  "instance",
						Value: 30,
					},
					{
						Name:  "job",
						Value: 30,
					},
					{
						Name:  "service",
						Value: 5,
					},
					{
						Name:  "role",
						Value: 1,
					},
				},
				MemoryInBytesByLabelName: []Statistic{
					{
						Name:  "__name__",
						Value: 6000,
					},
					{
						Name:  "instance",
						Value: 5000,
					},
					{
						Name:  "job",
						Value: 4500,
					},
					{
						Name:  "service",
						Value: 500,
					},
					{
						Name:  "role",
						Value: 100,
					},
				},
				SeriesCountByLabelValuePair: []Statistic{
					{
						Name:  "job=test",
						Value: 70,
					},
					{
						Name:  "service=web",
						Value: 35,
					},
					{
						Name:  "instance=localhost:9090",
						Value: 30,
					},
					{
						Name:  "instance=localhost:9091",
						Value: 20,
					},
					{
						Name:  "__name__=up",
						Value: 5,
					},
					{
						Name:  "role=receiver",
						Value: 1,
					},
				},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			tc.a.Merge(tc.b)
			testutil.Equals(t, tc.exp, tc.a)
		})
	}
}
