// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package series

import (
	"math/rand"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestConcreteSeriesSet(t *testing.T) {
	series1 := &ConcreteSeries{
		labels:  labels.FromStrings("foo", "bar"),
		samples: []model.SamplePair{{Value: 1, Timestamp: 2}},
	}
	series2 := &ConcreteSeries{
		labels:  labels.FromStrings("foo", "baz"),
		samples: []model.SamplePair{{Value: 3, Timestamp: 4}},
	}
	c := NewConcreteSeriesSet([]storage.Series{series2, series1})
	require.True(t, c.Next())
	require.Equal(t, series1, c.At())
	require.True(t, c.Next())
	require.Equal(t, series2, c.At())
	require.False(t, c.Next())
}

func TestMatrixToSeriesSetSortsMetricLabels(t *testing.T) {
	matrix := model.Matrix{
		{
			Metric: model.Metric{
				model.MetricNameLabel: "testmetric",
				"e":                   "f",
				"a":                   "b",
				"g":                   "h",
				"c":                   "d",
			},
			Values: []model.SamplePair{{Timestamp: 0, Value: 0}},
		},
	}
	ss := MatrixToSeriesSet(matrix)
	require.True(t, ss.Next())
	require.NoError(t, ss.Err())

	l := ss.At().Labels()
	require.Equal(t, labels.Labels{
		{Name: string(model.MetricNameLabel), Value: "testmetric"},
		{Name: "a", Value: "b"},
		{Name: "c", Value: "d"},
		{Name: "e", Value: "f"},
		{Name: "g", Value: "h"},
	}, l)
}

func TestDeletedSeriesIterator(t *testing.T) {
	cs := ConcreteSeries{labels: labels.FromStrings("foo", "bar")}
	// Insert random stuff from (0, 1000).
	for i := 0; i < 1000; i++ {
		cs.samples = append(cs.samples, model.SamplePair{Timestamp: model.Time(i), Value: model.SampleValue(rand.Float64())})
	}

	cases := []struct {
		r []model.Interval
	}{
		{r: []model.Interval{{Start: 1, End: 20}}},
		{r: []model.Interval{{Start: 1, End: 10}, {Start: 12, End: 20}, {Start: 21, End: 23}, {Start: 25, End: 30}}},
		{r: []model.Interval{{Start: 1, End: 10}, {Start: 12, End: 20}, {Start: 20, End: 30}}},
		{r: []model.Interval{{Start: 1, End: 10}, {Start: 12, End: 23}, {Start: 25, End: 30}}},
		{r: []model.Interval{{Start: 1, End: 23}, {Start: 12, End: 20}, {Start: 25, End: 30}}},
		{r: []model.Interval{{Start: 1, End: 23}, {Start: 12, End: 20}, {Start: 25, End: 3000}}},
		{r: []model.Interval{{Start: 0, End: 2000}}},
		{r: []model.Interval{{Start: 500, End: 2000}}},
		{r: []model.Interval{{Start: 0, End: 200}}},
		{r: []model.Interval{{Start: 1000, End: 20000}}},
	}

	for _, c := range cases {
		i := int64(-1)
		it := NewDeletedSeriesIterator(NewConcreteSeriesIterator(&cs), c.r)
		ranges := c.r[:]

		for it.Next() {
			i++
			for _, tr := range ranges {
				if inbound(model.Time(i), tr) {
					i = int64(tr.End + 1)
					ranges = ranges[1:]
				}
			}

			require.Equal(t, true, i < 1000)

			ts, v := it.At()
			require.Equal(t, int64(cs.samples[i].Timestamp), ts)
			require.Equal(t, float64(cs.samples[i].Value), v)
		}

		// There has been an extra call to Next().
		i++
		for _, tr := range ranges {
			if inbound(model.Time(i), tr) {
				i = int64(tr.End + 1)
				ranges = ranges[1:]
			}
		}

		require.Equal(t, true, i >= 1000)
		require.NoError(t, it.Err())
	}
}

func TestDeletedIterator_WithSeek(t *testing.T) {
	cs := ConcreteSeries{labels: labels.FromStrings("foo", "bar")}
	// Insert random stuff from (0, 1000).
	for i := 0; i < 1000; i++ {
		cs.samples = append(cs.samples, model.SamplePair{Timestamp: model.Time(i), Value: model.SampleValue(rand.Float64())})
	}

	cases := []struct {
		r        []model.Interval
		seek     int64
		ok       bool
		seekedTs int64
	}{
		{r: []model.Interval{{Start: 1, End: 20}}, seek: 1, ok: true, seekedTs: 21},
		{r: []model.Interval{{Start: 1, End: 20}}, seek: 20, ok: true, seekedTs: 21},
		{r: []model.Interval{{Start: 1, End: 20}}, seek: 10, ok: true, seekedTs: 21},
		{r: []model.Interval{{Start: 1, End: 20}}, seek: 999, ok: true, seekedTs: 999},
		{r: []model.Interval{{Start: 1, End: 20}}, seek: 1000, ok: false},
		{r: []model.Interval{{Start: 1, End: 23}, {Start: 24, End: 40}, {Start: 45, End: 3000}}, seek: 1, ok: true, seekedTs: 41},
		{r: []model.Interval{{Start: 5, End: 23}, {Start: 24, End: 40}, {Start: 41, End: 3000}}, seek: 5, ok: false},
		{r: []model.Interval{{Start: 0, End: 2000}}, seek: 10, ok: false},
		{r: []model.Interval{{Start: 500, End: 2000}}, seek: 10, ok: true, seekedTs: 10},
		{r: []model.Interval{{Start: 500, End: 2000}}, seek: 501, ok: false},
	}

	for _, c := range cases {
		it := NewDeletedSeriesIterator(NewConcreteSeriesIterator(&cs), c.r)

		require.Equal(t, c.ok, it.Seek(c.seek))
		if c.ok {
			ts, _ := it.At()
			require.Equal(t, c.seekedTs, ts)
		}
	}
}

func inbound(t model.Time, interval model.Interval) bool {
	return interval.Start <= t && t <= interval.End
}
