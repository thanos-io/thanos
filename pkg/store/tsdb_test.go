// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestTSDBStore_Info(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	resp, err := tsdbStore.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, []storepb.Label{{Name: "region", Value: "eu-west"}}, resp.Labels)
	testutil.Equals(t, storepb.StoreType_RULE, resp.StoreType)
	testutil.Equals(t, int64(0), resp.MinTime)
	testutil.Equals(t, int64(math.MaxInt64), resp.MaxTime)
}

func TestTSDBStore_Series(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	appender := db.Appender()

	for i := 1; i <= 3; i++ {
		_, err = appender.Add(labels.FromStrings("a", "1"), int64(i), float64(i))
		testutil.Ok(t, err)
	}
	err = appender.Commit()
	testutil.Ok(t, err)

	for _, tc := range []struct {
		title          string
		req            *storepb.SeriesRequest
		expectedSeries []rawSeries
		expectedError  string
	}{
		{
			title: "total match series",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "1"}, {Name: "region", Value: "eu-west"}},
					chunks: [][]sample{{{1, 1}, {2, 2}, {3, 3}}},
				},
			},
		},
		{
			title: "partially match time range series",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 2,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{
				{
					lset:   []storepb.Label{{Name: "a", Value: "1"}, {Name: "region", Value: "eu-west"}},
					chunks: [][]sample{{{1, 1}, {2, 2}}},
				},
			},
		},
		{
			title: "dont't match time range series",
			req: &storepb.SeriesRequest{
				MinTime: 4,
				MaxTime: 6,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{},
		},
		{
			title: "only match external label",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
				},
			},
			expectedError: "rpc error: code = InvalidArgument desc = no matchers specified (excluding external labels)",
		},
		{
			title: "dont't match labels",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"},
				},
			},
			expectedSeries: []rawSeries{},
		},
		{
			title: "no chunk",
			req: &storepb.SeriesRequest{
				MinTime: 1,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
				SkipChunks: true,
			},
			expectedSeries: []rawSeries{
				{
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "region", Value: "eu-west"}},
				},
			},
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			srv := newStoreSeriesServer(ctx)
			err := tsdbStore.Series(tc.req, srv)
			if len(tc.expectedError) > 0 {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.expectedError, err.Error())
			} else {
				testutil.Ok(t, err)
				seriesEquals(t, tc.expectedSeries, srv.SeriesSet)
			}
		}); !ok {
			return
		}
	}
}

func TestTSDBStore_LabelNames(t *testing.T) {
	var err error
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	appender := db.Appender()
	addLabels := func(lbs []string) {
		if len(lbs) > 0 {
			_, err = appender.Add(labels.FromStrings(lbs...), math.MaxInt64, 1)
			testutil.Ok(t, err)
		}
	}

	tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	for _, tc := range []struct {
		title         string
		labels        []string
		expectedNames []string
	}{
		{
			title:         "no label in tsdb",
			labels:        []string{},
			expectedNames: []string{},
		},
		{
			title:         "add one label",
			labels:        []string{"foo", "foo"},
			expectedNames: []string{"foo"},
		},
		{
			title:  "add another label",
			labels: []string{"bar", "bar"},
			// We will get two labels here.
			expectedNames: []string{"bar", "foo"},
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			addLabels(tc.labels)
			resp, err := tsdbStore.LabelNames(ctx, &storepb.LabelNamesRequest{})
			testutil.Ok(t, err)
			testutil.Equals(t, tc.expectedNames, resp.Names)
			testutil.Equals(t, 0, len(resp.Warnings))
		}); !ok {
			return
		}
	}
}

func TestTSDBStore_LabelValues(t *testing.T) {
	var err error
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	appender := db.Appender()
	addLabels := func(lbs []string) {
		if len(lbs) > 0 {
			_, err = appender.Add(labels.FromStrings(lbs...), math.MaxInt64, 1)
			testutil.Ok(t, err)
		}
	}

	tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

	for _, tc := range []struct {
		title          string
		addedLabels    []string
		queryLabel     string
		expectedValues []string
	}{
		{
			title:          "no label in tsdb",
			addedLabels:    []string{},
			queryLabel:     "foo",
			expectedValues: []string{},
		},
		{
			title:          "add one label value",
			addedLabels:    []string{"foo", "test"},
			queryLabel:     "foo",
			expectedValues: []string{"test"},
		},
		{
			title:          "add another label value",
			addedLabels:    []string{"foo", "test1"},
			queryLabel:     "foo",
			expectedValues: []string{"test", "test1"},
		},
	} {
		if ok := t.Run(tc.title, func(t *testing.T) {
			addLabels(tc.addedLabels)
			resp, err := tsdbStore.LabelValues(ctx, &storepb.LabelValuesRequest{Label: tc.queryLabel})
			testutil.Ok(t, err)
			testutil.Equals(t, tc.expectedValues, resp.Values)
			testutil.Equals(t, 0, len(resp.Warnings))
		}); !ok {
			return
		}
	}
}

// Regression test for https://github.com/thanos-io/thanos/issues/1038.
func TestTSDBStore_Series_SplitSamplesIntoChunksWithMaxSizeOfUint16_e2e(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	testSeries_SplitSamplesIntoChunksWithMaxSizeOfUint16_e2e(t, db.Appender(), func() storepb.StoreServer {
		tsdbStore := NewTSDBStore(nil, nil, db, component.Rule, labels.FromStrings("region", "eu-west"))

		return tsdbStore
	})
}
