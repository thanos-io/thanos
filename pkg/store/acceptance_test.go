// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type labelNameCallCase struct {
	matchers []storepb.LabelMatcher
	start    int64
	end      int64

	expectedNames []string
	expectErr     error
}

type labelValuesCallCase struct {
	label string

	matchers []storepb.LabelMatcher
	start    int64
	end      int64

	expectedValues []string
	expectErr      error
}

// testLabelAPIs tests labels methods from StoreAPI from closed box perspective.
func testLabelAPIs(t *testing.T, startStore func(extLset labels.Labels, append func(app storage.Appender)) storepb.StoreServer) {
	t.Helper()

	now := time.Now()
	extLset := labels.FromStrings("region", "eu-west")
	for _, tc := range []struct {
		desc             string
		appendFn         func(app storage.Appender)
		labelNameCalls   []labelNameCallCase
		labelValuesCalls []labelValuesCallCase
	}{
		{
			desc: "no label in tsdb, empty results",
			labelNameCalls: []labelNameCallCase{
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime)},
			},
			labelValuesCalls: []labelValuesCallCase{
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), expectErr: errors.New("rpc error: code = InvalidArgument desc = label name parameter cannot be empty")},
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), label: "foo"},
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), label: "region", expectedValues: []string{"eu-west"}}, // External labels should be visible.
			},
		},
		{
			desc: "{foo=foovalue1} 1",
			appendFn: func(app storage.Appender) {
				_, err := app.Append(0, labels.FromStrings("foo", "foovalue1"), timestamp.FromTime(now), 1)
				testutil.Ok(t, err)
				testutil.Ok(t, app.Commit())
			},
			labelNameCalls: []labelNameCallCase{
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), expectedNames: []string{"foo", "region"}},
			},
			labelValuesCalls: []labelValuesCallCase{
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), label: "foo", expectedValues: []string{"foovalue1"}},
			},
		},
		{
			desc: "{foo=foovalue2} 1 and {foo=foovalue2} 1",
			appendFn: func(app storage.Appender) {
				_, err := app.Append(0, labels.FromStrings("foo", "foovalue1"), timestamp.FromTime(now), 1)
				testutil.Ok(t, err)
				_, err = app.Append(0, labels.FromStrings("foo", "foovalue2"), timestamp.FromTime(now), 1)
				testutil.Ok(t, err)
				testutil.Ok(t, app.Commit())
			},
			labelNameCalls: []labelNameCallCase{
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), expectedNames: []string{"foo", "region"}},
			},
			labelValuesCalls: []labelValuesCallCase{
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), label: "foo", expectedValues: []string{"foovalue1", "foovalue2"}},
			},
		},
		{
			desc: "{foo=foovalue1, bar=barvalue1} 1 and {foo=foovalue2} 1 and {foo=foovalue2} 1",
			appendFn: func(app storage.Appender) {
				_, err := app.Append(0, labels.FromStrings("foo", "foovalue1"), timestamp.FromTime(now), 1)
				testutil.Ok(t, err)
				_, err = app.Append(0, labels.FromStrings("foo", "foovalue2"), timestamp.FromTime(now), 1)
				testutil.Ok(t, err)
				_, err = app.Append(0, labels.FromStrings("foo", "foovalue1", "bar", "barvalue1"), timestamp.FromTime(now), 1)
				testutil.Ok(t, err)
				testutil.Ok(t, app.Commit())
			},
			labelNameCalls: []labelNameCallCase{
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), expectedNames: []string{"bar", "foo", "region"}},
				// Query range outside added samples timestamp.
				// NOTE: Ideally we could do 'end: timestamp.FromTime(now.Add(-1 * time.Second))'. In practice however we index labels within block range, so we approximate label and label values to chunk of block time.
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(now.Add(-4 * time.Hour))},
				// Matchers on normal series.
				{
					start:         timestamp.FromTime(minTime),
					end:           timestamp.FromTime(maxTime),
					expectedNames: []string{"bar", "foo", "region"},
					matchers:      []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "bar", Value: "barvalue1"}},
				},
				{
					start:         timestamp.FromTime(minTime),
					end:           timestamp.FromTime(maxTime),
					expectedNames: []string{"foo", "region"},
					matchers:      []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "foovalue2"}},
				},
				{
					start:    timestamp.FromTime(minTime),
					end:      timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "bar", Value: "different"}},
				},
				// Matchers on external labels.
				{
					start:         timestamp.FromTime(minTime),
					end:           timestamp.FromTime(maxTime),
					expectedNames: []string{"bar", "foo", "region"},
					matchers:      []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"}},
				},
				{
					start:    timestamp.FromTime(minTime),
					end:      timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "different"}},
				},
			},
			labelValuesCalls: []labelValuesCallCase{
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), label: "foo", expectedValues: []string{"foovalue1", "foovalue2"}},
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), label: "bar", expectedValues: []string{"barvalue1"}},
				// Query range outside added samples timestamp.
				// NOTE: Ideally we could do 'end: timestamp.FromTime(now.Add(-1 * time.Second))'. In practice however we index labels within block range, so we approximate label and label values to chunk of block time.
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(now.Add(-4 * time.Hour)), label: "foo"},
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(now.Add(-4 * time.Hour)), label: "bar"},
				// Matchers on normal series.
				{
					start:          timestamp.FromTime(minTime),
					end:            timestamp.FromTime(maxTime),
					label:          "foo",
					expectedValues: []string{"foovalue1"},
					matchers:       []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "bar", Value: "barvalue1"}},
				},
				{
					start:    timestamp.FromTime(minTime),
					end:      timestamp.FromTime(maxTime),
					label:    "foo",
					matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "bar", Value: "different"}},
				},
				// Matchers on external labels.
				{
					start:          timestamp.FromTime(minTime),
					end:            timestamp.FromTime(maxTime),
					label:          "foo",
					expectedValues: []string{"foovalue1", "foovalue2"},
					matchers:       []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"}},
				},
				{
					start:          timestamp.FromTime(minTime),
					end:            timestamp.FromTime(maxTime),
					label:          "bar",
					expectedValues: []string{"barvalue1"},
					matchers:       []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"}},
				},
				{
					start:    timestamp.FromTime(minTime),
					end:      timestamp.FromTime(maxTime),
					label:    "foo",
					matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "different"}},
				},
				{
					start:    timestamp.FromTime(minTime),
					end:      timestamp.FromTime(maxTime),
					label:    "bar",
					matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "different"}},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			appendFn := tc.appendFn
			if appendFn == nil {
				appendFn = func(storage.Appender) {}
			}
			store := startStore(extLset, appendFn)
			for _, c := range tc.labelNameCalls {
				t.Run("label_names", func(t *testing.T) {
					resp, err := store.LabelNames(context.Background(), &storepb.LabelNamesRequest{
						Start:    c.start,
						End:      c.end,
						Matchers: c.matchers,
					})
					if c.expectErr != nil {
						testutil.NotOk(t, err)
						testutil.Equals(t, c.expectErr.Error(), err.Error())
						return
					}
					testutil.Ok(t, err)
					testutil.Equals(t, 0, len(resp.Warnings))
					if len(resp.Names) == 0 {
						resp.Names = nil
					}
					testutil.Equals(t, c.expectedNames, resp.Names)
				})
			}
			for _, c := range tc.labelValuesCalls {
				t.Run("label_name_values", func(t *testing.T) {
					resp, err := store.LabelValues(context.Background(), &storepb.LabelValuesRequest{
						Start:    c.start,
						End:      c.end,
						Label:    c.label,
						Matchers: c.matchers,
					})
					if c.expectErr != nil {
						testutil.NotOk(t, err)
						testutil.Equals(t, c.expectErr.Error(), err.Error())
						return
					}
					testutil.Ok(t, err)
					testutil.Equals(t, 0, len(resp.Warnings))
					if len(resp.Values) == 0 {
						resp.Values = nil
					}
					testutil.Equals(t, c.expectedValues, resp.Values)
				})
			}
		})
	}
}
