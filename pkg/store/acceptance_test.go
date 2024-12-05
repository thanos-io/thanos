// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

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

type seriesCallCase struct {
	matchers   []storepb.LabelMatcher
	start      int64
	end        int64
	skipChunks bool

	expectedLabels []labels.Labels
	expectErr      error
}

type startStoreFn func(t *testing.T, extLset labels.Labels, append func(app storage.Appender)) storepb.StoreServer

// testStoreAPIsAcceptance tests StoreAPI from closed box perspective.
func testStoreAPIsAcceptance(t *testing.T, startStore startStoreFn) {
	t.Helper()

	now := time.Now()
	extLset := labels.FromStrings("region", "eu-west")
	for _, tc := range []struct {
		desc             string
		appendFn         func(app storage.Appender)
		labelNameCalls   []labelNameCallCase
		labelValuesCalls []labelValuesCallCase
		seriesCalls      []seriesCallCase
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
				{start: timestamp.FromTime(minTime), end: timestamp.FromTime(maxTime), label: "replica"},
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
					label:          "region",
					expectedValues: []string(nil),
					matchers:       []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "nonexistent"}},
				},
				{
					start:          timestamp.FromTime(minTime),
					end:            timestamp.FromTime(maxTime),
					label:          "region",
					expectedValues: []string(nil),
					matchers:       []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-east"}},
				},
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
		{
			desc: "conflicting internal and external labels when skipping chunks",
			appendFn: func(app storage.Appender) {
				_, err := app.Append(0, labels.FromStrings("foo", "bar", "region", "somewhere"), 0, 0)
				testutil.Ok(t, err)
				testutil.Ok(t, app.Commit())
			},
			seriesCalls: []seriesCallCase{
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
					},
					skipChunks: true,
					expectedLabels: []labels.Labels{
						labels.FromStrings("foo", "bar", "region", "eu-west"),
					},
				},
			},
		},
		{
			desc: "series matcher on other labels when requesting external labels",
			appendFn: func(app storage.Appender) {
				_, err := app.Append(0, labels.FromStrings("__name__", "up", "foo", "bar", "job", "C"), 0, 0)
				testutil.Ok(t, err)
				_, err = app.Append(0, labels.FromStrings("__name__", "up", "foo", "baz", "job", "C"), 0, 0)
				testutil.Ok(t, err)

				testutil.Ok(t, app.Commit())
			},
			labelValuesCalls: []labelValuesCallCase{
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					label: "region",
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
						{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "C"},
					},
					expectedValues: []string{"eu-west"},
				},
			},
		},
		{
			// Testcases taken from https://github.com/prometheus/prometheus/blob/95e705612c1d557f1681bd081a841b78f93ee158/tsdb/querier_test.go#L1898
			desc: "matching behavior",
			appendFn: func(app storage.Appender) {
				_, err := app.Append(0, labels.FromStrings("n", "1"), 0, 0)
				testutil.Ok(t, err)
				_, err = app.Append(0, labels.FromStrings("n", "1", "i", "a"), 0, 0)
				testutil.Ok(t, err)
				_, err = app.Append(0, labels.FromStrings("n", "1", "i", "b"), 0, 0)
				testutil.Ok(t, err)
				_, err = app.Append(0, labels.FromStrings("n", "2"), 0, 0)
				testutil.Ok(t, err)
				_, err = app.Append(0, labels.FromStrings("n", "2.5"), 0, 0)
				testutil.Ok(t, err)

				testutil.Ok(t, app.Commit())
			},
			seriesCalls: []seriesCallCase{
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
						labels.FromStrings("n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_EQ, Name: "i", Value: "a"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_EQ, Name: "i", Value: "missing"},
					},
					expectedLabels: []labels.Labels{},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "missing", Value: ""},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
						labels.FromStrings("n", "1", "region", "eu-west"),
						labels.FromStrings("n", "2", "region", "eu-west"),
						labels.FromStrings("n", "2.5", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_NEQ, Name: "n", Value: "1"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "2", "region", "eu-west"),
						labels.FromStrings("n", "2.5", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: ".+"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: ".*"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
						labels.FromStrings("n", "1", "region", "eu-west"),
						labels.FromStrings("n", "2", "region", "eu-west"),
						labels.FromStrings("n", "2.5", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "i", Value: ""},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "1", "region", "eu-west"),
						labels.FromStrings("n", "2", "region", "eu-west"),
						labels.FromStrings("n", "2.5", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_NEQ, Name: "i", Value: ""},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_NEQ, Name: "missing", Value: ""},
					},
					expectedLabels: []labels.Labels{},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_NEQ, Name: "i", Value: "a"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
						labels.FromStrings("n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "n", Value: "^1$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
						labels.FromStrings("n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "^a$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "^a?$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "^$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "1", "region", "eu-west"),
						labels.FromStrings("n", "2", "region", "eu-west"),
						labels.FromStrings("n", "2.5", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "^$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "^.*$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
						labels.FromStrings("n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "^.+$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_NRE, Name: "n", Value: "^1$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "2", "region", "eu-west"),
						labels.FromStrings("n", "2.5", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_NRE, Name: "n", Value: "1"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "2", "region", "eu-west"),
						labels.FromStrings("n", "2.5", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_NRE, Name: "n", Value: "1|2.5"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "2", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_NRE, Name: "n", Value: "(1|2.5)"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "2", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_NRE, Name: "i", Value: "^a$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
						labels.FromStrings("n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_NRE, Name: "i", Value: "^a?$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_NRE, Name: "i", Value: "^$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_NRE, Name: "i", Value: "^.*$"},
					},
					expectedLabels: []labels.Labels{},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_NRE, Name: "i", Value: "^.+$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_NEQ, Name: "i", Value: ""},
						{Type: storepb.LabelMatcher_EQ, Name: "i", Value: "a"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "n", Value: "1"},
						{Type: storepb.LabelMatcher_NEQ, Name: "i", Value: "b"},
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "^(b|a).*$"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "n", Value: "(1|2)"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
						labels.FromStrings("n", "1", "region", "eu-west"),
						labels.FromStrings("n", "2", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "a|b"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "(a|b)"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("i", "a", "n", "1", "region", "eu-west"),
						labels.FromStrings("i", "b", "n", "1", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "n", Value: "x1|2"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "2", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "n", Value: "2|2\\.5"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "2", "region", "eu-west"),
						labels.FromStrings("n", "2.5", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "c||d"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "1", "region", "eu-west"),
						labels.FromStrings("n", "2", "region", "eu-west"),
						labels.FromStrings("n", "2.5", "region", "eu-west"),
					},
				},
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: "(c||d)"},
					},
					expectedLabels: []labels.Labels{
						labels.FromStrings("n", "1", "region", "eu-west"),
						labels.FromStrings("n", "2", "region", "eu-west"),
						labels.FromStrings("n", "2.5", "region", "eu-west"),
					},
				},
			},
		},
		{
			desc: "label values and names with non-equal matchers on external labels",
			appendFn: func(app storage.Appender) {
				_, err := app.Append(0, labels.FromStrings("__name__", "up", "foo", "bar"), 0, 0)
				testutil.Ok(t, err)
				testutil.Ok(t, app.Commit())
			},
			labelNameCalls: []labelNameCallCase{
				{
					start:         timestamp.FromTime(minTime),
					end:           timestamp.FromTime(maxTime),
					expectedNames: []string{"__name__", "foo", "region"},
					matchers:      []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "region", Value: ".*"}},
				},
			},
			labelValuesCalls: []labelValuesCallCase{
				{
					start: timestamp.FromTime(minTime),
					end:   timestamp.FromTime(maxTime),
					label: "region",
					matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
						{Type: storepb.LabelMatcher_RE, Name: "region", Value: ".*"},
					},
					expectedValues: []string{"eu-west"},
				},
			},
		},
		{
			desc: "label_values(kube_pod_info{}, pod) don't fetch postings for pod!=''",
			appendFn: func(app storage.Appender) {
				_, err := app.Append(0, labels.FromStrings("__name__", "up", "pod", "pod-1"), timestamp.FromTime(now), 1)
				testutil.Ok(t, err)
				_, err = app.Append(0, labels.FromStrings("__name__", "up", "pod", "pod-2"), timestamp.FromTime(now), 1)
				testutil.Ok(t, err)
				_, err = app.Append(0, labels.FromStrings("__name__", "kube_pod_info", "pod", "pod-1"), timestamp.FromTime(now), 1)
				testutil.Ok(t, err)
				testutil.Ok(t, app.Commit())
			},
			labelNameCalls: []labelNameCallCase{
				{
					start:         timestamp.FromTime(minTime),
					end:           timestamp.FromTime(maxTime),
					expectedNames: []string{"__name__", "pod", "region"},
					matchers:      []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "kube_pod_info"}},
				},
				{
					start:         timestamp.FromTime(minTime),
					end:           timestamp.FromTime(maxTime),
					expectedNames: []string{"__name__", "pod", "region"},
				},
			},
			labelValuesCalls: []labelValuesCallCase{
				{
					start:          timestamp.FromTime(minTime),
					end:            timestamp.FromTime(maxTime),
					label:          "pod",
					expectedValues: []string{"pod-1"},
					matchers:       []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "kube_pod_info"}},
				},
				{
					start:          timestamp.FromTime(minTime),
					end:            timestamp.FromTime(maxTime),
					label:          "pod",
					expectedValues: []string{"pod-1", "pod-2"},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			appendFn := tc.appendFn
			if appendFn == nil {
				appendFn = func(storage.Appender) {}
			}
			store := startStore(t, extLset, appendFn)

			for _, c := range tc.labelNameCalls {
				t.Run("label_names", func(t *testing.T) {
					resp, err := store.LabelNames(context.Background(), &storepb.LabelNamesRequest{
						Start:                c.start,
						End:                  c.end,
						Matchers:             c.matchers,
						WithoutReplicaLabels: []string{"replica"},
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
				t.Run("label_values", func(t *testing.T) {
					resp, err := store.LabelValues(context.Background(), &storepb.LabelValuesRequest{
						Start:                c.start,
						End:                  c.end,
						Label:                c.label,
						Matchers:             c.matchers,
						WithoutReplicaLabels: []string{"replica"},
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
			for _, c := range tc.seriesCalls {
				t.Run("series", func(t *testing.T) {
					srv := newStoreSeriesServer(context.Background())
					err := store.Series(&storepb.SeriesRequest{
						MinTime:              c.start,
						MaxTime:              c.end,
						Matchers:             c.matchers,
						SkipChunks:           c.skipChunks,
						WithoutReplicaLabels: []string{"replica"},
					}, srv)
					if c.expectErr != nil {
						testutil.NotOk(t, err)
						testutil.Equals(t, c.expectErr.Error(), err.Error())
						return
					}
					testutil.Ok(t, err)

					testutil.Assert(t, slices.IsSortedFunc(srv.SeriesSet, func(x, y storepb.Series) int {
						return labels.Compare(x.PromLabels(), y.PromLabels())
					}), "Unsorted Series response returned")

					receivedLabels := make([]labels.Labels, 0)
					for _, s := range srv.SeriesSet {
						receivedLabels = append(receivedLabels, s.PromLabels())
					}

					testutil.Equals(t, c.expectedLabels, receivedLabels)
				})
			}
		})
	}
}

// Regression test for https://github.com/thanos-io/thanos/issues/396.
// Note: Only TSDB and Prometheus Stores do this.
func testStoreAPIsSeriesSplitSamplesIntoChunksWithMaxSizeOf120(t *testing.T, startStore startStoreFn) {
	t.Run("should split into chunks of max size 120", func(t *testing.T) {
		baseT := timestamp.FromTime(time.Now().AddDate(0, 0, -2)) / 1000 * 1000
		offset := int64(2*math.MaxUint16 + 5)

		extLset := labels.FromStrings("region", "eu-west")
		appendFn := func(app storage.Appender) {
			var (
				ref storage.SeriesRef
				err error
			)
			for i := int64(0); i < offset; i++ {
				ref, err = app.Append(ref, labels.FromStrings("a", "b"), baseT+i, 1)
				testutil.Ok(t, err)
			}
			testutil.Ok(t, app.Commit())

		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := startStore(t, extLset, appendFn)
		srv := newStoreSeriesServer(ctx)

		testutil.Ok(t, client.Series(&storepb.SeriesRequest{
			MinTime: baseT,
			MaxTime: baseT + offset,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
				{Type: storepb.LabelMatcher_EQ, Name: "region", Value: "eu-west"},
			},
		}, srv))

		testutil.Equals(t, 1, len(srv.SeriesSet))

		firstSeries := srv.SeriesSet[0]

		testutil.Equals(t, []labelpb.ZLabel{
			{Name: "a", Value: "b"},
			{Name: "region", Value: "eu-west"},
		}, firstSeries.Labels)

		testutil.Equals(t, 1093, len(firstSeries.Chunks))
		for i := 0; i < len(firstSeries.Chunks)-1; i++ {
			chunk, err := chunkenc.FromData(chunkenc.EncXOR, firstSeries.Chunks[i].Raw.Data)
			testutil.Ok(t, err)
			testutil.Equals(t, 120, chunk.NumSamples())
		}

		chunk, err := chunkenc.FromData(chunkenc.EncXOR, firstSeries.Chunks[len(firstSeries.Chunks)-1].Raw.Data)
		testutil.Ok(t, err)
		testutil.Equals(t, 35, chunk.NumSamples())
	})
}

func TestBucketStore_Acceptance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	startStore := func(lazyExpandedPostings bool) func(tt *testing.T, extLset labels.Labels, appendFn func(app storage.Appender)) storepb.StoreServer {
		return func(tt *testing.T, extLset labels.Labels, appendFn func(app storage.Appender)) storepb.StoreServer {
			tmpDir := tt.TempDir()
			bktDir := filepath.Join(tmpDir, "bkt")
			auxDir := filepath.Join(tmpDir, "aux")
			metaDir := filepath.Join(tmpDir, "meta")

			testutil.Ok(tt, os.MkdirAll(metaDir, os.ModePerm))
			testutil.Ok(tt, os.MkdirAll(auxDir, os.ModePerm))

			bkt, err := filesystem.NewBucket(bktDir)
			testutil.Ok(tt, err)
			tt.Cleanup(func() { testutil.Ok(tt, bkt.Close()) })

			headOpts := tsdb.DefaultHeadOptions()
			headOpts.ChunkDirRoot = tmpDir
			headOpts.ChunkRange = 1000
			h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
			testutil.Ok(tt, err)
			tt.Cleanup(func() { testutil.Ok(tt, h.Close()) })
			logger := log.NewNopLogger()

			appendFn(h.Appender(context.Background()))

			if h.NumSeries() == 0 {
				tt.Skip("Bucket Store cannot handle empty HEAD")
			}

			for _, replica := range []string{"r1", "r2"} {
				id := storetestutil.CreateBlockFromHead(tt, auxDir, h)

				auxBlockDir := filepath.Join(auxDir, id.String())
				meta, err := metadata.ReadFromDir(auxBlockDir)
				testutil.Ok(t, err)
				stats, err := block.GatherIndexHealthStats(ctx, logger, filepath.Join(auxBlockDir, block.IndexFilename), meta.MinTime, meta.MaxTime)
				testutil.Ok(t, err)
				_, err = metadata.InjectThanos(log.NewNopLogger(), auxBlockDir, metadata.Thanos{
					Labels:     labels.NewBuilder(extLset).Set("replica", replica).Labels().Map(),
					Downsample: metadata.ThanosDownsample{Resolution: 0},
					Source:     metadata.TestSource,
					IndexStats: metadata.IndexStats{
						SeriesMaxSize:   stats.SeriesMaxSize,
						SeriesP90Size:   stats.SeriesP90Size,
						SeriesP99Size:   stats.SeriesP99Size,
						SeriesP999Size:  stats.SeriesP999Size,
						SeriesP9999Size: stats.SeriesP9999Size,
						ChunkMaxSize:    stats.ChunkMaxSize,
					},
				}, nil)
				testutil.Ok(tt, err)

				testutil.Ok(tt, block.Upload(ctx, logger, bkt, auxBlockDir, metadata.NoneFunc))
			}

			chunkPool, err := NewDefaultChunkBytesPool(2e5)
			testutil.Ok(tt, err)

			insBkt := objstore.WithNoopInstr(bkt)
			baseBlockIDsFetcher := block.NewConcurrentLister(logger, insBkt)
			metaFetcher, err := block.NewMetaFetcher(logger, 20, insBkt, baseBlockIDsFetcher, metaDir, nil, []block.MetadataFilter{
				block.NewTimePartitionMetaFilter(allowAllFilterConf.MinTime, allowAllFilterConf.MaxTime),
			})
			testutil.Ok(tt, err)

			bucketStore, err := NewBucketStore(
				objstore.WithNoopInstr(bkt),
				metaFetcher,
				"",
				NewChunksLimiterFactory(10e6),
				NewSeriesLimiterFactory(10e6),
				NewBytesLimiterFactory(10e6),
				NewGapBasedPartitioner(PartitionerMaxGapSize),
				20,
				true,
				DefaultPostingOffsetInMemorySampling,
				false,
				false,
				1*time.Minute,
				WithChunkPool(chunkPool),
				WithFilterConfig(allowAllFilterConf),
				WithLazyExpandedPostings(lazyExpandedPostings),
			)
			testutil.Ok(tt, err)
			tt.Cleanup(func() { testutil.Ok(tt, bucketStore.Close()) })

			testutil.Ok(tt, bucketStore.SyncBlocks(context.Background()))

			return bucketStore
		}
	}

	for _, lazyExpandedPostings := range []bool{false, true} {
		t.Run(fmt.Sprintf("lazyExpandedPostings:%t", lazyExpandedPostings), func(t *testing.T) {
			testStoreAPIsAcceptance(t, startStore(lazyExpandedPostings))
		})
	}
}

func TestPrometheusStore_Acceptance(t *testing.T) {
	t.Parallel()

	startStore := func(tt *testing.T, extLset labels.Labels, appendFn func(app storage.Appender)) storepb.StoreServer {
		p, err := e2eutil.NewPrometheus()
		testutil.Ok(tt, err)
		tt.Cleanup(func() { testutil.Ok(tt, p.Stop()) })

		appendFn(p.Appender())

		testutil.Ok(tt, p.Start(context.Background(), log.NewNopLogger()))
		u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
		testutil.Ok(tt, err)

		version, err := promclient.NewDefaultClient().BuildVersion(context.Background(), u)
		testutil.Ok(tt, err)

		promStore, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
			func() labels.Labels { return extLset },
			func() (int64, int64) { return timestamp.FromTime(minTime), timestamp.FromTime(maxTime) },
			func() string { return version })
		testutil.Ok(tt, err)

		// We build chunks only for SAMPLES method. Make sure we ask for SAMPLES only.
		promStore.remoteReadAcceptableResponses = []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES}

		return promStore
	}

	testStoreAPIsAcceptance(t, startStore)
	testStoreAPIsSeriesSplitSamplesIntoChunksWithMaxSizeOf120(t, startStore)
}

func TestTSDBStore_Acceptance(t *testing.T) {
	t.Parallel()

	startStore := func(tt *testing.T, extLset labels.Labels, appendFn func(app storage.Appender)) storepb.StoreServer {
		db, err := e2eutil.NewTSDB()
		testutil.Ok(tt, err)
		tt.Cleanup(func() { testutil.Ok(tt, db.Close()) })
		appendFn(db.Appender(context.Background()))

		return NewTSDBStore(nil, db, component.Rule, extLset)
	}

	testStoreAPIsAcceptance(t, startStore)
	testStoreAPIsSeriesSplitSamplesIntoChunksWithMaxSizeOf120(t, startStore)
}

func TestProxyStoreWithTSDBSelector_Acceptance(t *testing.T) {
	t.Skip("This is a known issue, we need to think how to fix it")

	ctx := context.Background()

	startStore := func(tt *testing.T, extLset labels.Labels, appendFn func(app storage.Appender)) storepb.StoreServer {
		startNestedStore := func(tt *testing.T, appendFn func(app storage.Appender), extLsets ...labels.Labels) storepb.StoreServer {
			tmpDir := tt.TempDir()
			bktDir := filepath.Join(tmpDir, "bkt")
			auxDir := filepath.Join(tmpDir, "aux")
			metaDir := filepath.Join(tmpDir, "meta")

			testutil.Ok(tt, os.MkdirAll(metaDir, os.ModePerm))
			testutil.Ok(tt, os.MkdirAll(auxDir, os.ModePerm))

			bkt, err := filesystem.NewBucket(bktDir)
			testutil.Ok(tt, err)
			tt.Cleanup(func() { testutil.Ok(tt, bkt.Close()) })

			headOpts := tsdb.DefaultHeadOptions()
			headOpts.ChunkDirRoot = tmpDir
			headOpts.ChunkRange = 1000
			h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
			testutil.Ok(tt, err)
			tt.Cleanup(func() { testutil.Ok(tt, h.Close()) })
			logger := log.NewNopLogger()

			appendFn(h.Appender(context.Background()))

			if h.NumSeries() == 0 {
				tt.Skip("Bucket Store cannot handle empty HEAD")
			}

			for _, extLset := range extLsets {
				id := storetestutil.CreateBlockFromHead(tt, auxDir, h)

				auxBlockDir := filepath.Join(auxDir, id.String())
				meta, err := metadata.ReadFromDir(auxBlockDir)
				testutil.Ok(t, err)
				stats, err := block.GatherIndexHealthStats(ctx, logger, filepath.Join(auxBlockDir, block.IndexFilename), meta.MinTime, meta.MaxTime)
				testutil.Ok(t, err)
				_, err = metadata.InjectThanos(log.NewNopLogger(), auxBlockDir, metadata.Thanos{
					Labels:     extLset.Map(),
					Downsample: metadata.ThanosDownsample{Resolution: 0},
					Source:     metadata.TestSource,
					IndexStats: metadata.IndexStats{
						SeriesMaxSize:   stats.SeriesMaxSize,
						SeriesP90Size:   stats.SeriesP90Size,
						SeriesP99Size:   stats.SeriesP99Size,
						SeriesP999Size:  stats.SeriesP999Size,
						SeriesP9999Size: stats.SeriesP9999Size,
						ChunkMaxSize:    stats.ChunkMaxSize,
					},
				}, nil)
				testutil.Ok(tt, err)

				testutil.Ok(tt, block.Upload(ctx, logger, bkt, auxBlockDir, metadata.NoneFunc))
			}

			chunkPool, err := NewDefaultChunkBytesPool(2e5)
			testutil.Ok(tt, err)

			insBkt := objstore.WithNoopInstr(bkt)
			baseBlockIDsFetcher := block.NewConcurrentLister(logger, insBkt)
			metaFetcher, err := block.NewMetaFetcher(logger, 20, insBkt, baseBlockIDsFetcher, metaDir, nil, []block.MetadataFilter{
				block.NewTimePartitionMetaFilter(allowAllFilterConf.MinTime, allowAllFilterConf.MaxTime),
			})
			testutil.Ok(tt, err)

			bucketStore, err := NewBucketStore(
				objstore.WithNoopInstr(bkt),
				metaFetcher,
				"",
				NewChunksLimiterFactory(10e6),
				NewSeriesLimiterFactory(10e6),
				NewBytesLimiterFactory(10e6),
				NewGapBasedPartitioner(PartitionerMaxGapSize),
				20,
				true,
				DefaultPostingOffsetInMemorySampling,
				false,
				false,
				1*time.Minute,
				WithChunkPool(chunkPool),
				WithFilterConfig(allowAllFilterConf),
			)
			testutil.Ok(tt, err)
			tt.Cleanup(func() { testutil.Ok(tt, bucketStore.Close()) })

			testutil.Ok(tt, bucketStore.SyncBlocks(context.Background()))

			return bucketStore
		}

		extLset1 := labels.NewBuilder(extLset).Set("L1", "A").Set("L2", "B").Labels()
		extLset2 := labels.NewBuilder(extLset).Set("L1", "C").Set("L2", "D").Labels()
		extLset3 := labels.NewBuilder(extLset).Set("L1", "A").Set("L2", "D").Labels()

		p1 := startNestedStore(tt, appendFn, extLset1, extLset2, extLset3)

		clients := []Client{
			storetestutil.TestClient{StoreClient: storepb.ServerAsClient(p1), ExtLset: []labels.Labels{extLset1, extLset2, extLset3}},
		}

		relabelCfgs := []*relabel.Config{{
			SourceLabels: model.LabelNames([]model.LabelName{"L1", "L2"}),
			Separator:    "-",
			Regex:        relabel.MustNewRegexp("(A-B|C-D)"),
			Action:       relabel.Keep,
		}}

		return NewProxyStore(nil, nil, func() []Client { return clients }, component.Query, labels.EmptyLabels(), 0*time.Second, RetrievalStrategy(EagerRetrieval), WithTSDBSelector(NewTSDBSelector(relabelCfgs)))
	}

	client := startStore(t, labels.EmptyLabels(), func(app storage.Appender) {
		_, err := app.Append(0, labels.FromStrings("a", "b"), 0, 0)
		testutil.Ok(t, err)
		testutil.Ok(t, app.Commit())
	})
	srv := newStoreSeriesServer(ctx)

	testutil.Ok(t, client.Series(&storepb.SeriesRequest{
		MinTime: minTime.Unix(),
		MaxTime: maxTime.Unix(),
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "b"},
		},
	}, srv))

	receivedLabels := make([]labels.Labels, 0)
	for _, s := range srv.SeriesSet {
		receivedLabels = append(receivedLabels, s.PromLabels())
	}

	// This fails currently because the method of using matchers cannot drop extLset3 even though we should only
	// select extLset1 and extLset2 because of the TSDB Selector
	testutil.Equals(t, receivedLabels, []labels.Labels{
		labels.FromStrings("L1", "A", "L2", "B", "a", "b"),
		labels.FromStrings("L1", "C", "L2", "D", "a", "b"),
	})

}

func TestProxyStoreWithReplicas_Acceptance(t *testing.T) {
	t.Parallel()

	startStore := func(tt *testing.T, extLset labels.Labels, appendFn func(app storage.Appender)) storepb.StoreServer {
		startNestedStore := func(tt *testing.T, extLset labels.Labels, appendFn func(app storage.Appender)) storepb.StoreServer {
			db, err := e2eutil.NewTSDB()
			testutil.Ok(tt, err)
			tt.Cleanup(func() { testutil.Ok(tt, db.Close()) })
			appendFn(db.Appender(context.Background()))

			return NewTSDBStore(nil, db, component.Rule, extLset)

		}

		extLset1 := labels.NewBuilder(extLset).Set("replica", "r1").Labels()
		extLset2 := labels.NewBuilder(extLset).Set("replica", "r2").Labels()

		p1 := startNestedStore(tt, extLset1, appendFn)
		p2 := startNestedStore(tt, extLset2, appendFn)

		clients := []Client{
			storetestutil.TestClient{StoreClient: storepb.ServerAsClient(p1), ExtLset: []labels.Labels{extLset1}},
			storetestutil.TestClient{StoreClient: storepb.ServerAsClient(p2), ExtLset: []labels.Labels{extLset2}},
		}

		return NewProxyStore(nil, nil, func() []Client { return clients }, component.Query, labels.EmptyLabels(), 0*time.Second, RetrievalStrategy(EagerRetrieval))
	}

	testStoreAPIsAcceptance(t, startStore)
}
