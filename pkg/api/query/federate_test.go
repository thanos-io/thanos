// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"

	baseAPI "github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestFederate(t *testing.T) {
	db, err := e2eutil.NewTSDB()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, db.Close()) }()

	now := time.Unix(3600, 0)
	nowMs := now.UnixMilli()

	app := db.Appender(context.Background())
	for _, s := range []struct {
		lset labels.Labels
		t    int64
		v    float64
	}{
		// Two replicas of the same series; the newest sample must win after deduplication.
		{labels.FromStrings("__name__", "up", "job", "a", "replica", "1"), nowMs - 60_000, 1},
		{labels.FromStrings("__name__", "up", "job", "a", "replica", "1"), nowMs - 30_000, 2},
		{labels.FromStrings("__name__", "up", "job", "a", "replica", "2"), nowMs - 30_000, 2},
		{labels.FromStrings("__name__", "up", "job", "b", "instance", "host:9090", "replica", "1"), nowMs - 15_000, 3},
		// Samples after "now" are ignored.
		{labels.FromStrings("__name__", "up", "job", "b", "instance", "host:9090", "replica", "1"), nowMs + 15_000, 99},
		// Outside of the lookback window.
		{labels.FromStrings("__name__", "up", "job", "old", "replica", "1"), nowMs - (10 * time.Minute).Milliseconds(), 4},
		// Stale series.
		{labels.FromStrings("__name__", "up", "job", "stale", "replica", "1"), nowMs - 60_000, 5},
		{labels.FromStrings("__name__", "up", "job", "stale", "replica", "1"), nowMs - 30_000, math.Float64frombits(value.StaleNaN)},
		{labels.FromStrings("__name__", "a_metric", "job", "a", "replica", "1"), nowMs - 30_000, 6},
	} {
		_, err := app.Append(0, s.lset, s.t, s.v)
		testutil.Ok(t, err)
	}
	testutil.Ok(t, app.Commit())

	api := &QueryAPI{
		baseAPI: &baseAPI.BaseAPI{
			Now: func() time.Time { return now },
		},
		logger:              log.NewNopLogger(),
		queryableCreate:     query.NewQueryableCreator(nil, nil, newProxyStoreWithTSDBStore(db), 2, timeout, dedup.AlgorithmPenalty, 1),
		lookbackDeltaCreate: func(int64) time.Duration { return 0 },
		replicaLabels:       []string{"replica"},
		tenantHeader:        "thanos-tenant",
		defaultTenant:       "default-tenant",
	}

	for _, tc := range []struct {
		name     string
		params   url.Values
		code     int
		expected string
	}{
		{
			name:   "no matchers",
			params: url.Values{},
			code:   http.StatusBadRequest,
		},
		{
			name:   "invalid matcher",
			params: url.Values{"match[]": []string{"up{"}},
			code:   http.StatusBadRequest,
		},
		{
			name:   "deduplicated latest samples",
			params: url.Values{"match[]": []string{"up", "a_metric"}},
			code:   http.StatusOK,
			expected: `# TYPE a_metric untyped
a_metric{instance="",job="a"} 6 3570000
# TYPE up untyped
up{instance="host:9090",job="b"} 3 3585000
up{instance="",job="a"} 2 3570000
`,
		},
		{
			name:   "without deduplication",
			params: url.Values{"match[]": []string{`up{job="a"}`}, "dedup": []string{"false"}},
			code:   http.StatusOK,
			expected: `# TYPE up untyped
up{instance="",job="a",replica="1"} 2 3570000
up{instance="",job="a",replica="2"} 2 3570000
`,
		},
		{
			name:   "custom lookback delta",
			params: url.Values{"match[]": []string{`up{job="old"}`}, "lookback_delta": []string{"15m"}},
			code:   http.StatusOK,
			expected: `# TYPE up untyped
up{instance="",job="old"} 4 3000000
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/federate?"+tc.params.Encode(), nil)
			rec := httptest.NewRecorder()
			api.Federate(rec, req)

			testutil.Equals(t, tc.code, rec.Code, rec.Body.String())
			if tc.code == http.StatusOK {
				testutil.Equals(t, tc.expected, rec.Body.String())
			}
		})
	}
}
