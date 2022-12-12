// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	promgate "github.com/prometheus/prometheus/util/gate"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-community/promql-engine/engine"

	baseAPI "github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/pkg/testutil/testpromcompatibility"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

type endpointTestCase struct {
	endpoint baseAPI.ApiFunc
	params   map[string]string
	query    url.Values
	method   string
	response interface{}
	errType  baseAPI.ErrorType
}
type responeCompareFunction func(interface{}, interface{}) bool

// Checks if both responses have Stats present or not.
func lookupStats(a, b interface{}) bool {
	ra := a.(*queryData)
	rb := b.(*queryData)
	return (ra.Stats == nil && rb.Stats == nil) || (ra.Stats != nil && rb.Stats != nil)
}

func testEndpoint(t *testing.T, test endpointTestCase, name string, responseCompareFunc responeCompareFunction) bool {
	return t.Run(name, func(t *testing.T) {
		// Build a context with the correct request params.
		ctx := context.Background()
		for p, v := range test.params {
			ctx = route.WithParam(ctx, p, v)
		}

		reqURL := "http://example.com"
		params := test.query.Encode()

		var body io.Reader
		if test.method == http.MethodPost {
			body = strings.NewReader(params)
		} else if test.method == "" {
			test.method = "ANY"
			reqURL += "?" + params
		}

		req, err := http.NewRequest(test.method, reqURL, body)
		if err != nil {
			t.Fatal(err)
		}

		if body != nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}

		resp, _, apiErr, releaseResources := test.endpoint(req.WithContext(ctx))
		defer releaseResources()
		if apiErr != nil {
			if test.errType == baseAPI.ErrorNone {
				t.Fatalf("Unexpected error: %s", apiErr)
			}
			if test.errType != apiErr.Typ {
				t.Fatalf("Expected error of type %q but got type %q", test.errType, apiErr.Typ)
			}
			return
		}
		if test.errType != baseAPI.ErrorNone {
			t.Fatalf("Expected error of type %q but got none", test.errType)
		}

		if !responseCompareFunc(resp, test.response) {
			t.Fatalf("Response does not match, expected:\n%+v\ngot:\n%+v", test.response, resp)
		}
	})
}

func TestQueryEndpoints(t *testing.T) {
	lbls := []labels.Labels{
		{
			labels.Label{Name: "__name__", Value: "test_metric1"},
			labels.Label{Name: "foo", Value: "bar"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric1"},
			labels.Label{Name: "foo", Value: "boo"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric2"},
			labels.Label{Name: "foo", Value: "boo"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric_replica1"},
			labels.Label{Name: "foo", Value: "bar"},
			labels.Label{Name: "replica", Value: "a"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric_replica1"},
			labels.Label{Name: "foo", Value: "boo"},
			labels.Label{Name: "replica", Value: "a"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric_replica1"},
			labels.Label{Name: "foo", Value: "boo"},
			labels.Label{Name: "replica", Value: "b"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric_replica1"},
			labels.Label{Name: "foo", Value: "boo"},
			labels.Label{Name: "replica1", Value: "a"},
		},
	}

	db, err := e2eutil.NewTSDB()
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	app := db.Appender(context.Background())
	for _, lbl := range lbls {
		for i := int64(0); i < 10; i++ {
			_, err := app.Append(0, lbl, i*60000, float64(i))
			testutil.Ok(t, err)
		}
	}
	testutil.Ok(t, app.Commit())

	now := time.Now()
	timeout := 100 * time.Second
	qe := promql.NewEngine(promql.EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 10000,
		Timeout:    timeout,
	})
	api := &QueryAPI{
		baseAPI: &baseAPI.BaseAPI{
			Now: func() time.Time { return now },
		},
		queryableCreate:       query.NewQueryableCreator(nil, nil, store.NewTSDBStore(nil, db, component.Query, nil), 2, timeout),
		queryEngine:           qe,
		lookbackDeltaCreate:   func(m int64) time.Duration { return time.Duration(0) },
		gate:                  gate.New(nil, 4),
		defaultRangeQueryStep: time.Second,
		queryRangeHist: promauto.With(prometheus.NewRegistry()).NewHistogram(prometheus.HistogramOpts{
			Name: "query_range_hist",
		}),
		seriesStatsAggregator: &store.NoopSeriesStatsAggregator{},
	}

	start := time.Unix(0, 0)

	var tests = []endpointTestCase{
		{
			endpoint: api.query,
			query: url.Values{
				"query": []string{"2"},
				"time":  []string{"123.4"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeScalar,
				Result: promql.Scalar{
					V: 2,
					T: timestamp.FromTime(start.Add(123*time.Second + 400*time.Millisecond)),
				},
			},
		},
		{
			endpoint: api.query,
			query: url.Values{
				"query": []string{"0.333"},
				"time":  []string{"1970-01-01T00:02:03Z"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeScalar,
				Result: promql.Scalar{
					V: 0.333,
					T: timestamp.FromTime(start.Add(123 * time.Second)),
				},
			},
		},
		{
			endpoint: api.query,
			query: url.Values{
				"query": []string{"0.333"},
				"time":  []string{"1970-01-01T01:02:03+01:00"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeScalar,
				Result: promql.Scalar{
					V: 0.333,
					T: timestamp.FromTime(start.Add(123 * time.Second)),
				},
			},
		},
		// Query endpoint without deduplication.
		{
			endpoint: api.query,
			query: url.Values{
				"query": []string{"test_metric_replica1"},
				"time":  []string{"1970-01-01T01:02:03+01:00"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					{
						Metric: labels.Labels{
							{
								Name:  "__name__",
								Value: "test_metric_replica1",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
							{
								Name:  "replica",
								Value: "a",
							},
						},
						Point: promql.Point{
							T: 123000,
							V: 2,
						},
					},
					{
						Metric: labels.Labels{
							{
								Name:  "__name__",
								Value: "test_metric_replica1",
							},
							{
								Name:  "foo",
								Value: "boo",
							},
							{
								Name:  "replica",
								Value: "a",
							},
						},
						Point: promql.Point{
							T: 123000,
							V: 2,
						},
					},
					{
						Metric: labels.Labels{
							{
								Name:  "__name__",
								Value: "test_metric_replica1",
							},
							{
								Name:  "foo",
								Value: "boo",
							},
							{
								Name:  "replica",
								Value: "b",
							},
						},
						Point: promql.Point{
							T: 123000,
							V: 2,
						},
					},
					{
						Metric: labels.Labels{
							{
								Name:  "__name__",
								Value: "test_metric_replica1",
							},
							{
								Name:  "foo",
								Value: "boo",
							},
							{
								Name:  "replica1",
								Value: "a",
							},
						},
						Point: promql.Point{
							T: 123000,
							V: 2,
						},
					},
				},
			},
		},
		// Query endpoint with single deduplication label.
		{
			endpoint: api.query,
			query: url.Values{
				"query":           []string{"test_metric_replica1"},
				"time":            []string{"1970-01-01T01:02:03+01:00"},
				"replicaLabels[]": []string{"replica"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					{
						Metric: labels.Labels{
							{
								Name:  "__name__",
								Value: "test_metric_replica1",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Point: promql.Point{
							T: 123000,
							V: 2,
						},
					},
					{
						Metric: labels.Labels{
							{
								Name:  "__name__",
								Value: "test_metric_replica1",
							},
							{
								Name:  "foo",
								Value: "boo",
							},
						},
						Point: promql.Point{
							T: 123000,
							V: 2,
						},
					},
					{
						Metric: labels.Labels{
							{
								Name:  "__name__",
								Value: "test_metric_replica1",
							},
							{
								Name:  "foo",
								Value: "boo",
							},
							{
								Name:  "replica1",
								Value: "a",
							},
						},
						Point: promql.Point{
							T: 123000,
							V: 2,
						},
					},
				},
			},
		},
		// Query endpoint with multiple deduplication label.
		{
			endpoint: api.query,
			query: url.Values{
				"query":           []string{"test_metric_replica1"},
				"time":            []string{"1970-01-01T01:02:03+01:00"},
				"replicaLabels[]": []string{"replica", "replica1"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeVector,
				Result: promql.Vector{
					{
						Metric: labels.Labels{
							{
								Name:  "__name__",
								Value: "test_metric_replica1",
							},
							{
								Name:  "foo",
								Value: "bar",
							},
						},
						Point: promql.Point{
							T: 123000,
							V: 2,
						},
					},
					{
						Metric: labels.Labels{
							{
								Name:  "__name__",
								Value: "test_metric_replica1",
							},
							{
								Name:  "foo",
								Value: "boo",
							},
						},
						Point: promql.Point{
							T: 123000,
							V: 2,
						},
					},
				},
			},
		},
		{
			endpoint: api.query,
			query: url.Values{
				"query": []string{"0.333"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeScalar,
				Result: promql.Scalar{
					V: 0.333,
					T: timestamp.FromTime(now),
				},
			},
		},
		// Bad dedup parameter.
		{
			endpoint: api.query,
			query: url.Values{
				"query": []string{"0.333"},
				"dedup": []string{"sdfsf"},
			},
			errType: baseAPI.ErrorBadData,
		},
		{
			endpoint: api.queryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"0"},
				"end":   []string{"500"},
				"step":  []string{"1"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Points: func(end, step float64) []promql.Point {
							var res []promql.Point
							for v := float64(0); v <= end; v += step {
								res = append(res, promql.Point{V: v, T: timestamp.FromTime(start.Add(time.Duration(v) * time.Second))})
							}
							return res
						}(500, 1),
						Metric: nil,
					},
				},
			},
		},
		// Use default step when missing.
		{
			endpoint: api.queryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"0"},
				"end":   []string{"2"},
			},
			response: &queryData{
				ResultType: parser.ValueTypeMatrix,
				Result: promql.Matrix{
					promql.Series{
						Points: []promql.Point{
							{V: 0, T: timestamp.FromTime(start)},
							{V: 1, T: timestamp.FromTime(start.Add(1 * time.Second))},
							{V: 2, T: timestamp.FromTime(start.Add(2 * time.Second))},
						},
						Metric: nil,
					},
				},
			},
		},
		// Missing query params in range queries.
		{
			endpoint: api.queryRange,
			query: url.Values{
				"query": []string{"time()"},
				"end":   []string{"2"},
				"step":  []string{"1"},
			},
			errType: baseAPI.ErrorBadData,
		},
		{
			endpoint: api.queryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"0"},
				"step":  []string{"1"},
			},
			errType: baseAPI.ErrorBadData,
		},
		// Bad query expression.
		{
			endpoint: api.query,
			query: url.Values{
				"query": []string{"invalid][query"},
				"time":  []string{"1970-01-01T01:02:03+01:00"},
			},
			errType: baseAPI.ErrorBadData,
		},
		{
			endpoint: api.queryRange,
			query: url.Values{
				"query": []string{"invalid][query"},
				"start": []string{"0"},
				"end":   []string{"100"},
				"step":  []string{"1"},
			},
			errType: baseAPI.ErrorBadData,
		},
		// Invalid step.
		{
			endpoint: api.queryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"1"},
				"end":   []string{"2"},
				"step":  []string{"0"},
			},
			errType: baseAPI.ErrorBadData,
		},
		// Start after end.
		{
			endpoint: api.queryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"2"},
				"end":   []string{"1"},
				"step":  []string{"1"},
			},
			errType: baseAPI.ErrorBadData,
		},
		// Start overflows int64 internally.
		{
			endpoint: api.queryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"148966367200.372"},
				"end":   []string{"1489667272.372"},
				"step":  []string{"1"},
			},
			errType: baseAPI.ErrorBadData,
		},
		// Bad dedup parameter.
		{
			endpoint: api.queryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"0"},
				"end":   []string{"2"},
				"step":  []string{"1"},
				"dedup": []string{"sdfsf-range"},
			},
			errType: baseAPI.ErrorBadData,
		},
	}

	for i, test := range tests {
		if ok := testEndpoint(t, test, fmt.Sprintf("#%d %s", i, test.query.Encode()), reflect.DeepEqual); !ok {
			return
		}
	}

	qs := &stats.BuiltinStats{}
	tests = []endpointTestCase{
		{
			endpoint: api.query,
			query: url.Values{
				"query": []string{"2"},
				"time":  []string{"123.4"},
			},
			response: &queryData{},
		},
		{
			endpoint: api.query,
			query: url.Values{
				"query": []string{"2"},
				"time":  []string{"123.4"},
				"stats": []string{"true"},
			},
			response: &queryData{
				Stats: qs,
			},
		},
	}

	for i, test := range tests {
		if ok := testEndpoint(t, test, fmt.Sprintf("#%d %s", i, test.query.Encode()), lookupStats); !ok {
			return
		}
	}
}

func TestMetadataEndpoints(t *testing.T) {
	var old = []labels.Labels{
		{
			labels.Label{Name: "__name__", Value: "test_metric1"},
			labels.Label{Name: "foo", Value: "bar"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric1"},
			labels.Label{Name: "foo", Value: "boo"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric2"},
			labels.Label{Name: "foo", Value: "boo"},
		},
	}

	var recent = []labels.Labels{
		{
			labels.Label{Name: "__name__", Value: "test_metric_replica1"},
			labels.Label{Name: "foo", Value: "bar"},
			labels.Label{Name: "replica", Value: "a"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric_replica1"},
			labels.Label{Name: "foo", Value: "boo"},
			labels.Label{Name: "replica", Value: "a"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric_replica1"},
			labels.Label{Name: "foo", Value: "boo"},
			labels.Label{Name: "replica", Value: "b"},
		},
		{
			labels.Label{Name: "__name__", Value: "test_metric_replica2"},
			labels.Label{Name: "foo", Value: "boo"},
			labels.Label{Name: "replica1", Value: "a"},
		},
	}

	dir := t.TempDir()

	const chunkRange int64 = 600_000
	var series []storage.Series

	for _, lbl := range old {
		var samples []tsdbutil.Sample

		for i := int64(0); i < 10; i++ {
			samples = append(samples, sample{
				t: i * 60_000,
				v: float64(i),
			})
		}

		series = append(series, storage.NewListSeries(lbl, samples))
	}

	_, err := tsdb.CreateBlock(series, dir, chunkRange, log.NewNopLogger())
	testutil.Ok(t, err)

	opts := tsdb.DefaultOptions()
	opts.RetentionDuration = math.MaxInt64
	db, err := tsdb.Open(dir, nil, nil, opts, nil)
	defer func() { testutil.Ok(t, db.Close()) }()
	testutil.Ok(t, err)

	var (
		apiLookbackDelta = 2 * time.Hour
		start            = time.Now().Add(-apiLookbackDelta).Unix() * 1000
		app              = db.Appender(context.Background())
	)
	for _, lbl := range recent {
		for i := int64(0); i < 10; i++ {
			_, err := app.Append(0, lbl, start+(i*60_000), float64(i)) // ms
			testutil.Ok(t, err)
		}
	}
	testutil.Ok(t, app.Commit())

	now := time.Now()
	timeout := 100 * time.Second
	qe := promql.NewEngine(promql.EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 10000,
		Timeout:    timeout,
	})
	api := &QueryAPI{
		baseAPI: &baseAPI.BaseAPI{
			Now: func() time.Time { return now },
		},
		queryableCreate:     query.NewQueryableCreator(nil, nil, store.NewTSDBStore(nil, db, component.Query, nil), 2, timeout),
		queryEngine:         qe,
		lookbackDeltaCreate: func(m int64) time.Duration { return time.Duration(0) },
		gate:                gate.New(nil, 4),
		queryRangeHist: promauto.With(prometheus.NewRegistry()).NewHistogram(prometheus.HistogramOpts{
			Name: "query_range_hist",
		}),
		seriesStatsAggregator: &store.NoopSeriesStatsAggregator{},
	}
	apiWithLabelLookback := &QueryAPI{
		baseAPI: &baseAPI.BaseAPI{
			Now: func() time.Time { return now },
		},
		queryableCreate:          query.NewQueryableCreator(nil, nil, store.NewTSDBStore(nil, db, component.Query, nil), 2, timeout),
		queryEngine:              qe,
		lookbackDeltaCreate:      func(m int64) time.Duration { return time.Duration(0) },
		gate:                     gate.New(nil, 4),
		defaultMetadataTimeRange: apiLookbackDelta,
		queryRangeHist: promauto.With(prometheus.NewRegistry()).NewHistogram(prometheus.HistogramOpts{
			Name: "query_range_hist",
		}),
		seriesStatsAggregator: &store.NoopSeriesStatsAggregator{},
	}

	var tests = []endpointTestCase{
		{
			endpoint: api.labelValues,
			params: map[string]string{
				"name": "__name__",
			},
			response: []string{
				"test_metric1",
				"test_metric2",
				"test_metric_replica1",
				"test_metric_replica2",
			},
		},
		{
			endpoint: apiWithLabelLookback.labelValues,
			params: map[string]string{
				"name": "__name__",
			},
			response: []string{
				"test_metric_replica1",
				"test_metric_replica2",
			},
		},
		{
			endpoint: api.labelValues,
			query: url.Values{
				"start": []string{"1970-01-01T00:00:00Z"},
				"end":   []string{"1970-01-01T00:09:00Z"},
			},
			params: map[string]string{
				"name": "__name__",
			},
			response: []string{
				"test_metric1",
				"test_metric2",
			},
		},
		{
			endpoint: apiWithLabelLookback.labelValues,
			query: url.Values{
				"start": []string{"1970-01-01T00:00:00Z"},
				"end":   []string{"1970-01-01T00:09:00Z"},
			},
			params: map[string]string{
				"name": "__name__",
			},
			response: []string{
				"test_metric1",
				"test_metric2",
			},
		},
		{
			endpoint: api.labelNames,
			response: []string{
				"__name__",
				"foo",
				"replica",
				"replica1",
			},
		},
		{
			endpoint: apiWithLabelLookback.labelNames,
			response: []string{
				"__name__",
				"foo",
				"replica",
				"replica1",
			},
		},
		{
			endpoint: api.labelNames,
			query: url.Values{
				"start": []string{"1970-01-01T00:00:00Z"},
				"end":   []string{"1970-01-01T00:09:00Z"},
			},
			response: []string{
				"__name__",
				"foo",
			},
		},
		{
			endpoint: apiWithLabelLookback.labelNames,
			query: url.Values{
				"start": []string{"1970-01-01T00:00:00Z"},
				"end":   []string{"1970-01-01T00:09:00Z"},
			},
			response: []string{
				"__name__",
				"foo",
			},
		},
		// Failed, to parse matchers.
		{
			endpoint: api.labelNames,
			query: url.Values{
				"match[]": []string{`{xxxx`},
			},
			errType: baseAPI.ErrorBadData,
		},
		// Failed to parse matchers.
		{
			endpoint: api.labelValues,
			query: url.Values{
				"match[]": []string{`{xxxx`},
			},
			params: map[string]string{
				"name": "__name__",
			},
			errType: baseAPI.ErrorBadData,
		},
		{
			endpoint: api.labelNames,
			query: url.Values{
				"match[]": []string{`test_metric_replica2`},
			},
			response: []string{"__name__", "foo", "replica1"},
		},
		{
			endpoint: api.labelValues,
			query: url.Values{
				"match[]": []string{`test_metric_replica2`},
			},
			params: map[string]string{
				"name": "__name__",
			},
			response: []string{"test_metric_replica2"},
		},
		{
			endpoint: api.labelValues,
			query: url.Values{
				"match[]": []string{`{foo="bar"}`, `{foo="boo"}`},
			},
			params: map[string]string{
				"name": "__name__",
			},
			response: []string{"test_metric1", "test_metric2", "test_metric_replica1", "test_metric_replica2"},
		},
		// No matched series.
		{
			endpoint: api.labelValues,
			query: url.Values{
				"match[]": []string{`{foo="yolo"}`},
			},
			params: map[string]string{
				"name": "__name__",
			},
			response: []string{},
		},
		{
			endpoint: api.labelValues,
			query: url.Values{
				"match[]": []string{`test_metric_replica2`},
			},
			params: map[string]string{
				"name": "replica1",
			},
			response: []string{"a"},
		},
		// Bad name parameter.
		{
			endpoint: api.labelValues,
			params: map[string]string{
				"name": "not!!!allowed",
			},
			errType: baseAPI.ErrorBadData,
		},
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric2", "foo", "boo"),
			},
		},
		{
			endpoint: apiWithLabelLookback.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
			},
			response: []labels.Labels{},
		},
		{
			endpoint: apiWithLabelLookback.series,
			query: url.Values{
				"match[]": []string{`test_metric_replica1`},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric_replica1", "foo", "bar", "replica", "a"),
				labels.FromStrings("__name__", "test_metric_replica1", "foo", "boo", "replica", "a"),
				labels.FromStrings("__name__", "test_metric_replica1", "foo", "boo", "replica", "b"),
			},
		},
		// Series that does not exist should return an empty array.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`foobar`},
			},
			response: []labels.Labels{},
		},
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric1{foo=~".+o"}`},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric1", "foo", "boo"),
			},
		},
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric1{foo=~".+o$"}`, `test_metric1{foo=~".+o"}`},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric1", "foo", "boo"),
			},
		},
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric1{foo=~".+o"}`, `none`},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric1", "foo", "boo"),
			},
		},
		// Start and end before series starts.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"-2"},
				"end":     []string{"-1"},
			},
			response: []labels.Labels{},
		},
		// Start and end after series ends.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"100000"},
				"end":     []string{"100001"},
			},
			response: []labels.Labels{},
		},
		// Start before series starts, end after series ends.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"-1"},
				"end":     []string{"100000"},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric2", "foo", "boo"),
			},
		},
		// Start and end within series.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"1"},
				"end":     []string{"100"},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric2", "foo", "boo"),
			},
		},
		// Start within series, end after.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"1"},
				"end":     []string{"100000"},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric2", "foo", "boo"),
			},
		},
		// Start before series, end within series.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"-1"},
				"end":     []string{"1"},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric2", "foo", "boo"),
			},
		},
		// Missing match[] query params in series requests.
		{
			endpoint: api.series,
			errType:  baseAPI.ErrorBadData,
		},
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"dedup":   []string{"sdfsf-series"},
			},
			errType: baseAPI.ErrorBadData,
		},
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric2", "foo", "boo"),
			},
			method: http.MethodPost,
		},
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric1{foo=~".+o"}`},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric1", "foo", "boo"),
			},
			method: http.MethodPost,
		},
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric1{foo=~".+o$"}`, `test_metric1{foo=~".+o"}`},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric1", "foo", "boo"),
			},
			method: http.MethodPost,
		},
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric1{foo=~".+o"}`, `none`},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric1", "foo", "boo"),
			},
			method: http.MethodPost,
		},
		// Start and end before series starts.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"-2"},
				"end":     []string{"-1"},
			},
			response: []labels.Labels{},
		},
		// Start and end after series ends.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"100000"},
				"end":     []string{"100001"},
			},
			response: []labels.Labels{},
		},
		// Start before series starts, end after series ends.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"-1"},
				"end":     []string{"100000"},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric2", "foo", "boo"),
			},
			method: http.MethodPost,
		},
		// Start and end within series.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"1"},
				"end":     []string{"100"},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric2", "foo", "boo"),
			},
			method: http.MethodPost,
		},
		// Start within series, end after.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"1"},
				"end":     []string{"100000"},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric2", "foo", "boo"),
			},
			method: http.MethodPost,
		},
		// Start before series, end within series.
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"start":   []string{"-1"},
				"end":     []string{"1"},
			},
			response: []labels.Labels{
				labels.FromStrings("__name__", "test_metric2", "foo", "boo"),
			},
			method: http.MethodPost,
		},
		// Missing match[] query params in series requests.
		{
			endpoint: api.series,
			errType:  baseAPI.ErrorBadData,
			method:   http.MethodPost,
		},
		{
			endpoint: api.series,
			query: url.Values{
				"match[]": []string{`test_metric2`},
				"dedup":   []string{"sdfsf-series"},
			},
			errType: baseAPI.ErrorBadData,
			method:  http.MethodPost,
		},
	}

	for i, test := range tests {
		if ok := testEndpoint(t, test, strings.TrimSpace(fmt.Sprintf("#%d %s", i, test.query.Encode())), reflect.DeepEqual); !ok {
			return
		}
	}
}

func TestStoresEndpoint(t *testing.T) {
	apiWithNotEndpoints := &QueryAPI{
		endpointStatus: func() []query.EndpointStatus {
			return []query.EndpointStatus{}
		},
	}
	apiWithValidEndpoints := &QueryAPI{
		endpointStatus: func() []query.EndpointStatus {
			return []query.EndpointStatus{
				{
					Name:          "endpoint-1",
					ComponentType: component.Store,
				},
				{
					Name:          "endpoint-2",
					ComponentType: component.Store,
				},
				{
					Name:          "endpoint-3",
					ComponentType: component.Sidecar,
				},
			}
		},
	}
	apiWithInvalidEndpoint := &QueryAPI{
		endpointStatus: func() []query.EndpointStatus {
			return []query.EndpointStatus{
				{
					Name:          "endpoint-1",
					ComponentType: component.Store,
				},
				{
					Name: "endpoint-2",
				},
			}
		},
	}

	testCases := []endpointTestCase{
		{
			endpoint: apiWithNotEndpoints.stores,
			method:   http.MethodGet,
			response: map[string][]query.EndpointStatus{},
		},
		{
			endpoint: apiWithValidEndpoints.stores,
			method:   http.MethodGet,
			response: map[string][]query.EndpointStatus{
				"store": {
					{
						Name:          "endpoint-1",
						ComponentType: component.Store,
					},
					{
						Name:          "endpoint-2",
						ComponentType: component.Store,
					},
				},
				"sidecar": {
					{
						Name:          "endpoint-3",
						ComponentType: component.Sidecar,
					},
				},
			},
		},
		{
			endpoint: apiWithInvalidEndpoint.stores,
			method:   http.MethodGet,
			response: map[string][]query.EndpointStatus{
				"store": {
					{
						Name:          "endpoint-1",
						ComponentType: component.Store,
					},
				},
			},
		},
	}

	for i, test := range testCases {
		if ok := testEndpoint(t, test, strings.TrimSpace(fmt.Sprintf("#%d %s", i, test.query.Encode())), reflect.DeepEqual); !ok {
			return
		}
	}
}

func TestParseTime(t *testing.T) {
	ts, err := time.Parse(time.RFC3339Nano, "2015-06-03T13:21:58.555Z")
	if err != nil {
		panic(err)
	}

	var tests = []struct {
		input  string
		fail   bool
		result time.Time
	}{
		{
			input: "",
			fail:  true,
		}, {
			input: "abc",
			fail:  true,
		}, {
			input: "30s",
			fail:  true,
		}, {
			input:  "123",
			result: time.Unix(123, 0),
		}, {
			input:  "123.123",
			result: time.Unix(123, 123000000),
		}, {
			input:  "2015-06-03T13:21:58.555Z",
			result: ts,
		}, {
			input:  "2015-06-03T14:21:58.555+01:00",
			result: ts,
		}, {
			// Test float rounding.
			input:  "1543578564.705",
			result: time.Unix(1543578564, 705*1e6),
		},
	}

	for _, test := range tests {
		ts, err := parseTime(test.input)
		if err != nil && !test.fail {
			t.Errorf("Unexpected error for %q: %s", test.input, err)
			continue
		}
		if err == nil && test.fail {
			t.Errorf("Expected error for %q but got none", test.input)
			continue
		}
		if !test.fail && !ts.Equal(test.result) {
			t.Errorf("Expected time %v for input %q but got %v", test.result, test.input, ts)
		}
	}
}

func TestParseDuration(t *testing.T) {
	var tests = []struct {
		input  string
		fail   bool
		result time.Duration
	}{
		{
			input: "",
			fail:  true,
		}, {
			input: "abc",
			fail:  true,
		}, {
			input: "2015-06-03T13:21:58.555Z",
			fail:  true,
		}, {
			// Internal int64 overflow.
			input: "-148966367200.372",
			fail:  true,
		}, {
			// Internal int64 overflow.
			input: "148966367200.372",
			fail:  true,
		}, {
			input:  "123",
			result: 123 * time.Second,
		}, {
			input:  "123.333",
			result: 123*time.Second + 333*time.Millisecond,
		}, {
			input:  "15s",
			result: 15 * time.Second,
		}, {
			input:  "5m",
			result: 5 * time.Minute,
		},
	}

	for _, test := range tests {
		d, err := parseDuration(test.input)
		if err != nil && !test.fail {
			t.Errorf("Unexpected error for %q: %s", test.input, err)
			continue
		}
		if err == nil && test.fail {
			t.Errorf("Expected error for %q but got none", test.input)
			continue
		}
		if !test.fail && d != test.result {
			t.Errorf("Expected duration %v for input %q but got %v", test.result, test.input, d)
		}
	}
}

func TestParseDownsamplingParamMillis(t *testing.T) {
	var tests = []struct {
		maxSourceResolutionParam string
		result                   int64
		step                     time.Duration
		fail                     bool
		enableAutodownsampling   bool
	}{
		{
			maxSourceResolutionParam: "0s",
			enableAutodownsampling:   false,
			step:                     time.Hour,
			result:                   int64(compact.ResolutionLevelRaw),
			fail:                     false,
		},
		{
			maxSourceResolutionParam: "5m",
			step:                     time.Hour,
			enableAutodownsampling:   false,
			result:                   int64(compact.ResolutionLevel5m),
			fail:                     false,
		},
		{
			maxSourceResolutionParam: "1h",
			step:                     time.Hour,
			enableAutodownsampling:   false,
			result:                   int64(compact.ResolutionLevel1h),
			fail:                     false,
		},
		{
			maxSourceResolutionParam: "",
			enableAutodownsampling:   true,
			step:                     time.Hour,
			result:                   int64(time.Hour / (5 * 1000 * 1000)),
			fail:                     false,
		},
		{
			maxSourceResolutionParam: "",
			enableAutodownsampling:   true,
			step:                     time.Hour,
			result:                   int64((1 * time.Hour) / 6),
			fail:                     true,
		},
		{
			maxSourceResolutionParam: "",
			enableAutodownsampling:   true,
			step:                     time.Hour,
			result:                   int64((1 * time.Hour) / 6),
			fail:                     true,
		},
		// maxSourceResolution param can be overwritten.
		{
			maxSourceResolutionParam: "1m",
			enableAutodownsampling:   true,
			step:                     time.Hour,
			result:                   int64(time.Minute / (1000 * 1000)),
			fail:                     false,
		},
	}

	for i, test := range tests {
		api := QueryAPI{
			enableAutodownsampling: test.enableAutodownsampling,
			gate:                   gate.New(nil, 4),
			queryRangeHist: promauto.With(prometheus.NewRegistry()).NewHistogram(prometheus.HistogramOpts{
				Name: "query_range_hist",
			}),
		}
		v := url.Values{}
		v.Set(MaxSourceResolutionParam, test.maxSourceResolutionParam)
		r := http.Request{PostForm: v}

		// If no max_source_resolution is specified fit at least 5 samples between steps.
		maxResMillis, _ := api.parseDownsamplingParamMillis(&r, test.step/5)
		if test.fail == false {
			testutil.Assert(t, maxResMillis == test.result, "case %v: expected %v to be equal to %v", i, maxResMillis, test.result)
		} else {
			testutil.Assert(t, maxResMillis != test.result, "case %v: expected %v not to be equal to %v", i, maxResMillis, test.result)
		}

	}
}

func TestParseStoreDebugMatchersParam(t *testing.T) {
	for i, tc := range []struct {
		storeMatchers string
		fail          bool
		result        [][]*labels.Matcher
	}{
		{
			storeMatchers: "123",
			fail:          true,
		},
		{
			storeMatchers: "foo",
			fail:          false,
			result:        [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo")}},
		},
		{
			storeMatchers: `{__address__="localhost:10905"}`,
			fail:          false,
			result:        [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "__address__", "localhost:10905")}},
		},
		{
			storeMatchers: `{__address__="localhost:10905", cluster="test"}`,
			fail:          false,
			result: [][]*labels.Matcher{{
				labels.MustNewMatcher(labels.MatchEqual, "__address__", "localhost:10905"),
				labels.MustNewMatcher(labels.MatchEqual, "cluster", "test"),
			}},
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			api := QueryAPI{
				gate: promgate.New(4),
				queryRangeHist: promauto.With(prometheus.NewRegistry()).NewHistogram(prometheus.HistogramOpts{
					Name: "query_range_hist",
				}),
			}
			v := url.Values{}
			v.Set(StoreMatcherParam, tc.storeMatchers)
			r := &http.Request{PostForm: v}

			storeMatchers, err := api.parseStoreDebugMatchersParam(r)
			if !tc.fail {
				testutil.Equals(t, tc.result, storeMatchers)
				testutil.Equals(t, (*baseAPI.ApiError)(nil), err)
			} else {
				testutil.NotOk(t, err)
			}
		})
	}
}

func TestRulesHandler(t *testing.T) {
	twoHAgo := time.Now().Add(-2 * time.Hour)
	all := []*rulespb.Rule{
		rulespb.NewRecordingRule(&rulespb.RecordingRule{
			Name:                      "1",
			LastEvaluation:            time.Time{}.Add(1 * time.Minute),
			EvaluationDurationSeconds: 12,
			Health:                    "x",
			Query:                     "sum(up)",
			Labels:                    labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "some", Value: "label"}}},
			LastError:                 "err1",
		}),
		rulespb.NewRecordingRule(&rulespb.RecordingRule{
			Name:                      "2",
			LastEvaluation:            time.Time{}.Add(2 * time.Minute),
			EvaluationDurationSeconds: 12,
			Health:                    "x",
			Query:                     "sum(up1)",
			Labels:                    labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "some", Value: "label2"}}},
		}),
		rulespb.NewAlertingRule(&rulespb.Alert{
			Name:                      "3",
			LastEvaluation:            time.Time{}.Add(3 * time.Minute),
			EvaluationDurationSeconds: 12,
			Health:                    "x",
			Query:                     "sum(up2) == 2",
			DurationSeconds:           101,
			Labels:                    labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "some", Value: "label3"}}},
			Annotations:               labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "ann", Value: "a1"}}},
			Alerts: []*rulespb.AlertInstance{
				{
					Labels:      labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "inside", Value: "1"}}},
					Annotations: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "insideann", Value: "2"}}},
					State:       rulespb.AlertState_FIRING,
					ActiveAt:    &twoHAgo,
					Value:       "1",
					// This is unlikely if groups is warn, but test nevertheless.
					PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
				},
				{
					Labels:      labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "inside", Value: "3"}}},
					Annotations: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "insideann", Value: "4"}}},
					State:       rulespb.AlertState_PENDING,
					ActiveAt:    nil,
					Value:       "2",
					// This is unlikely if groups is warn, but test nevertheless.
					PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
				},
			},
			State: rulespb.AlertState_FIRING,
		}),
		rulespb.NewAlertingRule(&rulespb.Alert{
			Name:                      "4",
			LastEvaluation:            time.Time{}.Add(4 * time.Minute),
			EvaluationDurationSeconds: 122,
			Health:                    "x",
			DurationSeconds:           102,
			Query:                     "sum(up3) == 3",
			Labels:                    labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "some", Value: "label4"}}},
			State:                     rulespb.AlertState_INACTIVE,
		}),
	}

	endpoint := NewRulesHandler(mockedRulesClient{
		g: map[rulespb.RulesRequest_Type][]*rulespb.RuleGroup{
			rulespb.RulesRequest_ALL: {
				{
					Name:                      "grp",
					File:                      "/path/to/groupfile1",
					Rules:                     all,
					Interval:                  1,
					EvaluationDurationSeconds: 214,
					LastEvaluation:            time.Time{}.Add(10 * time.Minute),
					PartialResponseStrategy:   storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                      "grp2",
					File:                      "/path/to/groupfile2",
					Rules:                     all[3:],
					Interval:                  10,
					EvaluationDurationSeconds: 2142,
					LastEvaluation:            time.Time{}.Add(100 * time.Minute),
					PartialResponseStrategy:   storepb.PartialResponseStrategy_ABORT,
				},
			},
			rulespb.RulesRequest_RECORD: {
				{
					Name:                      "grp",
					File:                      "/path/to/groupfile1",
					Rules:                     all[:2],
					Interval:                  1,
					EvaluationDurationSeconds: 214,
					LastEvaluation:            time.Time{}.Add(20 * time.Minute),
					PartialResponseStrategy:   storepb.PartialResponseStrategy_WARN,
				},
			},
			rulespb.RulesRequest_ALERT: {
				{
					Name:                      "grp",
					File:                      "/path/to/groupfile1",
					Rules:                     all[2:],
					Interval:                  1,
					EvaluationDurationSeconds: 214,
					LastEvaluation:            time.Time{}.Add(30 * time.Minute),
					PartialResponseStrategy:   storepb.PartialResponseStrategy_WARN,
				},
			},
		},
	}, false)

	type test struct {
		params   map[string]string
		query    url.Values
		response interface{}
	}
	expectedAll := []testpromcompatibility.Rule{
		testpromcompatibility.RecordingRule{
			Name:           all[0].GetRecording().Name,
			Query:          all[0].GetRecording().Query,
			Labels:         labelpb.ZLabelsToPromLabels(all[0].GetRecording().Labels.Labels),
			Health:         rules.RuleHealth(all[0].GetRecording().Health),
			LastError:      all[0].GetRecording().LastError,
			LastEvaluation: all[0].GetRecording().LastEvaluation,
			EvaluationTime: all[0].GetRecording().EvaluationDurationSeconds,
			Type:           "recording",
		},
		testpromcompatibility.RecordingRule{
			Name:           all[1].GetRecording().Name,
			Query:          all[1].GetRecording().Query,
			Labels:         labelpb.ZLabelsToPromLabels(all[1].GetRecording().Labels.Labels),
			Health:         rules.RuleHealth(all[1].GetRecording().Health),
			LastError:      all[1].GetRecording().LastError,
			LastEvaluation: all[1].GetRecording().LastEvaluation,
			EvaluationTime: all[1].GetRecording().EvaluationDurationSeconds,
			Type:           "recording",
		},
		testpromcompatibility.AlertingRule{
			State:          strings.ToLower(all[2].GetAlert().State.String()),
			Name:           all[2].GetAlert().Name,
			Query:          all[2].GetAlert().Query,
			Labels:         labelpb.ZLabelsToPromLabels(all[2].GetAlert().Labels.Labels),
			Health:         rules.RuleHealth(all[2].GetAlert().Health),
			LastError:      all[2].GetAlert().LastError,
			LastEvaluation: all[2].GetAlert().LastEvaluation,
			EvaluationTime: all[2].GetAlert().EvaluationDurationSeconds,
			Duration:       all[2].GetAlert().DurationSeconds,
			Annotations:    labelpb.ZLabelsToPromLabels(all[2].GetAlert().Annotations.Labels),
			Alerts: []*testpromcompatibility.Alert{
				{
					Labels:                  labelpb.ZLabelsToPromLabels(all[2].GetAlert().Alerts[0].Labels.Labels),
					Annotations:             labelpb.ZLabelsToPromLabels(all[2].GetAlert().Alerts[0].Annotations.Labels),
					State:                   strings.ToLower(all[2].GetAlert().Alerts[0].State.String()),
					ActiveAt:                all[2].GetAlert().Alerts[0].ActiveAt,
					Value:                   all[2].GetAlert().Alerts[0].Value,
					PartialResponseStrategy: all[2].GetAlert().Alerts[0].PartialResponseStrategy.String(),
				},
				{
					Labels:                  labelpb.ZLabelsToPromLabels(all[2].GetAlert().Alerts[1].Labels.Labels),
					Annotations:             labelpb.ZLabelsToPromLabels(all[2].GetAlert().Alerts[1].Annotations.Labels),
					State:                   strings.ToLower(all[2].GetAlert().Alerts[1].State.String()),
					ActiveAt:                all[2].GetAlert().Alerts[1].ActiveAt,
					Value:                   all[2].GetAlert().Alerts[1].Value,
					PartialResponseStrategy: all[2].GetAlert().Alerts[1].PartialResponseStrategy.String(),
				},
			},
			Type: "alerting",
		},
		testpromcompatibility.AlertingRule{
			State:          strings.ToLower(all[3].GetAlert().State.String()),
			Name:           all[3].GetAlert().Name,
			Query:          all[3].GetAlert().Query,
			Labels:         labelpb.ZLabelsToPromLabels(all[3].GetAlert().Labels.Labels),
			Health:         rules.RuleHealth(all[2].GetAlert().Health),
			LastError:      all[3].GetAlert().LastError,
			LastEvaluation: all[3].GetAlert().LastEvaluation,
			EvaluationTime: all[3].GetAlert().EvaluationDurationSeconds,
			Duration:       all[3].GetAlert().DurationSeconds,
			Annotations:    nil,
			Alerts:         []*testpromcompatibility.Alert{},
			Type:           "alerting",
		},
	}
	for _, test := range []test{
		{
			response: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name:                    "grp",
						File:                    "/path/to/groupfile1",
						Rules:                   expectedAll,
						Interval:                1,
						EvaluationTime:          214,
						LastEvaluation:          time.Time{}.Add(10 * time.Minute),
						PartialResponseStrategy: "WARN",
					},
					{
						Name:                    "grp2",
						File:                    "/path/to/groupfile2",
						Rules:                   expectedAll[3:],
						Interval:                10,
						EvaluationTime:          2142,
						LastEvaluation:          time.Time{}.Add(100 * time.Minute),
						PartialResponseStrategy: "ABORT",
					},
				},
			},
		},
		{
			query: url.Values{"type": []string{"record"}},
			response: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name:                    "grp",
						File:                    "/path/to/groupfile1",
						Rules:                   expectedAll[:2],
						Interval:                1,
						EvaluationTime:          214,
						LastEvaluation:          time.Time{}.Add(20 * time.Minute),
						PartialResponseStrategy: "WARN",
					},
				},
			},
		},
		{
			query: url.Values{"type": []string{"alert"}},
			response: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name:                    "grp",
						File:                    "/path/to/groupfile1",
						Rules:                   expectedAll[2:],
						Interval:                1,
						EvaluationTime:          214,
						LastEvaluation:          time.Time{}.Add(30 * time.Minute),
						PartialResponseStrategy: "WARN",
					},
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("endpoint=%s/method=%s/query=%q", "rules", http.MethodGet, test.query.Encode()), func(t *testing.T) {
			// Build a context with the correct request params.
			ctx := context.Background()
			for p, v := range test.params {
				ctx = route.WithParam(ctx, p, v)
			}

			req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://example.com?%s", test.query.Encode()), nil)
			if err != nil {
				t.Fatal(err)
			}
			res, errors, apiError, releaseResources := endpoint(req.WithContext(ctx))
			defer releaseResources()
			if errors != nil {
				t.Fatalf("Unexpected errors: %s", errors)
				return
			}
			testutil.Assert(t, apiError == nil, "unexpected error %v", apiError)

			// Those are different types now, but let's JSON outputs.
			got, err := json.MarshalIndent(res, "", " ")
			testutil.Ok(t, err)
			exp, err := json.MarshalIndent(test.response, "", " ")
			testutil.Ok(t, err)

			testutil.Equals(t, string(exp), string(got))
		})
	}
}

type hintRecordingQuerier struct {
	storepb.StoreServer
	mux   sync.Mutex
	hints []storepb.QueryHints
}

func (h *hintRecordingQuerier) Series(request *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	h.mux.Lock()
	defer h.mux.Unlock()
	h.hints = append(h.hints, *request.QueryHints)
	return nil
}

func TestSelectHintsSetCorrectly(t *testing.T) {
	for _, tc := range []struct {
		query string

		// All times are in milliseconds.
		start int64
		end   int64

		// TODO(bwplotka): Add support for better hints when subquerying.
		expected []storepb.QueryHints
	}{
		{
			query: "foo", start: 10000,
			expected: []storepb.QueryHints{
				{StartMillis: 5000, EndMillis: 10000},
			},
		}, {
			query: "foo @ 15", start: 10000,
			expected: []storepb.QueryHints{
				{StartMillis: 10000, EndMillis: 15000},
			},
		}, {
			query: "foo @ 1", start: 10000,
			expected: []storepb.QueryHints{
				{StartMillis: -4000, EndMillis: 1000},
			},
		}, {
			query: "foo[2m]", start: 200000,
			expected: []storepb.QueryHints{
				{StartMillis: 80000, EndMillis: 200000, Range: &storepb.Range{Millis: 120000}},
			},
		}, {
			query: "foo[2m] @ 180", start: 200000,
			expected: []storepb.QueryHints{
				{StartMillis: 60000, EndMillis: 180000, Range: &storepb.Range{Millis: 120000}},
			},
		}, {
			query: "foo[2m] @ 300", start: 200000,
			expected: []storepb.QueryHints{
				{StartMillis: 180000, EndMillis: 300000, Range: &storepb.Range{Millis: 120000}},
			},
		}, {
			query: "foo[2m] @ 60", start: 200000,
			expected: []storepb.QueryHints{
				{StartMillis: -60000, EndMillis: 60000, Range: &storepb.Range{Millis: 120000}},
			},
		}, {
			query: "foo[2m] offset 2m", start: 300000,
			expected: []storepb.QueryHints{
				{StartMillis: 60000, EndMillis: 180000, Range: &storepb.Range{Millis: 120000}},
			},
		}, {
			query: "foo[2m] @ 200 offset 2m", start: 300000,
			expected: []storepb.QueryHints{
				{StartMillis: -40000, EndMillis: 80000, Range: &storepb.Range{Millis: 120000}},
			},
		}, {
			query: "foo[2m:1s]", start: 300000,
			expected: []storepb.QueryHints{
				{StartMillis: 175000, EndMillis: 300000, StepMillis: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s])", start: 300000,
			expected: []storepb.QueryHints{
				{StartMillis: 175000, EndMillis: 300000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 300)", start: 200000,
			expected: []storepb.QueryHints{
				{StartMillis: 175000, EndMillis: 300000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 200)", start: 200000,
			expected: []storepb.QueryHints{
				{StartMillis: 75000, EndMillis: 200000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 100)", start: 200000,
			expected: []storepb.QueryHints{
				{StartMillis: -25000, EndMillis: 100000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] offset 10s)", start: 300000,
			expected: []storepb.QueryHints{
				{StartMillis: 165000, EndMillis: 290000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time((foo offset 10s)[2m:1s] offset 10s)", start: 300000,
			expected: []storepb.QueryHints{
				{StartMillis: 155000, EndMillis: 280000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] offset 10s)", start: 300000,
			expected: []storepb.QueryHints{
				{StartMillis: 185000, EndMillis: 190000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000,
			expected: []storepb.QueryHints{
				{StartMillis: 185000, EndMillis: 190000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time((foo offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000,
			expected: []storepb.QueryHints{
				{StartMillis: -45000, EndMillis: 80000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "foo", start: 10000, end: 20000,
			expected: []storepb.QueryHints{
				{StartMillis: 5000, EndMillis: 20000, StepMillis: 1000},
			},
		}, {
			query: "foo @ 15", start: 10000, end: 20000,
			expected: []storepb.QueryHints{
				{StartMillis: 10000, EndMillis: 15000, StepMillis: 1000},
			},
		}, {
			query: "foo @ 1", start: 10000, end: 20000,
			expected: []storepb.QueryHints{
				{StartMillis: -4000, EndMillis: 1000, StepMillis: 1000},
			},
		}, {
			query: "rate(foo[2m] @ 180)", start: 200000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 60000, EndMillis: 180000, Range: &storepb.Range{Millis: 120000}, TimeFunc: &storepb.Func{Name: "rate"}, StepMillis: 1000},
			},
		}, {
			query: "rate(foo[2m] @ 300)", start: 200000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 180000, EndMillis: 300000, Range: &storepb.Range{Millis: 120000}, TimeFunc: &storepb.Func{Name: "rate"}, StepMillis: 1000},
			},
		}, {
			query: "rate(foo[2m] @ 60)", start: 200000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: -60000, EndMillis: 60000, Range: &storepb.Range{Millis: 120000}, TimeFunc: &storepb.Func{Name: "rate"}, StepMillis: 1000},
			},
		}, {
			query: "rate(foo[2m])", start: 200000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 80000, EndMillis: 500000, Range: &storepb.Range{Millis: 120000}, TimeFunc: &storepb.Func{Name: "rate"}, StepMillis: 1000},
			},
		}, {
			query: "rate(foo[2m] offset 2m)", start: 300000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 60000, EndMillis: 380000, Range: &storepb.Range{Millis: 120000}, TimeFunc: &storepb.Func{Name: "rate"}, StepMillis: 1000},
			},
		}, {
			query: "rate(foo[2m:1s])", start: 300000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 175000, EndMillis: 500000, TimeFunc: &storepb.Func{Name: "rate"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s])", start: 300000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 175000, EndMillis: 500000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] offset 10s)", start: 300000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 165000, EndMillis: 490000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 300)", start: 200000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 175000, EndMillis: 300000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 200)", start: 200000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 75000, EndMillis: 200000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time(foo[2m:1s] @ 100)", start: 200000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: -25000, EndMillis: 100000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time((foo offset 10s)[2m:1s] offset 10s)", start: 300000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 155000, EndMillis: 480000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] offset 10s)", start: 300000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 185000, EndMillis: 190000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			// When the @ is on the vector selector, the enclosing subquery parameters
			// don't affect the hint ranges.
			query: "count_over_time((foo @ 200 offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 185000, EndMillis: 190000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "count_over_time((foo offset 10s)[2m:1s] @ 100 offset 10s)", start: 300000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: -45000, EndMillis: 80000, TimeFunc: &storepb.Func{Name: "count_over_time"}, StepMillis: 1000},
			},
		}, {
			query: "sum by (dim1) (foo)", start: 10000,
			expected: []storepb.QueryHints{
				{StartMillis: 5000, EndMillis: 10000, AggrFunc: &storepb.Func{Name: "sum"}, Grouping: &storepb.Grouping{By: true, Labels: []string{"dim1"}}},
			},
		}, {
			query: "sum without (dim1) (foo)", start: 10000,
			expected: []storepb.QueryHints{
				{StartMillis: 5000, EndMillis: 10000, AggrFunc: &storepb.Func{Name: "sum"}, Grouping: &storepb.Grouping{By: false, Labels: []string{"dim1"}}},
			},
		}, {
			query: "sum by (dim1) (avg_over_time(foo[1s]))", start: 10000,
			expected: []storepb.QueryHints{
				{StartMillis: 9000, EndMillis: 10000, AggrFunc: &storepb.Func{Name: "sum"}, Grouping: &storepb.Grouping{By: true, Labels: []string{"dim1"}}, TimeFunc: &storepb.Func{Name: "avg_over_time"}, Range: &storepb.Range{Millis: 1000}}},
		}, {
			query: "sum by (dim1) (sort(avg_over_time(foo[1s])))", start: 10000,
			expected: []storepb.QueryHints{
				{StartMillis: 9000, EndMillis: 10000, AggrFunc: &storepb.Func{Name: "sort"}, TimeFunc: &storepb.Func{Name: "avg_over_time"}, Range: &storepb.Range{Millis: 1000}}},
		}, {
			query: "sum by (dim1) (max by (dim2) (foo))", start: 10000,
			expected: []storepb.QueryHints{
				{StartMillis: 5000, EndMillis: 10000, AggrFunc: &storepb.Func{Name: "max"}, Grouping: &storepb.Grouping{By: true, Labels: []string{"dim2"}}},
			},
		}, {
			query: "(max by (dim1) (foo))[5s:1s]", start: 10000,
			expected: []storepb.QueryHints{
				{StartMillis: 0, EndMillis: 10000, AggrFunc: &storepb.Func{Name: "max"}, Grouping: &storepb.Grouping{By: true, Labels: []string{"dim1"}}, StepMillis: 1000},
			},
		}, {
			query: "(sum(http_requests{group=~\"p.*\"})+max(http_requests{group=~\"c.*\"}))[20s:5s]", start: 120000,
			expected: []storepb.QueryHints{
				{StartMillis: 95000, EndMillis: 120000, AggrFunc: &storepb.Func{Name: "sum"}, Grouping: &storepb.Grouping{By: true}, StepMillis: 5000},
				{StartMillis: 95000, EndMillis: 120000, AggrFunc: &storepb.Func{Name: "max"}, Grouping: &storepb.Grouping{By: true}, StepMillis: 5000},
			},
		}, {
			query: "foo @ 50 + bar @ 250 + baz @ 900", start: 100000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 45000, EndMillis: 50000, StepMillis: 1000},
				{StartMillis: 245000, EndMillis: 250000, StepMillis: 1000},
				{StartMillis: 895000, EndMillis: 900000, StepMillis: 1000},
			},
		}, {
			query: "foo @ 50 + bar + baz @ 900", start: 100000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 45000, EndMillis: 50000, StepMillis: 1000},
				{StartMillis: 95000, EndMillis: 500000, StepMillis: 1000},
				{StartMillis: 895000, EndMillis: 900000, StepMillis: 1000},
			},
		}, {
			query: "rate(foo[2s] @ 50) + bar @ 250 + baz @ 900", start: 100000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 48000, EndMillis: 50000, StepMillis: 1000, TimeFunc: &storepb.Func{Name: "rate"}, Range: &storepb.Range{Millis: 2000}},
				{StartMillis: 245000, EndMillis: 250000, StepMillis: 1000},
				{StartMillis: 895000, EndMillis: 900000, StepMillis: 1000},
			},
		}, {
			query: "rate(foo[2s:1s] @ 50) + bar + baz", start: 100000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 43000, EndMillis: 50000, StepMillis: 1000, TimeFunc: &storepb.Func{Name: "rate"}},
				{StartMillis: 95000, EndMillis: 500000, StepMillis: 1000},
				{StartMillis: 95000, EndMillis: 500000, StepMillis: 1000},
			},
		}, {
			query: "rate(foo[2s:1s] @ 50) + bar + rate(baz[2m:1s] @ 900 offset 2m) ", start: 100000, end: 500000,
			expected: []storepb.QueryHints{
				{StartMillis: 43000, EndMillis: 50000, StepMillis: 1000, TimeFunc: &storepb.Func{Name: "rate"}},
				{StartMillis: 95000, EndMillis: 500000, StepMillis: 1000},
				{StartMillis: 655000, EndMillis: 780000, StepMillis: 1000, TimeFunc: &storepb.Func{Name: "rate"}},
			},
		}, { // Hints are based on the inner most subquery timestamp.
			query: `sum_over_time(sum_over_time(metric{job="1"}[100s])[100s:25s] @ 50)[3s:1s] @ 3000`, start: 100000,
			expected: []storepb.QueryHints{
				{StartMillis: -150000, EndMillis: 50000, Range: &storepb.Range{Millis: 100000}, TimeFunc: &storepb.Func{Name: "sum_over_time"}, StepMillis: 25000}},
		},
		{ // Hints are based on the inner most subquery timestamp.
			query: `sum_over_time(sum_over_time(metric{job="1"}[100s])[100s:25s] @ 3000)[3s:1s] @ 50`,
			expected: []storepb.QueryHints{
				{StartMillis: 2800000, EndMillis: 3000000, Range: &storepb.Range{Millis: 100000}, TimeFunc: &storepb.Func{Name: "sum_over_time"}, StepMillis: 25000}},
		},
	} {
		t.Run(tc.query, func(t *testing.T) {
			opts := promql.EngineOpts{
				Logger:           nil,
				Reg:              nil,
				MaxSamples:       10,
				Timeout:          10 * time.Second,
				LookbackDelta:    5 * time.Second,
				EnableAtModifier: true,
			}

			reg := prometheus.NewRegistry()
			hintsRecorder := &hintRecordingQuerier{}
			qapi := QueryAPI{
				baseAPI:               baseAPI.NewBaseAPI(log.NewNopLogger(), false, nil),
				gate:                  gate.NewNoop(),
				seriesStatsAggregator: &store.NoopSeriesStatsAggregator{},
				lookbackDeltaCreate: func(i int64) time.Duration {
					return opts.LookbackDelta
				},
				queryableCreate:     query.NewQueryableCreator(log.NewNopLogger(), nil, hintsRecorder, 1, 5*time.Minute),
				queryEngine:         engine.New(engine.Opts{EngineOpts: opts}),
				enableQueryPushdown: true,
				queryRangeHist: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
					Name:    "query_duration_seconds",
					Buckets: []float64{1},
				}),
			}

			request := httptest.NewRequest("GET", "/", nil)
			q := request.URL.Query()
			q.Add("query", tc.query)
			if tc.end == 0 {
				q.Add("time", fmt.Sprintf("%d", tc.start/1000))
				request.URL.RawQuery = q.Encode()
				_, _, apiErr, _ := qapi.query(request)
				testutil.Assert(t, apiErr == nil, "unexpected error %v", apiErr)
			} else {
				q.Add("start", fmt.Sprintf("%d", tc.start/1000))
				q.Add("end", fmt.Sprintf("%d", tc.end/1000))
				q.Add("step", "1")
				request.URL.RawQuery = q.Encode()
				_, _, apiErr, _ := qapi.queryRange(request)
				testutil.Assert(t, apiErr == nil, "unexpected error %v", apiErr)
			}

			// Selects are done in parallel so check that all hints are // present, but order does not matter.
			testutil.Equals(t, len(tc.expected), len(hintsRecorder.hints))
			for _, expected := range tc.expected {
				contains := false
				for _, hint := range hintsRecorder.hints {
					if reflect.DeepEqual(expected, hint) {
						contains = true
					}
				}
				testutil.Assert(t, contains, "hints did not contain contain %#v", expected)
			}
		})
	}
}

func BenchmarkQueryResultEncoding(b *testing.B) {
	var mat promql.Matrix
	for i := 0; i < 1000; i++ {
		lset := labels.FromStrings(
			"__name__", "my_test_metric_name",
			"instance", fmt.Sprintf("abcdefghijklmnopqrstuvxyz-%d", i),
			"job", "test-test",
			"method", "ABCD",
			"status", "199",
			"namespace", "something",
			"long-label", "34grnt83j0qxj309je9rgt9jf2jd-92jd-92jf9wrfjre",
		)
		var points []promql.Point
		for j := 0; j < b.N/1000; j++ {
			points = append(points, promql.Point{
				T: int64(j * 10000),
				V: rand.Float64(),
			})
		}
		mat = append(mat, promql.Series{
			Metric: lset,
			Points: points,
		})
	}
	input := &queryData{
		ResultType: parser.ValueTypeMatrix,
		Result:     mat,
	}
	b.ResetTimer()

	_, err := json.Marshal(&input)
	testutil.Ok(b, err)
}

type mockedRulesClient struct {
	g   map[rulespb.RulesRequest_Type][]*rulespb.RuleGroup
	w   storage.Warnings
	err error
}

func (c mockedRulesClient) Rules(_ context.Context, req *rulespb.RulesRequest) (*rulespb.RuleGroups, storage.Warnings, error) {
	return &rulespb.RuleGroups{Groups: c.g[req.Type]}, c.w, c.err
}

type sample struct {
	t int64
	v float64
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) V() float64 {
	return s.v
}

// TODO(rabenhorst): Needs to be implemented for native histogram support.
func (s sample) H() *histogram.Histogram {
	panic("not implemented")
}

func (s sample) FH() *histogram.FloatHistogram {
	panic("not implemented")
}

func (s sample) Type() chunkenc.ValueType {
	return chunkenc.ValFloat
}
