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
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"

	baseAPI "github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/pkg/testutil/testpromcompatibility"
)

func TestMain(m *testing.M) {
	testutil.TolerantVerifyLeakMain(m)
}

func TestEndpoints(t *testing.T) {
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
			_, err := app.Add(lbl, i*60000, float64(i))
			testutil.Ok(t, err)
		}
	}
	testutil.Ok(t, app.Commit())

	now := time.Now()
	timeout := 100 * time.Second
	api := &QueryAPI{
		baseAPI: &baseAPI.BaseAPI{
			Now: func() time.Time { return now },
		},
		queryableCreate: query.NewQueryableCreator(nil, nil, store.NewTSDBStore(nil, nil, db, component.Query, nil), 2, timeout),
		queryEngine: promql.NewEngine(promql.EngineOpts{
			Logger:     nil,
			Reg:        nil,
			MaxSamples: 10000,
			Timeout:    timeout,
		}),
		gate: gate.NewKeeper(nil).NewGate(4),
	}

	start := time.Unix(0, 0)

	var tests = []struct {
		endpoint baseAPI.ApiFunc
		params   map[string]string
		query    url.Values
		method   string
		response interface{}
		errType  baseAPI.ErrorType
	}{
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
				"end":   []string{"2"},
				"step":  []string{"1"},
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
		{
			endpoint: api.queryRange,
			query: url.Values{
				"query": []string{"time()"},
				"start": []string{"0"},
				"end":   []string{"2"},
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
		{
			endpoint: api.labelValues,
			params: map[string]string{
				"name": "__name__",
			},
			response: []string{
				"test_metric1",
				"test_metric2",
				"test_metric_replica1",
			},
		},
		{
			endpoint: api.labelValues,
			params: map[string]string{
				"name": "foo",
			},
			response: []string{
				"bar",
				"boo",
			},
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

	for _, test := range tests {
		if ok := t.Run(test.query.Encode(), func(t *testing.T) {
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

			resp, _, apiErr := test.endpoint(req.WithContext(ctx))
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

			if !reflect.DeepEqual(resp, test.response) {
				t.Fatalf("Response does not match, expected:\n%+v\ngot:\n%+v", test.response, resp)
			}
		}); !ok {
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
			gate:                   gate.NewKeeper(nil).NewGate(4),
		}
		v := url.Values{}
		v.Set("max_source_resolution", test.maxSourceResolutionParam)
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

type mockedRulesClient struct {
	g   map[rulespb.RulesRequest_Type][]*rulespb.RuleGroup
	w   storage.Warnings
	err error
}

func (c mockedRulesClient) Rules(_ context.Context, req *rulespb.RulesRequest) (*rulespb.RuleGroups, storage.Warnings, error) {
	return &rulespb.RuleGroups{Groups: c.g[req.Type]}, c.w, c.err
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
			Labels:                    rulespb.PromLabels{Labels: []storepb.Label{{Name: "some", Value: "label"}}},
			LastError:                 "err1",
		}),
		rulespb.NewRecordingRule(&rulespb.RecordingRule{
			Name:                      "2",
			LastEvaluation:            time.Time{}.Add(2 * time.Minute),
			EvaluationDurationSeconds: 12,
			Health:                    "x",
			Query:                     "sum(up1)",
			Labels:                    rulespb.PromLabels{Labels: []storepb.Label{{Name: "some", Value: "label2"}}},
		}),
		rulespb.NewAlertingRule(&rulespb.Alert{
			Name:                      "3",
			LastEvaluation:            time.Time{}.Add(3 * time.Minute),
			EvaluationDurationSeconds: 12,
			Health:                    "x",
			Query:                     "sum(up2) == 2",
			DurationSeconds:           101,
			Labels:                    rulespb.PromLabels{Labels: []storepb.Label{{Name: "some", Value: "label3"}}},
			Annotations:               rulespb.PromLabels{Labels: []storepb.Label{{Name: "ann", Value: "a1"}}},
			Alerts: []*rulespb.AlertInstance{
				{
					Labels:      rulespb.PromLabels{Labels: []storepb.Label{{Name: "inside", Value: "1"}}},
					Annotations: rulespb.PromLabels{Labels: []storepb.Label{{Name: "insideann", Value: "2"}}},
					State:       rulespb.AlertState_FIRING,
					ActiveAt:    &twoHAgo,
					Value:       "1",
					// This is unlikely if groups is warn, but test nevertheless.
					PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
				},
				{
					Labels:      rulespb.PromLabels{Labels: []storepb.Label{{Name: "inside", Value: "3"}}},
					Annotations: rulespb.PromLabels{Labels: []storepb.Label{{Name: "insideann", Value: "4"}}},
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
			Labels:                    rulespb.PromLabels{Labels: []storepb.Label{{Name: "some", Value: "label4"}}},
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
			Labels:         storepb.LabelsToPromLabels(all[0].GetRecording().Labels.Labels),
			Health:         rules.RuleHealth(all[0].GetRecording().Health),
			LastError:      all[0].GetRecording().LastError,
			LastEvaluation: all[0].GetRecording().LastEvaluation,
			EvaluationTime: all[0].GetRecording().EvaluationDurationSeconds,
			Type:           "recording",
		},
		testpromcompatibility.RecordingRule{
			Name:           all[1].GetRecording().Name,
			Query:          all[1].GetRecording().Query,
			Labels:         storepb.LabelsToPromLabels(all[1].GetRecording().Labels.Labels),
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
			Labels:         storepb.LabelsToPromLabels(all[2].GetAlert().Labels.Labels),
			Health:         rules.RuleHealth(all[2].GetAlert().Health),
			LastError:      all[2].GetAlert().LastError,
			LastEvaluation: all[2].GetAlert().LastEvaluation,
			EvaluationTime: all[2].GetAlert().EvaluationDurationSeconds,
			Duration:       all[2].GetAlert().DurationSeconds,
			Annotations:    storepb.LabelsToPromLabels(all[2].GetAlert().Annotations.Labels),
			Alerts: []*testpromcompatibility.Alert{
				{
					Labels:                  storepb.LabelsToPromLabels(all[2].GetAlert().Alerts[0].Labels.Labels),
					Annotations:             storepb.LabelsToPromLabels(all[2].GetAlert().Alerts[0].Annotations.Labels),
					State:                   strings.ToLower(all[2].GetAlert().Alerts[0].State.String()),
					ActiveAt:                all[2].GetAlert().Alerts[0].ActiveAt,
					Value:                   all[2].GetAlert().Alerts[0].Value,
					PartialResponseStrategy: all[2].GetAlert().Alerts[0].PartialResponseStrategy.String(),
				},
				{
					Labels:                  storepb.LabelsToPromLabels(all[2].GetAlert().Alerts[1].Labels.Labels),
					Annotations:             storepb.LabelsToPromLabels(all[2].GetAlert().Alerts[1].Annotations.Labels),
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
			Labels:         storepb.LabelsToPromLabels(all[3].GetAlert().Labels.Labels),
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
	var tests = []test{
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
	}

	for _, test := range tests {
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
			res, errors, apiError := endpoint(req.WithContext(ctx))
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
