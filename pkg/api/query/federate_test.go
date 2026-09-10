// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	baseAPI "github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestFederateEndpoints(t *testing.T) {
	now := time.Unix(600, 0)

	db, err := e2eutil.NewTSDB()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, db.Close()) }()

	series := []struct {
		lset labels.Labels
		t    int64
		v    float64
	}{
		{lset: labels.FromStrings("__name__", "test_metric1", "foo", "bar"), t: 540000, v: 2},
		{lset: labels.FromStrings("__name__", "test_metric1", "foo", "boo"), t: 540000, v: 3},
		{lset: labels.FromStrings("__name__", "test_metric2", "foo", "boo"), t: 540000, v: 4},
		// Last sample outside the lookback window.
		{lset: labels.FromStrings("__name__", "test_metric_old", "foo", "bar"), t: 100000, v: 5},
		// Latest sample is a stale marker.
		{lset: labels.FromStrings("__name__", "test_metric_stale", "foo", "bar"), t: 480000, v: 1},
		{lset: labels.FromStrings("__name__", "test_metric_stale", "foo", "bar"), t: 540000, v: math.Float64frombits(value.StaleNaN)},
		// Same series scraped via two replicas.
		{lset: labels.FromStrings("__name__", "test_metric_replica1", "foo", "boo", "replica", "a"), t: 540000, v: 6},
		{lset: labels.FromStrings("__name__", "test_metric_replica1", "foo", "boo", "replica", "b"), t: 540000, v: 6},
		{lset: labels.FromStrings("__name__", "test_tenant_metric", "tenant_id", "tenant-a"), t: 540000, v: 7},
		{lset: labels.FromStrings("__name__", "test_tenant_metric", "tenant_id", "tenant-b"), t: 540000, v: 8},
		// No __name__ label.
		{lset: labels.FromStrings("nameless", "true"), t: 540000, v: 9},
	}

	app := db.Appender(context.Background())
	for _, s := range series {
		_, err := app.Append(0, s.lset, s.t, s.v)
		testutil.Ok(t, err)
	}
	_, err = app.AppendHistogram(
		0,
		labels.FromStrings("__name__", "test_histogram_native", "foo", "bar"),
		540000,
		nil,
		tsdbutil.GenerateTestFloatHistogram(0),
	)
	testutil.Ok(t, err)
	_, err = app.AppendHistogram(
		0,
		labels.FromStrings("__name__", "test_histogram_nhcb", "foo", "bar"),
		540000,
		nil,
		tsdbutil.GenerateTestCustomBucketsFloatHistogram(0),
	)
	testutil.Ok(t, err)
	testutil.Ok(t, app.Commit())

	newAPI := func(enforceTenancy bool) *QueryAPI {
		return &QueryAPI{
			baseAPI: &baseAPI.BaseAPI{
				Now: func() time.Time { return now },
			},
			logger:              log.NewNopLogger(),
			queryableCreate:     query.NewQueryableCreator(nil, nil, newProxyStoreWithTSDBStore(db), 2, timeout, dedup.AlgorithmPenalty, 1),
			lookbackDeltaCreate: func(int64) time.Duration { return 5 * time.Minute },
			replicaLabels:       []string{"replica"},
			gate:                gate.New(nil, 4, gate.Queries),
			tenantHeader:        "thanos-tenant",
			defaultTenant:       "default-tenant",
			enforceTenancy:      enforceTenancy,
			tenantLabel:         "tenant_id",
		}
	}

	limitedAPI := newAPI(false)
	limitedAPI.federateMaxSeries = 1

	atLimitAPI := newAPI(false)
	atLimitAPI.federateMaxSeries = 2

	occupiedGate := gate.New(nil, 1, gate.Queries)
	testutil.Ok(t, occupiedGate.Start(context.Background()))
	t.Cleanup(occupiedGate.Done)
	gatedAPI := newAPI(false)
	gatedAPI.gate = occupiedGate
	gateCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)

	zeroLookbackAPI := newAPI(false)
	zeroLookbackAPI.lookbackDeltaCreate = func(int64) time.Duration { return 0 }

	partialAPI := newAPI(false)
	partialAPI.enableQueryPartialResponse = true
	partialAPI.queryableCreate = query.NewQueryableCreator(nil, nil, newFailingProxyStore(), 2, timeout, dedup.AlgorithmPenalty, 1)

	textContentType := "text/plain; version=0.0.4; charset=utf-8; escaping=underscores"

	for _, tc := range []struct {
		name                string
		api                 *QueryAPI
		match               []string
		accept              string
		tenantHeaderValue   string
		ctx                 context.Context
		expectedCode        int
		expectedContentType string
		expectedBody        string
		expectedFamilies    []*dto.MetricFamily
	}{
		{
			name:                "no match[] parameter returns an empty exposition",
			api:                 newAPI(false),
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody:        "",
		},
		{
			name:                "unparsable match[] parameter is rejected",
			api:                 newAPI(false),
			match:               []string{"{invalid"},
			expectedCode:        http.StatusBadRequest,
			expectedContentType: "text/plain; charset=utf-8",
			expectedBody:        "1:9: parse error: unexpected end of input inside braces\n",
		},
		{
			name:                "single matcher returns only matching series sorted by name",
			api:                 newAPI(false),
			match:               []string{"test_metric1"},
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody: `# TYPE test_metric1 untyped
test_metric1{foo="bar"} 2 540000
test_metric1{foo="boo"} 3 540000
`,
		},
		{
			name:                "multiple match[] parameters return the union of series",
			api:                 newAPI(false),
			match:               []string{"test_metric1", "test_metric2"},
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody: `# TYPE test_metric1 untyped
test_metric1{foo="bar"} 2 540000
test_metric1{foo="boo"} 3 540000
# TYPE test_metric2 untyped
test_metric2{foo="boo"} 4 540000
`,
		},
		{
			name:                "series without a sample inside the lookback window is omitted",
			api:                 newAPI(false),
			match:               []string{"test_metric_old"},
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody:        "",
		},
		{
			name:                "zero lookback delta falls back to the default five minute window",
			api:                 zeroLookbackAPI,
			match:               []string{"test_metric1"},
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody: `# TYPE test_metric1 untyped
test_metric1{foo="bar"} 2 540000
test_metric1{foo="boo"} 3 540000
`,
		},
		{
			name:                "series whose latest sample is a stale marker is dropped",
			api:                 newAPI(false),
			match:               []string{"test_metric_stale"},
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody:        "",
		},
		{
			name:                "replicated series are deduplicated and the replica label is removed",
			api:                 newAPI(false),
			match:               []string{"test_metric_replica1"},
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody: `# TYPE test_metric_replica1 untyped
test_metric_replica1{foo="boo"} 6 540000
`,
		},
		{
			name:              "enforced tenancy returns only series of the requesting tenant",
			api:               newAPI(true),
			match:             []string{"test_tenant_metric"},
			tenantHeaderValue: "tenant-a",
			expectedCode:      http.StatusOK,
			expectedBody: `# TYPE test_tenant_metric untyped
test_tenant_metric{tenant_id="tenant-a"} 7 540000
`,
		},
		{
			name:                "series without a metric name is skipped",
			api:                 newAPI(false),
			match:               []string{`{nameless="true"}`},
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody:        "",
		},
		{
			name:                "native histogram is not served in the text exposition format",
			api:                 newAPI(false),
			match:               []string{"test_histogram_native"},
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody:        "",
		},
		{
			name:                "NHCB series is served as a classic histogram",
			api:                 newAPI(false),
			match:               []string{"test_histogram_nhcb"},
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody: `# TYPE test_histogram_nhcb histogram
test_histogram_nhcb_bucket{foo="bar",le="0"} 1 540000
test_histogram_nhcb_bucket{foo="bar",le="1"} 3 540000
test_histogram_nhcb_bucket{foo="bar",le="2"} 3 540000
test_histogram_nhcb_bucket{foo="bar",le="3"} 4 540000
test_histogram_nhcb_bucket{foo="bar",le="4"} 5 540000
test_histogram_nhcb_bucket{foo="bar",le="+Inf"} 5 540000
test_histogram_nhcb_sum{foo="bar"} 18.4 540000
test_histogram_nhcb_count{foo="bar"} 5 540000
`,
		},
		{
			// expfmt.Negotiate never returns an OpenMetrics format, same as
			// upstream Prometheus, so the plain text format is served instead.
			name:                "openmetrics accept header falls back to the text format",
			api:                 newAPI(false),
			match:               []string{"test_metric1"},
			accept:              "application/openmetrics-text",
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody: `# TYPE test_metric1 untyped
test_metric1{foo="bar"} 2 540000
test_metric1{foo="boo"} 3 540000
`,
		},
		{
			name:                "native histogram is served in the protobuf format",
			api:                 newAPI(false),
			match:               []string{"test_histogram_native"},
			accept:              "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited",
			expectedCode:        http.StatusOK,
			expectedContentType: "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited; escaping=underscores",
			expectedFamilies: []*dto.MetricFamily{
				{
					Name: proto.String("test_histogram_native"),
					Type: dto.MetricType_HISTOGRAM.Enum(),
					Metric: []*dto.Metric{
						{
							Label: []*dto.LabelPair{
								{
									Name:  proto.String("foo"),
									Value: proto.String("bar"),
								},
							},
							TimestampMs: proto.Int64(540000),
							Histogram: &dto.Histogram{
								SampleCountFloat: proto.Float64(12),
								SampleSum:        proto.Float64(18.4),
								Schema:           proto.Int32(1),
								ZeroThreshold:    proto.Float64(0.001),
								ZeroCountFloat:   proto.Float64(2),
								NegativeSpan: []*dto.BucketSpan{
									{
										Offset: proto.Int32(0),
										Length: proto.Uint32(2),
									},
									{
										Offset: proto.Int32(1),
										Length: proto.Uint32(2),
									},
								},
								NegativeCount: []float64{1, 2, 1, 1},
								PositiveSpan: []*dto.BucketSpan{
									{
										Offset: proto.Int32(0),
										Length: proto.Uint32(2),
									},
									{
										Offset: proto.Int32(1),
										Length: proto.Uint32(2),
									},
								},
								PositiveCount: []float64{1, 2, 1, 1},
							},
						},
					},
				},
			},
		},
		{
			name:                "federation matching more series than the configured limit fails",
			api:                 limitedAPI,
			match:               []string{"test_metric1"},
			expectedCode:        http.StatusInternalServerError,
			expectedContentType: "text/plain; charset=utf-8",
			expectedBody:        "federation matched more series than the configured limit of 1\n",
		},
		{
			name:                "federation matching exactly the configured limit succeeds",
			api:                 atLimitAPI,
			match:               []string{"test_metric1"},
			expectedCode:        http.StatusOK,
			expectedContentType: textContentType,
			expectedBody: `# TYPE test_metric1 untyped
test_metric1{foo="bar"} 2 540000
test_metric1{foo="boo"} 3 540000
`,
		},
		{
			name:                "request is rejected when the query concurrency gate is full",
			api:                 gatedAPI,
			match:               []string{"test_metric1"},
			ctx:                 gateCtx,
			expectedCode:        http.StatusInternalServerError,
			expectedContentType: "text/plain; charset=utf-8",
			expectedBody:        "context deadline exceeded\n",
		},
		{
			name:                "partial store response fails the scrape",
			api:                 partialAPI,
			match:               []string{"test_metric1"},
			expectedCode:        http.StatusInternalServerError,
			expectedContentType: "text/plain; charset=utf-8",
			expectedBody:        "federation select returned warnings: receive series from failing: test store error\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := url.Values{}
			for _, m := range tc.match {
				v.Add("match[]", m)
			}
			req := httptest.NewRequest(http.MethodGet, "/federate?"+v.Encode(), nil)
			if tc.ctx != nil {
				req = req.WithContext(tc.ctx)
			}
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			if tc.tenantHeaderValue != "" {
				req.Header.Set("thanos-tenant", tc.tenantHeaderValue)
			}
			rec := httptest.NewRecorder()
			tc.api.federate(rec, req)
			testutil.Equals(t, tc.expectedCode, rec.Code)
			if tc.expectedContentType != "" {
				testutil.Equals(t, tc.expectedContentType, rec.Header().Get("Content-Type"))
			}
			if tc.expectedFamilies != nil {
				// The response is binary protobuf, decode it and assert the
				// resulting structure instead of raw bytes. proto.Equal does a
				// full semantic comparison, testutil.Equals would trip over
				// protobuf internal state fields.
				var families []*dto.MetricFamily
				dec := expfmt.NewDecoder(rec.Body, expfmt.NewFormat(expfmt.TypeProtoDelim))
				for {
					var mf dto.MetricFamily
					if err := dec.Decode(&mf); err != nil {
						testutil.Equals(t, io.EOF, err)
						break
					}
					families = append(families, &mf)
				}
				testutil.Equals(t, len(tc.expectedFamilies), len(families))
				for i := range families {
					if !proto.Equal(tc.expectedFamilies[i], families[i]) {
						t.Fatalf("metric family %d does not match, expected:\n%v\ngot:\n%v", i, tc.expectedFamilies[i], families[i])
					}
				}
				return
			}
			testutil.Equals(t, tc.expectedBody, rec.Body.String())
		})
	}
}

type failingStoreServer struct {
	storepb.UnimplementedStoreServer
}

func (failingStoreServer) Series(*storepb.SeriesRequest, storepb.Store_SeriesServer) error {
	return errors.New("test store error")
}

func newFailingProxyStore() *store.ProxyStore {
	c := &storetestutil.TestClient{
		Name:        "failing",
		StoreClient: storepb.ServerAsClient(&failingStoreServer{}, atomic.Bool{}),
		MinTime:     math.MinInt64, MaxTime: math.MaxInt64,
	}

	return store.NewProxyStore(
		nil,
		nil,
		func() []store.Client { return []store.Client{c} },
		component.Query,
		labels.EmptyLabels(),
		0,
		store.EagerRetrieval,
	)
}
