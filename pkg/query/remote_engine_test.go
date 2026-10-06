// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"io"
	"math"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"
	"google.golang.org/grpc"

	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/extannotations"
	"github.com/thanos-io/thanos/pkg/extpromql"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func TestRemoteEngine_Warnings(t *testing.T) {
	t.Parallel()

	client := NewClient(&warnClient{}, "testclient", nil)
	engine := NewRemoteEngine(log.NewNopLogger(), client, Opts{
		Timeout: 1 * time.Second,
	})
	var (
		start = time.Unix(0, 0)
		end   = time.Unix(120, 0)
		step  = 30 * time.Second
	)
	qryExpr, err := extpromql.ParseExpr("up")
	testutil.Ok(t, err)

	plan, err := logicalplan.NewFromAST(qryExpr, &query.Options{
		Start: time.Now(),
		End:   time.Now().Add(2 * time.Hour),
	}, logicalplan.PlanOptions{})
	testutil.Ok(t, err)

	t.Run("instant_query", func(t *testing.T) {
		qry, err := engine.NewInstantQuery(context.Background(), nil, plan.Root(), start)
		testutil.Ok(t, err)
		res := qry.Exec(context.Background())
		testutil.Ok(t, res.Err)
		testutil.Equals(t, 1, len(res.Warnings))
	})

	t.Run("range_query", func(t *testing.T) {
		qry, err := engine.NewRangeQuery(context.Background(), nil, plan.Root(), start, end, step)
		testutil.Ok(t, err)
		res := qry.Exec(context.Background())
		testutil.Ok(t, res.Err)
		testutil.Equals(t, 1, len(res.Warnings))
	})
}

// Regression test for https://github.com/thanos-io/thanos/issues/9062.
func TestRemoteEngine_PromQLAnnotations(t *testing.T) {
	t.Parallel()

	var (
		start = time.Unix(0, 0)
		end   = time.Unix(120, 0)
		step  = 30 * time.Second
	)
	qryExpr, err := extpromql.ParseExpr("up")
	testutil.Ok(t, err)

	plan, err := logicalplan.NewFromAST(qryExpr, &query.Options{
		Start: time.Now(),
		End:   time.Now().Add(2 * time.Hour),
	}, logicalplan.PlanOptions{})
	testutil.Ok(t, err)

	for _, tc := range []struct {
		name       string
		warning    string
		annotation error
	}{
		{
			name:       "info",
			warning:    `PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "some_gauge"`,
			annotation: annotations.PromQLInfo,
		},
		{
			name:       "warning",
			warning:    `PromQL warning: encountered a mix of histograms and floats for metric name "some_metric"`,
			annotation: annotations.PromQLWarning,
		},
		{
			name:    "store warning",
			warning: "fetch series: store unavailable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := NewClient(&warnClient{warning: tc.warning}, "testclient", nil)
			engine := NewRemoteEngine(log.NewNopLogger(), client, Opts{
				Timeout: 1 * time.Second,
			})

			check := func(t *testing.T, res *promql.Result) {
				testutil.Ok(t, res.Err)
				warns := res.Warnings.AsErrors()
				testutil.Equals(t, 1, len(warns))

				if tc.annotation == nil {
					testutil.Equals(t, "remote query warning (testclient): "+tc.warning, warns[0].Error())
					testutil.Assert(t, !extannotations.IsPromQLAnnotation(warns[0].Error()), "store warning recognized as a PromQL annotation")
					return
				}
				testutil.Equals(t, tc.warning, warns[0].Error())
				testutil.Assert(t, errors.Is(warns[0], tc.annotation), "expected %v to wrap %v", warns[0], tc.annotation)
				testutil.Assert(t, extannotations.IsPromQLAnnotation(warns[0].Error()), "PromQL annotation not recognized: %v", warns[0])
			}

			t.Run("instant_query", func(t *testing.T) {
				qry, err := engine.NewInstantQuery(context.Background(), nil, plan.Root(), start)
				testutil.Ok(t, err)
				check(t, qry.Exec(context.Background()))
			})

			t.Run("range_query", func(t *testing.T) {
				qry, err := engine.NewRangeQuery(context.Background(), nil, plan.Root(), start, end, step)
				testutil.Ok(t, err)
				check(t, qry.Exec(context.Background()))
			})
		})
	}
}

func TestRemoteEngine_PartialResponse(t *testing.T) {
	t.Parallel()

	client := NewClient(&errClient{}, "testclient", nil)
	engine := NewRemoteEngine(log.NewNopLogger(), client, Opts{
		Timeout:         1 * time.Second,
		PartialResponse: true,
	})
	var (
		start = time.Unix(0, 0)
		end   = time.Unix(120, 0)
		step  = 30 * time.Second
	)
	qryExpr, err := extpromql.ParseExpr("up")
	testutil.Ok(t, err)

	plan, err := logicalplan.NewFromAST(qryExpr, &query.Options{
		Start: time.Now(),
		End:   time.Now().Add(2 * time.Hour),
	}, logicalplan.PlanOptions{})
	testutil.Ok(t, err)

	t.Run("instant_query", func(t *testing.T) {
		qry, err := engine.NewInstantQuery(context.Background(), nil, plan.Root(), start)
		testutil.Ok(t, err)
		res := qry.Exec(context.Background())
		testutil.Ok(t, res.Err)
		testutil.Equals(t, 1, len(res.Warnings))
	})

	t.Run("range_query", func(t *testing.T) {
		qry, err := engine.NewRangeQuery(context.Background(), nil, plan.Root(), start, end, step)
		testutil.Ok(t, err)
		res := qry.Exec(context.Background())
		testutil.Ok(t, res.Err)
		testutil.Equals(t, 1, len(res.Warnings))
	})
}

func TestRemoteEngine_LabelSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                       string
		tsdbInfos                  []infopb.TSDBInfo
		replicaLabels              []string
		partitionLabels            []string
		expectedLabelSets          []labels.Labels
		expectedPartitionLabelSets []labels.Labels
	}{
		{
			name:                       "empty label sets",
			tsdbInfos:                  []infopb.TSDBInfo{},
			expectedLabelSets:          []labels.Labels{},
			expectedPartitionLabelSets: []labels.Labels{},
		},
		{
			name:                       "empty label sets with replica labels",
			tsdbInfos:                  []infopb.TSDBInfo{},
			replicaLabels:              []string{"replica"},
			expectedLabelSets:          []labels.Labels{},
			expectedPartitionLabelSets: []labels.Labels{},
		},
		{
			name: "non-empty label sets",
			tsdbInfos: []infopb.TSDBInfo{{
				Labels: zLabelSetFromStrings("a", "1"),
			}},
			expectedLabelSets:          []labels.Labels{labels.FromStrings("a", "1")},
			expectedPartitionLabelSets: []labels.Labels{labels.FromStrings("a", "1")},
		},
		{
			name: "non-empty label sets with replica labels",
			tsdbInfos: []infopb.TSDBInfo{{
				Labels: zLabelSetFromStrings("a", "1", "b", "2"),
			}},
			replicaLabels:              []string{"a"},
			expectedLabelSets:          []labels.Labels{labels.FromStrings("b", "2")},
			expectedPartitionLabelSets: []labels.Labels{labels.FromStrings("b", "2")},
		},
		{
			name: "replica labels not in label sets",
			tsdbInfos: []infopb.TSDBInfo{
				{
					Labels: zLabelSetFromStrings("a", "1", "c", "2"),
				},
			},
			replicaLabels:              []string{"a", "b"},
			expectedLabelSets:          []labels.Labels{labels.FromStrings("c", "2")},
			expectedPartitionLabelSets: []labels.Labels{labels.FromStrings("c", "2")},
		},
		{
			name: "non-empty label sets with partition labels",
			tsdbInfos: []infopb.TSDBInfo{
				{
					Labels: zLabelSetFromStrings("a", "1", "c", "2"),
				},
			},
			partitionLabels:            []string{"a"},
			expectedLabelSets:          []labels.Labels{labels.FromStrings("a", "1", "c", "2")},
			expectedPartitionLabelSets: []labels.Labels{labels.FromStrings("a", "1")},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			client := NewClient(nil, "", testCase.tsdbInfos)
			engine := NewRemoteEngine(log.NewNopLogger(), client, Opts{
				ReplicaLabels:   testCase.replicaLabels,
				PartitionLabels: testCase.partitionLabels,
			})

			testutil.Equals(t, testCase.expectedPartitionLabelSets, engine.PartitionLabelSets())
			testutil.Equals(t, testCase.expectedLabelSets, engine.LabelSets())
		})
	}
}

func TestRemoteEngine_MinT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		tsdbInfos     []infopb.TSDBInfo
		replicaLabels []string
		expected      int64
	}{
		{
			name:      "empty label sets",
			tsdbInfos: []infopb.TSDBInfo{},
			expected:  math.MaxInt64,
		},
		{
			name:          "empty label sets with replica labels",
			tsdbInfos:     []infopb.TSDBInfo{},
			replicaLabels: []string{"replica"},
			expected:      math.MaxInt64,
		},
		{
			name: "non-empty label sets",
			tsdbInfos: []infopb.TSDBInfo{{
				Labels:  zLabelSetFromStrings("a", "1"),
				MinTime: 30,
			}},
			expected: 30,
		},
		{
			name: "non-empty label sets with replica labels",
			tsdbInfos: []infopb.TSDBInfo{{
				Labels:  zLabelSetFromStrings("a", "1", "b", "2"),
				MinTime: 30,
			}},
			replicaLabels: []string{"a"},
			expected:      30,
		},
		{
			name: "replicated labelsets with different mint",
			tsdbInfos: []infopb.TSDBInfo{
				{
					Labels:  zLabelSetFromStrings("a", "1", "replica", "1"),
					MinTime: 30,
				},
				{
					Labels:  zLabelSetFromStrings("a", "1", "replica", "2"),
					MinTime: 60,
				},
			},
			replicaLabels: []string{"replica"},
			expected:      60,
		},
		{
			name: "multiple replicated labelsets with different mint",
			tsdbInfos: []infopb.TSDBInfo{
				{
					Labels:  zLabelSetFromStrings("a", "1", "replica", "1"),
					MinTime: 30,
				},
				{
					Labels:  zLabelSetFromStrings("a", "1", "replica", "2"),
					MinTime: 60,
				},
				{
					Labels:  zLabelSetFromStrings("a", "2", "replica", "1"),
					MinTime: 80,
				},
				{
					Labels:  zLabelSetFromStrings("a", "2", "replica", "2"),
					MinTime: 120,
				},
			},
			replicaLabels: []string{"replica"},
			expected:      60,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			client := NewClient(nil, "", testCase.tsdbInfos)
			engine := NewRemoteEngine(log.NewNopLogger(), client, Opts{
				ReplicaLabels: testCase.replicaLabels,
			})

			testutil.Equals(t, testCase.expected, engine.MinT())
		})
	}
}

func zLabelSetFromStrings(ss ...string) labelpb.ZLabelSet {
	return labelpb.ZLabelSet{
		Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings(ss...)),
	}
}

type warnClient struct {
	querypb.QueryClient
	// warning is the warning sent by the remote engine; "warning" if empty.
	warning string
}

func (m warnClient) warn() error {
	if m.warning == "" {
		return errors.New("warning")
	}
	return errors.New(m.warning)
}

func (m warnClient) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.Query_QueryClient, error) {
	return &queryWarnClient{warning: m.warn()}, nil
}

func (m warnClient) QueryRange(ctx context.Context, in *querypb.QueryRangeRequest, opts ...grpc.CallOption) (querypb.Query_QueryRangeClient, error) {
	return &queryRangeWarnClient{warning: m.warn()}, nil
}

type queryRangeWarnClient struct {
	querypb.Query_QueryRangeClient
	warning  error
	warnSent bool
}

func (m *queryRangeWarnClient) Recv() (*querypb.QueryRangeResponse, error) {
	if m.warnSent {
		return nil, io.EOF
	}
	m.warnSent = true
	return querypb.NewQueryRangeWarningsResponse(m.warning), nil
}

type queryWarnClient struct {
	querypb.Query_QueryClient
	warning  error
	warnSent bool
}

func (m *queryWarnClient) Recv() (*querypb.QueryResponse, error) {
	if m.warnSent {
		return nil, io.EOF
	}
	m.warnSent = true
	return querypb.NewQueryWarningsResponse(m.warning), nil
}

type errClient struct {
	querypb.QueryClient
}

func (m errClient) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.Query_QueryClient, error) {
	return &queryErrClient{}, nil
}

func (m errClient) QueryRange(ctx context.Context, in *querypb.QueryRangeRequest, opts ...grpc.CallOption) (querypb.Query_QueryRangeClient, error) {
	return &queryRangeErrClient{}, nil
}

type queryRangeErrClient struct {
	querypb.Query_QueryRangeClient
	errSent bool
}

func (m *queryRangeErrClient) Recv() (*querypb.QueryRangeResponse, error) {
	if m.errSent {
		return nil, io.EOF
	}
	m.errSent = true
	return nil, errors.New("error")
}

type queryErrClient struct {
	querypb.Query_QueryClient
	errSent bool
}

func (m *queryErrClient) Recv() (*querypb.QueryResponse, error) {
	if m.errSent {
		return nil, io.EOF
	}
	m.errSent = true
	return nil, errors.New("error")
}
