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
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/extpromql"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func TestRemoteEngine_Warnings(t *testing.T) {
	client := NewClient(&warnClient{}, "", nil)
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

	plan := logicalplan.NewFromAST(qryExpr, &query.Options{
		Start: time.Now(),
		End:   time.Now().Add(2 * time.Hour),
	}, logicalplan.PlanOptions{})

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
	tests := []struct {
		name            string
		tsdbInfos       []infopb.TSDBInfo
		replicaLabels   []string
		expected        []labels.Labels
		partitionLabels []string
	}{
		{
			name:      "empty label sets",
			tsdbInfos: []infopb.TSDBInfo{},
			expected:  []labels.Labels{},
		},
		{
			name:          "empty label sets with replica labels",
			tsdbInfos:     []infopb.TSDBInfo{},
			replicaLabels: []string{"replica"},
			expected:      []labels.Labels{},
		},
		{
			name: "non-empty label sets",
			tsdbInfos: []infopb.TSDBInfo{{
				Labels: zLabelSetFromStrings("a", "1"),
			}},
			expected: []labels.Labels{labels.FromStrings("a", "1")},
		},
		{
			name: "non-empty label sets with replica labels",
			tsdbInfos: []infopb.TSDBInfo{{
				Labels: zLabelSetFromStrings("a", "1", "b", "2"),
			}},
			replicaLabels: []string{"a"},
			expected:      []labels.Labels{labels.FromStrings("b", "2")},
		},
		{
			name: "replica labels not in label sets",
			tsdbInfos: []infopb.TSDBInfo{
				{
					Labels: zLabelSetFromStrings("a", "1", "c", "2"),
				},
			},
			replicaLabels: []string{"a", "b"},
			expected:      []labels.Labels{labels.FromStrings("c", "2")},
		},
		{
			name: "non-empty label sets with partition labels",
			tsdbInfos: []infopb.TSDBInfo{
				{
					Labels: zLabelSetFromStrings("a", "1", "c", "2"),
				},
			},
			partitionLabels: []string{"a"},
			expected:        []labels.Labels{labels.FromStrings("a", "1")},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			client := NewClient(nil, "", testCase.tsdbInfos)
			engine := NewRemoteEngine(log.NewNopLogger(), client, Opts{
				ReplicaLabels:   testCase.replicaLabels,
				PartitionLabels: testCase.partitionLabels,
			})

			testutil.Equals(t, testCase.expected, engine.LabelSets())
		})
	}
}

func TestRemoteEngine_MinT(t *testing.T) {
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
}

func (m warnClient) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.Query_QueryClient, error) {
	return &queryWarnClient{}, nil
}

func (m warnClient) QueryRange(ctx context.Context, in *querypb.QueryRangeRequest, opts ...grpc.CallOption) (querypb.Query_QueryRangeClient, error) {
	return &queryRangeWarnClient{}, nil
}

type queryRangeWarnClient struct {
	querypb.Query_QueryRangeClient
	warnSent bool
}

func (m *queryRangeWarnClient) Recv() (*querypb.QueryRangeResponse, error) {
	if m.warnSent {
		return nil, io.EOF
	}
	m.warnSent = true
	return querypb.NewQueryRangeWarningsResponse(errors.New("warning")), nil
}

type queryWarnClient struct {
	querypb.Query_QueryClient
	warnSent bool
}

func (m *queryWarnClient) Recv() (*querypb.QueryResponse, error) {
	if m.warnSent {
		return nil, io.EOF
	}
	m.warnSent = true
	return querypb.NewQueryWarningsResponse(errors.New("warning")), nil
}
