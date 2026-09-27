// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/extpromql"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type testExemplarClient struct {
	grpc.ClientStream
	exemplarErr, recvErr error
	response             *exemplarspb.ExemplarsResponse
	sentResponse         atomic.Bool
}

func (t *testExemplarClient) String() string {
	return "test"
}

func (t *testExemplarClient) Recv() (*exemplarspb.ExemplarsResponse, error) {
	// A simulation of underlying grpc Recv behavior as per https://github.com/grpc/grpc-go/blob/7f2581f910fc21497091c4109b56d310276fc943/stream.go#L117-L125.
	if t.recvErr != nil {
		return nil, t.recvErr
	}

	if t.sentResponse.Load() {
		return nil, io.EOF
	}
	t.sentResponse.Store(true)

	return t.response, nil
}

func (t *testExemplarClient) Exemplars(ctx context.Context, in *exemplarspb.ExemplarsRequest, opts ...grpc.CallOption) (exemplarspb.Exemplars_ExemplarsClient, error) {
	expr, err := extpromql.ParseExpr(in.Query)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := t.assertUniqueMatchers(expr); err != nil {
		return nil, err
	}

	return t, t.exemplarErr
}

func (t *testExemplarClient) assertUniqueMatchers(expr parser.Expr) error {
	matchersList := parser.ExtractSelectors(expr)
	for _, matchers := range matchersList {
		matcherSet := make(map[string]struct{})
		for _, matcher := range matchers {
			if _, ok := matcherSet[matcher.String()]; ok {
				return status.Error(codes.Internal, fmt.Sprintf("duplicate matcher set found %s", matcher))
			}
			matcherSet[matcher.String()] = struct{}{}
		}
	}

	return nil
}

var _ exemplarspb.ExemplarsClient = &testExemplarClient{}

type testExemplarServer struct {
	grpc.ServerStream
	sendErr   error
	responses []*exemplarspb.ExemplarsResponse
	mu        sync.Mutex
}

func (t *testExemplarServer) String() string {
	return "test"
}

func (t *testExemplarServer) Send(response *exemplarspb.ExemplarsResponse) error {
	if t.sendErr != nil {
		return t.sendErr
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.responses = append(t.responses, response)
	return nil
}

func (t *testExemplarServer) Context() context.Context {
	return context.Background()
}

func TestProxy(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)

	for _, tc := range []struct {
		name           string
		request        *exemplarspb.ExemplarsRequest
		clients        []*exemplarspb.ExemplarStore
		server         *testExemplarServer
		selectorLabels labels.Labels
		wantResponses  []*exemplarspb.ExemplarsResponse
		wantError      error
	}{
		{
			name: "proxy success",
			request: &exemplarspb.ExemplarsRequest{
				Query:                   `http_request_duration_bucket`,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			clients: []*exemplarspb.ExemplarStore{
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
						}),
					},
					LabelSets: []labels.Labels{
						labels.FromMap(map[string]string{"cluster": "A"}),
						labels.FromMap(map[string]string{"cluster": "B"}),
					},
				},
			},
			server: &testExemplarServer{},
			wantResponses: []*exemplarspb.ExemplarsResponse{
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
					Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
				}),
			},
		},
		{
			name: "proxy success with multiple selectors",
			request: &exemplarspb.ExemplarsRequest{
				Query:                   `http_request_duration_bucket{region="us-east1"} / on (region) group_left() http_request_duration_bucket`,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			clients: []*exemplarspb.ExemplarStore{
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
						}),
					},
					LabelSets: []labels.Labels{
						labels.FromMap(map[string]string{"cluster": "A"}),
						labels.FromMap(map[string]string{"cluster": "B"}),
					},
				},
			},
			server: &testExemplarServer{},
			wantResponses: []*exemplarspb.ExemplarsResponse{
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
					Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
				}),
			},
		},
		{
			name: "warning proxy success",
			request: &exemplarspb.ExemplarsRequest{
				Query:                   `http_request_duration_bucket`,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			clients: []*exemplarspb.ExemplarStore{
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewWarningExemplarsResponse(errors.New("warning from client")),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
				},
			},
			server: &testExemplarServer{},
			wantResponses: []*exemplarspb.ExemplarsResponse{
				exemplarspb.NewWarningExemplarsResponse(errors.New("warning from client")),
			},
		},
		{
			// The input query external label doesn't match with the current querier, return null.
			name: "external label doesn't match selector labels",
			request: &exemplarspb.ExemplarsRequest{
				Query:                   `http_request_duration_bucket{query="foo"}`,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			clients: []*exemplarspb.ExemplarStore{
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
				},
			},
			selectorLabels: labels.FromMap(map[string]string{"query": "bar"}),
			server:         &testExemplarServer{},
			wantResponses:  nil,
		},
		{
			// The input query external label matches with the current querier.
			name: "external label matches selector labels",
			request: &exemplarspb.ExemplarsRequest{
				Query:                   `http_request_duration_bucket{query="foo"}`,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			clients: []*exemplarspb.ExemplarStore{
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
				},
			},
			selectorLabels: labels.FromMap(map[string]string{"query": "foo"}),
			server:         &testExemplarServer{},
			wantResponses: []*exemplarspb.ExemplarsResponse{
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
					Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
				}),
			},
		},
		{
			name: "one external label matches one of the selector labels",
			request: &exemplarspb.ExemplarsRequest{
				Query:                   `http_request_duration_bucket{cluster="A"}`,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			clients: []*exemplarspb.ExemplarStore{
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "non-matching"}), labels.FromMap(map[string]string{"cluster": "A"})},
				},
			},
			server: &testExemplarServer{},
			wantResponses: []*exemplarspb.ExemplarsResponse{
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
					Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
				}),
			},
		},
		{
			name: "external label selects stores",
			request: &exemplarspb.ExemplarsRequest{
				Query:                   `http_request_duration_bucket{cluster="A"}`,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			clients: []*exemplarspb.ExemplarStore{
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "bar"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
				},
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "baz"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 2}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "B"})},
				},
			},
			server: &testExemplarServer{},
			wantResponses: []*exemplarspb.ExemplarsResponse{
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "bar"}))},
					Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
				}),
			},
		},
		{
			name: "external label matches different stores",
			request: &exemplarspb.ExemplarsRequest{
				Query:                   `http_request_duration_bucket{cluster="A"} + http_request_duration_bucket{cluster="B"}`,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			clients: []*exemplarspb.ExemplarStore{
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "bar"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
				},
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "baz"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 2}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "B"})},
				},
			},
			server: &testExemplarServer{},
			wantResponses: []*exemplarspb.ExemplarsResponse{
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "bar"}))},
					Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
				}),
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "baz"}))},
					Exemplars:    []*exemplarspb.Exemplar{{Value: 2}},
				}),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewProxy(logger, func() []*exemplarspb.ExemplarStore {
				return tc.clients
			}, tc.selectorLabels)

			err := p.Exemplars(tc.request, tc.server)
			gotErr := "<nil>"
			if err != nil {
				gotErr = err.Error()
			}
			wantErr := "<nil>"
			if tc.wantError != nil {
				wantErr = tc.wantError.Error()
			}

			if gotErr != wantErr {
				t.Errorf("want error %q, got %q", wantErr, gotErr)
			}

			testutil.Equals(t, len(tc.wantResponses), len(tc.server.responses))

			// Actual responses are unordered so we search
			// for matched response for simplicity.
		Outer:
			for _, exp := range tc.wantResponses {
				var lres *exemplarspb.ExemplarsResponse
				for _, res := range tc.server.responses {
					if reflect.DeepEqual(exp, res) {
						continue Outer
					}
				}
				t.Errorf("miss expected response %v. got: %v", exp, lres)
			}
		})
	}
}

// testExemplarClientWithQueryCapture extends testExemplarClient to capture the forwarded query.
type testExemplarClientWithQueryCapture struct {
	testExemplarClient
	capturedQuery atomic.String
}

func (t *testExemplarClientWithQueryCapture) Exemplars(ctx context.Context, in *exemplarspb.ExemplarsRequest, opts ...grpc.CallOption) (exemplarspb.Exemplars_ExemplarsClient, error) {
	t.capturedQuery.Store(in.Query)
	return t.testExemplarClient.Exemplars(ctx, in, opts...)
}

var _ exemplarspb.ExemplarsClient = &testExemplarClientWithQueryCapture{}

func TestProxyExternalLabelsPreservedForQueryStores(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)

	// Simulate a multi-tier topology: Query A → [Query B (cluster=A), Sidecar (cluster=B)]
	// When querying with {cluster="A"}, Query B should receive cluster="A" in its query
	// (SupportsExternalLabels=true), while a Sidecar would have it stripped.

	queryClient := &testExemplarClientWithQueryCapture{
		testExemplarClient: testExemplarClient{
			response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
				SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
				Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
			}),
		},
	}

	sidecarClient := &testExemplarClientWithQueryCapture{
		testExemplarClient: testExemplarClient{
			response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
				SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
				Exemplars:    []*exemplarspb.Exemplar{{Value: 2}},
			}),
		},
	}

	clients := []*exemplarspb.ExemplarStore{
		{
			ExemplarsClient:        queryClient,
			LabelSets:              []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
			SupportsExternalLabels: true, // This is a Query node.
		},
		{
			ExemplarsClient:        sidecarClient,
			LabelSets:              []labels.Labels{labels.FromMap(map[string]string{"cluster": "B"})},
			SupportsExternalLabels: false, // This is a Sidecar.
		},
	}

	p := NewProxy(logger, func() []*exemplarspb.ExemplarStore {
		return clients
	}, labels.EmptyLabels())

	server := &testExemplarServer{}

	// Query with cluster="A" and namespace="foo".
	err := p.Exemplars(&exemplarspb.ExemplarsRequest{
		Query:                   `http_request_duration_bucket{cluster="A", namespace="foo"}`,
		PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
	}, server)
	testutil.Ok(t, err)

	// Only the Query store (cluster=A) should have been queried.
	testutil.Equals(t, 1, len(server.responses))

	// Verify the Query node received the full matchers including external label.
	queryForwarded := queryClient.capturedQuery.Load()
	testutil.Assert(t, queryForwarded != "", "query store should have been called")

	// The forwarded query to the Query node must contain cluster="A"
	// because it needs it for its own downstream routing.
	expr, err := extpromql.ParseExpr(queryForwarded)
	testutil.Ok(t, err)
	selectors := parser.ExtractSelectors(expr)
	testutil.Assert(t, len(selectors) > 0, "expected at least one selector")

	foundCluster := false
	foundNamespace := false
	for _, matcherSet := range selectors {
		for _, m := range matcherSet {
			if m.Name == "cluster" && m.Value == "A" {
				foundCluster = true
			}
			if m.Name == "namespace" && m.Value == "foo" {
				foundNamespace = true
			}
		}
	}
	testutil.Assert(t, foundCluster, "query store should receive cluster matcher for downstream routing")
	testutil.Assert(t, foundNamespace, "query store should receive namespace matcher")

	// Verify the Sidecar was NOT queried (cluster=B doesn't match cluster="A").
	sidecarForwarded := sidecarClient.capturedQuery.Load()
	testutil.Assert(t, sidecarForwarded == "", "sidecar with cluster=B should not be queried for cluster=A request")
}

func TestProxyExternalLabelsStrippedForSidecarStores(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)

	// Verify that the existing behavior of stripping external labels for Sidecars is preserved.
	sidecarClient := &testExemplarClientWithQueryCapture{
		testExemplarClient: testExemplarClient{
			response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
				SeriesLabels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
				Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
			}),
		},
	}

	clients := []*exemplarspb.ExemplarStore{
		{
			ExemplarsClient:        sidecarClient,
			LabelSets:              []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
			SupportsExternalLabels: false, // Sidecar.
		},
	}

	p := NewProxy(logger, func() []*exemplarspb.ExemplarStore {
		return clients
	}, labels.EmptyLabels())

	server := &testExemplarServer{}

	err := p.Exemplars(&exemplarspb.ExemplarsRequest{
		Query:                   `http_request_duration_bucket{cluster="A", namespace="foo"}`,
		PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
	}, server)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(server.responses))

	// Verify the Sidecar received the query WITHOUT the external label matcher.
	sidecarForwarded := sidecarClient.capturedQuery.Load()
	testutil.Assert(t, sidecarForwarded != "", "sidecar should have been called")

	expr, err := extpromql.ParseExpr(sidecarForwarded)
	testutil.Ok(t, err)
	selectors := parser.ExtractSelectors(expr)
	testutil.Assert(t, len(selectors) > 0, "expected at least one selector")

	for _, matcherSet := range selectors {
		for _, m := range matcherSet {
			testutil.Assert(t, m.Name != "cluster",
				"sidecar should NOT receive external label matcher cluster, but got: %s", m.String())
		}
	}
}

// TestProxyDataRace find the concurrent data race bug ( go test -race -run TestProxyDataRace -v ).
func TestProxyDataRace(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)
	p := NewProxy(logger, func() []*exemplarspb.ExemplarStore {
		es := &exemplarspb.ExemplarStore{
			ExemplarsClient: &testExemplarClient{
				recvErr: errors.New("err"),
			},
			LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
		}
		size := 100
		endpoints := make([]*exemplarspb.ExemplarStore, 0, size)
		for range size {
			endpoints = append(endpoints, es)
		}
		return endpoints
	}, labels.FromMap(map[string]string{"query": "foo"}))
	req := &exemplarspb.ExemplarsRequest{
		Query:                   `http_request_duration_bucket{query="foo"}`,
		PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
	}
	s := &exemplarsServer{
		ctx: context.Background(),
	}
	_ = p.Exemplars(req, s)
}
