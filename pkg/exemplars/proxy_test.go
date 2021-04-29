package exemplars

import (
	"context"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
)

type testExemplarClient struct {
	grpc.ClientStream
	exemplarErr, recvErr error
	response             *exemplarspb.ExemplarsResponse
	sentResponse         bool
}

func (t *testExemplarClient) String() string {
	return "test"
}

func (t *testExemplarClient) Recv() (*exemplarspb.ExemplarsResponse, error) {
	// A simulation of underlying grpc Recv behavior as per https://github.com/grpc/grpc-go/blob/7f2581f910fc21497091c4109b56d310276fc943/stream.go#L117-L125.
	if t.recvErr != nil {
		return nil, t.recvErr
	}

	if t.sentResponse {
		return nil, io.EOF
	}
	t.sentResponse = true

	return t.response, nil
}

func (t *testExemplarClient) Exemplars(ctx context.Context, in *exemplarspb.ExemplarsRequest, opts ...grpc.CallOption) (exemplarspb.Exemplars_ExemplarsClient, error) {
	return t, t.exemplarErr
}

var _ exemplarspb.ExemplarsClient = &testExemplarClient{}

type testExemplarServer struct {
	grpc.ServerStream
	sendErr   error
	responses []*exemplarspb.ExemplarsResponse
}

func (t *testExemplarServer) String() string {
	return "test"
}

func (t *testExemplarServer) Send(response *exemplarspb.ExemplarsResponse) error {
	if t.sendErr != nil {
		return t.sendErr
	}
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
				Query:                   "http_request_duration_bucket",
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			clients: []*exemplarspb.ExemplarStore{
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
				},
			},
			server: &testExemplarServer{},
			wantResponses: []*exemplarspb.ExemplarsResponse{
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
					Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
				}),
			},
		},
		{
			name: "warning proxy success",
			request: &exemplarspb.ExemplarsRequest{
				Query:                   "http_request_duration_bucket",
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
							SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
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
							SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
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
					SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"__name__": "http_request_duration_bucket"}))},
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
							SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "bar"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
				},
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "baz"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 2}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "B"})},
				},
			},
			server: &testExemplarServer{},
			wantResponses: []*exemplarspb.ExemplarsResponse{
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "bar"}))},
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
							SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "bar"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "A"})},
				},
				{
					ExemplarsClient: &testExemplarClient{
						response: exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
							SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "baz"}))},
							Exemplars:    []*exemplarspb.Exemplar{{Value: 2}},
						}),
					},
					LabelSets: []labels.Labels{labels.FromMap(map[string]string{"cluster": "B"})},
				},
			},
			server: &testExemplarServer{},
			wantResponses: []*exemplarspb.ExemplarsResponse{
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "bar"}))},
					Exemplars:    []*exemplarspb.Exemplar{{Value: 1}},
				}),
				exemplarspb.NewExemplarsResponse(&exemplarspb.ExemplarData{
					SeriesLabels: labelpb.ZLabelSet{labelpb.ZLabelsFromPromLabels(labels.FromMap(map[string]string{"foo": "baz"}))},
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

			if !reflect.DeepEqual(tc.wantResponses, tc.server.responses) {
				t.Errorf("want response %v, got %v", tc.wantResponses, tc.server.responses)
			}
		})
	}
}
