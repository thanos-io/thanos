// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type testRulesClient struct {
	grpc.ClientStream
	rulesErr, recvErr error
	response          *rulespb.RulesResponse
	sentResponse      bool
}

func (t *testRulesClient) String() string {
	return "test"
}

func (t *testRulesClient) Recv() (*rulespb.RulesResponse, error) {
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

func (t *testRulesClient) Rules(ctx context.Context, in *rulespb.RulesRequest, opts ...grpc.CallOption) (rulespb.Rules_RulesClient, error) {
	return t, t.rulesErr
}

var _ rulespb.RulesClient = &testRulesClient{}

type testRulesServer struct {
	grpc.ServerStream
	sendErr  error
	response *rulespb.RulesResponse
}

func (t *testRulesServer) String() string {
	return "test"
}

func (t *testRulesServer) Send(response *rulespb.RulesResponse) error {
	if t.sendErr != nil {
		return t.sendErr
	}
	t.response = response
	return nil
}

func (t *testRulesServer) Context() context.Context {
	return context.Background()
}

func TestProxy(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)

	for _, tc := range []struct {
		name         string
		request      *rulespb.RulesRequest
		client       rulespb.RulesClient
		server       *testRulesServer
		wantResponse *rulespb.RulesResponse
		wantError    error
	}{
		{
			name: "rule group proxy success",
			request: &rulespb.RulesRequest{
				Type:                    rulespb.RulesRequest_ALL,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			client: &testRulesClient{
				response: rulespb.NewRuleGroupRulesResponse(&rulespb.RuleGroup{
					Name: "foo",
				}),
				recvErr: nil,
			},
			server: &testRulesServer{},
			wantResponse: rulespb.NewRuleGroupRulesResponse(&rulespb.RuleGroup{
				Name: "foo",
			}),
		},
		{
			name: "warning proxy success",
			request: &rulespb.RulesRequest{
				Type:                    rulespb.RulesRequest_ALL,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			client: &testRulesClient{
				response: rulespb.NewWarningRulesResponse(errors.New("warning from client")),
				recvErr:  nil,
			},
			server:       &testRulesServer{},
			wantResponse: rulespb.NewWarningRulesResponse(errors.New("warning from client")),
		},
		{
			name: "warn: retrieving rules client failed",
			request: &rulespb.RulesRequest{
				Type:                    rulespb.RulesRequest_ALL,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			client: &testRulesClient{
				response: nil,
				rulesErr: errors.New("retrieving rules failed"),
			},
			server:       &testRulesServer{},
			wantResponse: rulespb.NewWarningRulesResponse(errors.New("fetching rules from rules client test: retrieving rules failed")),
		},
		{
			name: "warn: retrieving rules client failed, forward warning failed",
			request: &rulespb.RulesRequest{
				Type:                    rulespb.RulesRequest_ALL,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			client: &testRulesClient{
				response: nil,
				rulesErr: errors.New("retrieving rules failed"),
			},
			server: &testRulesServer{
				sendErr: errors.New("forwarding warning response failed"),
			},
			wantError: errors.New("forwarding warning response failed"),
		},
		{
			name: "abort: retrieving rules client failed",
			request: &rulespb.RulesRequest{
				Type:                    rulespb.RulesRequest_ALL,
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			client: &testRulesClient{
				response: nil,
				rulesErr: errors.New("retrieving rules failed"),
			},
			server:    &testRulesServer{},
			wantError: errors.New("fetching rules from rules client test: retrieving rules failed"),
		},
		{
			name: "warn: receive failed",
			request: &rulespb.RulesRequest{
				Type:                    rulespb.RulesRequest_ALL,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			client: &testRulesClient{
				response: nil,
				recvErr:  errors.New("503 from Prometheus"),
			},
			server:       &testRulesServer{},
			wantResponse: rulespb.NewWarningRulesResponse(errors.New("receiving rules from rules client test: 503 from Prometheus")),
		},
		{
			name: "warn: receive failed, forward warning failed",
			request: &rulespb.RulesRequest{
				Type:                    rulespb.RulesRequest_ALL,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			client: &testRulesClient{
				response: nil,
				recvErr:  errors.New("503 from Prometheus"),
			},
			server: &testRulesServer{
				sendErr: errors.New("forwarding warning response failed"),
			},
			wantError: errors.New("sending rules error to server test: forwarding warning response failed"),
		},
		{
			name: "abort: receive failed",
			request: &rulespb.RulesRequest{
				Type:                    rulespb.RulesRequest_ALL,
				PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
			},
			client: &testRulesClient{
				response: nil,
				recvErr:  errors.New("503 from Prometheus"),
			},
			server:    &testRulesServer{},
			wantError: errors.New("receiving rules from rules client test: 503 from Prometheus"),
		},
		{
			name: "send failed",
			request: &rulespb.RulesRequest{
				Type:                    rulespb.RulesRequest_ALL,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			client: &testRulesClient{
				response: rulespb.NewRuleGroupRulesResponse(&rulespb.RuleGroup{
					Name: "foo",
				}),
				recvErr: nil,
			},
			server: &testRulesServer{
				sendErr: errors.New("sending message failed"),
			},
			wantError: errors.New("rpc error: code = Unknown desc = send rules response: sending message failed"),
		},
		{
			name: "sending warning response failed",
			request: &rulespb.RulesRequest{
				Type:                    rulespb.RulesRequest_ALL,
				PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
			},
			client: &testRulesClient{
				response: rulespb.NewWarningRulesResponse(errors.New("warning from client")),
				recvErr:  nil,
			},
			server: &testRulesServer{
				sendErr: errors.New("sending message failed"),
			},
			wantError: errors.New("sending rules warning to server test: sending message failed"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewProxy(logger, func() []rulespb.RulesClient {
				return []rulespb.RulesClient{tc.client}
			})

			err := p.Rules(tc.request, tc.server)
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

			if !reflect.DeepEqual(tc.wantResponse, tc.server.response) {
				t.Errorf("want response %v, got %v", tc.wantResponse, tc.server.response)
			}
		})
	}
}

// TestProxyDataRace find the concurrent data race bug ( go test -race -run TestProxyDataRace -v ).
func TestProxyDataRace(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)
	p := NewProxy(logger, func() []rulespb.RulesClient {
		es := &testRulesClient{
			recvErr: errors.New("err"),
		}
		size := 100
		endpoints := make([]rulespb.RulesClient, 0, size)
		for i := 0; i < size; i++ {
			endpoints = append(endpoints, es)
		}
		return endpoints
	})
	req := &rulespb.RulesRequest{
		Type:                    rulespb.RulesRequest_ALL,
		PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
	}
	s := &rulesServer{
		ctx: context.Background(),
	}
	_ = p.Rules(req, s)
}
