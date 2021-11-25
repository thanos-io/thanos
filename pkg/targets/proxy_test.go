// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package targets

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
)

type testTargetsClient struct {
	grpc.ClientStream
	targetsErr, recvErr error
	response            *targetspb.TargetsResponse
	sentResponse        bool
}

func (t *testTargetsClient) String() string {
	return "test"
}

func (t *testTargetsClient) Recv() (*targetspb.TargetsResponse, error) {
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

func (t *testTargetsClient) Targets(ctx context.Context, in *targetspb.TargetsRequest, opts ...grpc.CallOption) (targetspb.Targets_TargetsClient, error) {
	return t, t.targetsErr
}

var _ targetspb.TargetsClient = &testTargetsClient{}

// TestProxyDataRace find the concurrent data race bug ( go test -race -run TestProxyDataRace -v ).
func TestProxyDataRace(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)
	p := NewProxy(logger, func() []targetspb.TargetsClient {
		es := &testTargetsClient{
			recvErr: errors.New("err"),
		}
		size := 100
		endpoints := make([]targetspb.TargetsClient, 0, size)
		for i := 0; i < size; i++ {
			endpoints = append(endpoints, es)
		}
		return endpoints
	})
	req := &targetspb.TargetsRequest{
		State:                   targetspb.TargetsRequest_ANY,
		PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
	}
	s := &targetsServer{
		ctx: context.Background(),
	}
	_ = p.Targets(req, s)
}
