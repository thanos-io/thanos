// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type testMetadataClient struct {
	grpc.ClientStream
	metadataErr, recvErr error
	response             *metadatapb.MetricMetadataResponse
	sentResponse         atomic.Bool
}

func (t *testMetadataClient) String() string {
	return "test"
}

func (t *testMetadataClient) Recv() (*metadatapb.MetricMetadataResponse, error) {
	if t.recvErr != nil {
		return nil, t.recvErr
	}

	if t.sentResponse.Load() {
		return nil, io.EOF
	}
	t.sentResponse.Store(true)

	return t.response, nil
}

func (t *testMetadataClient) MetricMetadata(ctx context.Context, in *metadatapb.MetricMetadataRequest, opts ...grpc.CallOption) (metadatapb.Metadata_MetricMetadataClient, error) {
	return t, t.metadataErr
}

var _ metadatapb.MetadataClient = &testMetadataClient{}

// TestProxyDataRace find the concurrent data race bug ( go test -race -run TestProxyDataRace -v ).
func TestProxyDataRace(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)
	p := NewProxy(logger, func() []metadatapb.MetadataClient {
		es := &testMetadataClient{
			recvErr: errors.New("err"),
		}
		size := 100
		endpoints := make([]metadatapb.MetadataClient, 0, size)
		for i := 0; i < size; i++ {
			endpoints = append(endpoints, es)
		}
		return endpoints
	})
	req := &metadatapb.MetricMetadataRequest{
		Metric:                  `http_request_duration_bucket`,
		PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
	}
	s := &metadataServer{
		ctx: context.Background(),
	}
	_ = p.MetricMetadata(req, s)
}
