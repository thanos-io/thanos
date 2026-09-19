// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

var _ UnaryClient = &GRPCClient{}

// UnaryClient is a gRPC metadatapb.Metadata client which expands streaming metadata API. Useful for consumers that does not
// support streaming.
type UnaryClient interface {
	MetricMetadata(ctx context.Context, req *metadatapb.MetricMetadataRequest) (map[string][]metadatapb.Meta, annotations.Annotations, error)
}

// GRPCClient allows to retrieve metadata from local gRPC streaming server implementation.
// TODO(bwplotka): Switch to native gRPC transparent client->server adapter once available.
type GRPCClient struct {
	proxy metadatapb.MetadataServer
}

func NewGRPCClient(ts metadatapb.MetadataServer) *GRPCClient {
	return &GRPCClient{
		proxy: ts,
	}
}

func (rr *GRPCClient) MetricMetadata(ctx context.Context, req *metadatapb.MetricMetadataRequest) (map[string][]metadatapb.Meta, annotations.Annotations, error) {
	span, ctx := tracing.StartSpan(ctx, "metadata_grpc_request")
	defer span.Finish()

	srv := &metadataServer{ctx: ctx, metric: req.Metric, limit: int(req.Limit)}

	if req.Limit >= 0 {
		if req.Metric != "" {
			srv.metadataMap = make(map[string][]metadatapb.Meta, 1)
		} else if req.Limit <= 100 {
			srv.metadataMap = make(map[string][]metadatapb.Meta, req.Limit)
		} else {
			srv.metadataMap = make(map[string][]metadatapb.Meta)
		}
	} else {
		srv.metadataMap = make(map[string][]metadatapb.Meta)
	}

	if err := rr.proxy.MetricMetadata(req, srv); err != nil {
		return nil, nil, errors.Wrap(err, "proxy MetricMetadata")
	}

	return srv.metadataMap, srv.warnings, nil
}

type metadataServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	metadatapb.Metadata_MetricMetadataServer
	ctx context.Context

	metric string
	limit  int

	warnings    annotations.Annotations
	metadataMap map[string][]metadatapb.Meta
	mu          sync.Mutex
}

func (srv *metadataServer) Send(res *metadatapb.MetricMetadataResponse) error {
	if res.GetWarning() != "" {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		srv.warnings.Add(errors.New(res.GetWarning()))
		return nil
	}

	if res.GetMetadata() == nil {
		return errors.New("no metadata")
	}

	// If limit is set to 0, we don't need to add anything.
	if srv.limit == 0 {
		return nil
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	for k, v := range res.GetMetadata().Metadata {
		if metadata, ok := srv.metadataMap[k]; !ok {
			// If limit is set and it is positive, we limit the size of the map.
			if srv.limit < 0 || srv.limit > 0 && len(srv.metadataMap) < srv.limit {
				srv.metadataMap[k] = v.Metas
			}
		} else {
			// There shouldn't be many metadata for one single metric.
		Outer:
			for _, meta := range v.Metas {
				for _, m := range metadata {
					if meta == m {
						continue Outer
					}
				}
				srv.metadataMap[k] = append(srv.metadataMap[k], meta)
			}
		}
	}

	return nil
}

func (srv *metadataServer) Context() context.Context {
	return srv.ctx
}
