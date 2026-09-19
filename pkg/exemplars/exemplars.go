// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"context"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

var _ UnaryClient = &GRPCClient{}

// UnaryClient is gRPC exemplarspb.Exemplars client which expands streaming exemplars API. Useful for consumers that does not
// support streaming.
type UnaryClient interface {
	Exemplars(ctx context.Context, req *exemplarspb.ExemplarsRequest) ([]*exemplarspb.ExemplarData, annotations.Annotations, error)
}

// GRPCClient allows to retrieve exemplars from local gRPC streaming server implementation.
// TODO(bwplotka): Switch to native gRPC transparent client->server adapter once available.
type GRPCClient struct {
	proxy exemplarspb.ExemplarsServer

	replicaLabels map[string]struct{}
}

type exemplarsServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	exemplarspb.Exemplars_ExemplarsServer
	ctx context.Context

	warnings annotations.Annotations
	data     []*exemplarspb.ExemplarData
	mu       sync.Mutex
}

func (srv *exemplarsServer) Send(res *exemplarspb.ExemplarsResponse) error {
	if res.GetWarning() != "" {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		srv.warnings.Add(errors.New(res.GetWarning()))
		return nil
	}

	if res.GetData() == nil {
		return errors.New("empty exemplars data")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.data = append(srv.data, res.GetData())
	return nil
}

func (srv *exemplarsServer) Context() context.Context {
	return srv.ctx
}

func NewGRPCClient(es exemplarspb.ExemplarsServer) *GRPCClient {
	return NewGRPCClientWithDedup(es, nil)
}

func NewGRPCClientWithDedup(es exemplarspb.ExemplarsServer, replicaLabels []string) *GRPCClient {
	c := &GRPCClient{
		proxy:         es,
		replicaLabels: map[string]struct{}{},
	}

	for _, label := range replicaLabels {
		c.replicaLabels[label] = struct{}{}
	}
	return c
}

func (rr *GRPCClient) Exemplars(ctx context.Context, req *exemplarspb.ExemplarsRequest) ([]*exemplarspb.ExemplarData, annotations.Annotations, error) {
	span, ctx := tracing.StartSpan(ctx, "exemplar_grpc_request")
	defer span.Finish()

	resp := &exemplarsServer{ctx: ctx}

	if err := rr.proxy.Exemplars(req, resp); err != nil {
		return nil, nil, errors.Wrap(err, "proxy Exemplars")
	}

	if resp.data == nil {
		return make([]*exemplarspb.ExemplarData, 0), resp.warnings, nil
	}

	resp.data = dedupExemplarsResponse(resp.data, rr.replicaLabels)
	return resp.data, resp.warnings, nil
}

func dedupExemplarsResponse(exemplarsData []*exemplarspb.ExemplarData, replicaLabels map[string]struct{}) []*exemplarspb.ExemplarData {
	if len(exemplarsData) == 0 {
		return exemplarsData
	}

	// Deduplicate series labels.
	hashToExemplar := make(map[uint64]*exemplarspb.ExemplarData)
	for _, e := range exemplarsData {
		if len(e.Exemplars) == 0 {
			continue
		}
		e.SeriesLabels.Labels = removeReplicaLabels(e.SeriesLabels.Labels, replicaLabels)
		h := labelpb.ZLabelsToPromLabels(e.SeriesLabels.Labels).Hash()
		if ref, ok := hashToExemplar[h]; ok {
			ref.Exemplars = append(ref.Exemplars, e.Exemplars...)
		} else {
			hashToExemplar[h] = e
		}
	}

	res := make([]*exemplarspb.ExemplarData, 0, len(hashToExemplar))
	for _, e := range hashToExemplar {
		// Dedup exemplars with the same series labels.
		e.Exemplars = dedupExemplars(e.Exemplars)
		res = append(res, e)
	}

	// Sort by series labels.
	sort.Slice(res, func(i, j int) bool {
		return res[i].Compare(res[j]) < 0
	})
	return res
}

func dedupExemplars(exemplars []*exemplarspb.Exemplar) []*exemplarspb.Exemplar {
	for _, e := range exemplars {
		sort.Slice(e.Labels.Labels, func(i, j int) bool {
			return e.Labels.Labels[i].Compare(e.Labels.Labels[j]) < 0
		})
	}

	sort.Slice(exemplars, func(i, j int) bool {
		return exemplars[i].Compare(exemplars[j]) < 0
	})

	i := 0
	for j := 1; j < len(exemplars); j++ {
		if exemplars[i].Compare(exemplars[j]) != 0 {
			// Effectively retain exemplars[j] in the resulting slice.
			i++
			exemplars[i] = exemplars[j]
		}
	}

	return exemplars[:i+1]
}

func removeReplicaLabels(labels []labelpb.ZLabel, replicaLabels map[string]struct{}) []labelpb.ZLabel {
	if len(replicaLabels) == 0 {
		return labels
	}
	newLabels := make([]labelpb.ZLabel, 0, len(labels))
	for _, l := range labels {
		if _, ok := replicaLabels[l.Name]; !ok {
			newLabels = append(newLabels, l)
		}
	}

	return newLabels
}
