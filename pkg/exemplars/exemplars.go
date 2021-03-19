// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"context"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

var _ UnaryClient = &GRPCClient{}

// UnaryClient is gRPC exemplarspb.Exemplars client which expands streaming exemplars API. Useful for consumers that does not
// support streaming.
type UnaryClient interface {
	Exemplars(ctx context.Context, req *exemplarspb.ExemplarsRequest) ([]*exemplarspb.ExemplarData, storage.Warnings, error)
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

	warnings []error
	data     []*exemplarspb.ExemplarData
}

func (srv *exemplarsServer) Send(res *exemplarspb.ExemplarsResponse) error {
	if res.GetWarning() != "" {
		srv.warnings = append(srv.warnings, errors.New(res.GetWarning()))
		return nil
	}

	if res.GetData() == nil {
		return errors.New("empty exemplars data")
	}

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

func (rr *GRPCClient) Exemplars(ctx context.Context, req *exemplarspb.ExemplarsRequest) ([]*exemplarspb.ExemplarData, storage.Warnings, error) {
	resp := &exemplarsServer{ctx: ctx}

	if err := rr.proxy.Exemplars(req, resp); err != nil {
		return nil, nil, errors.Wrap(err, "proxy Exemplars")
	}

	resp.data = dedupExemplarsData(resp.data, rr.replicaLabels)
	for _, d := range resp.data {
		d.Exemplars = dedupExemplars(d.Exemplars, rr.replicaLabels)
	}

	return resp.data, resp.warnings, nil
}

func dedupExemplarsData(exemplarsData []*exemplarspb.ExemplarData, replicaLabels map[string]struct{}) []*exemplarspb.ExemplarData {
	if len(exemplarsData) == 0 {
		return exemplarsData
	}

	// Sort each exemplar's label names such that they are comparable.
	for _, d := range exemplarsData {
		sort.Slice(d.SeriesLabels.Labels, func(i, j int) bool {
			return d.SeriesLabels.Labels[i].Name < d.SeriesLabels.Labels[j].Name
		})
	}

	// Sort exemplars data such that they appear next to each other.
	sort.Slice(exemplarsData, func(i, j int) bool {
		return exemplarsData[i].Compare(exemplarsData[j]) < 0
	})

	i := 0
	removeReplicaLabels(exemplarsData[i].SeriesLabels, replicaLabels)
	for j := 1; j < len(exemplarsData); j++ {
		removeReplicaLabels(exemplarsData[j].SeriesLabels, replicaLabels)
		if exemplarsData[i].Compare(exemplarsData[j]) != 0 {
			// Effectively retain exemplarsData[j] in the resulting slice.
			i++
			exemplarsData[i] = exemplarsData[j]
			continue
		}
	}

	return exemplarsData[:i+1]
}

func dedupExemplars(exemplars []*exemplarspb.Exemplar, replicaLabels map[string]struct{}) []*exemplarspb.Exemplar {
	if len(exemplars) == 0 {
		return exemplars
	}

	for _, e := range exemplars {
		sort.Slice(e.Labels.Labels, func(i, j int) bool {
			return e.Labels.Labels[i].Name < e.Labels.Labels[j].Name
		})
	}

	sort.Slice(exemplars, func(i, j int) bool {
		return exemplars[i].Compare(exemplars[j]) < 0
	})

	i := 0
	removeReplicaLabels(exemplars[i].Labels, replicaLabels)
	for j := 1; j < len(exemplars); j++ {
		removeReplicaLabels(exemplars[j].Labels, replicaLabels)
		if exemplars[i].Compare(exemplars[j]) != 0 {
			// Effectively retain exemplars[j] in the resulting slice.
			i++
			exemplars[i] = exemplars[j]
		}
	}

	return exemplars[:i+1]
}

func removeReplicaLabels(labelSet labelpb.ZLabelSet, replicaLabels map[string]struct{}) {
	newLabels := make([]labelpb.ZLabel, 0, len(labelSet.Labels))
	for _, l := range labelSet.Labels {
		if _, ok := replicaLabels[l.Name]; !ok {
			newLabels = append(newLabels, l)
		}
	}

	labelSet.Labels = newLabels
}
