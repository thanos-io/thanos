// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package targets

import (
	"context"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/store/labelpbv2"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
)

var _ UnaryClient = &GRPCClient{}

// UnaryClient is gRPC targetspb.Targets client which expands streaming targets API. Useful for consumers that does not
// support streaming.
type UnaryClient interface {
	Targets(ctx context.Context, req *targetspb.TargetsRequest) (*targetspb.TargetDiscovery, annotations.Annotations, error)
}

// GRPCClient allows to retrieve targets from local gRPC streaming server implementation.
// TODO(bwplotka): Switch to native gRPC transparent client->server adapter once available.
type GRPCClient struct {
	proxy targetspb.TargetsServer

	replicaLabels map[string]struct{}
}

func NewGRPCClient(ts targetspb.TargetsServer) *GRPCClient {
	return NewGRPCClientWithDedup(ts, nil)
}

func NewGRPCClientWithDedup(ts targetspb.TargetsServer, replicaLabels []string) *GRPCClient {
	c := &GRPCClient{
		proxy:         ts,
		replicaLabels: map[string]struct{}{},
	}

	for _, label := range replicaLabels {
		c.replicaLabels[label] = struct{}{}
	}
	return c
}

func (rr *GRPCClient) Targets(ctx context.Context, req *targetspb.TargetsRequest) (*targetspb.TargetDiscovery, annotations.Annotations, error) {
	resp := &targetsServer{ctx: ctx, targets: &targetspb.TargetDiscovery{
		ActiveTargets:  make([]*targetspb.ActiveTarget, 0),
		DroppedTargets: make([]*targetspb.DroppedTarget, 0),
	}}

	if err := rr.proxy.Targets(req, resp); err != nil {
		return nil, nil, errors.Wrap(err, "proxy Targets")
	}

	resp.targets = dedupTargets(resp.targets, rr.replicaLabels)

	return resp.targets, resp.warnings, nil
}

// dedupTargets re-sorts the set so that the same target with different replica
// labels are coming right after each other.
func dedupTargets(targets *targetspb.TargetDiscovery, replicaLabels map[string]struct{}) *targetspb.TargetDiscovery {
	if targets == nil {
		return nil
	}

	targets.ActiveTargets = dedupActiveTargets(targets.ActiveTargets, replicaLabels)
	targets.DroppedTargets = dedupDroppedTargets(targets.DroppedTargets, replicaLabels)

	return targets
}

func dedupDroppedTargets(droppedTargets []*targetspb.DroppedTarget, replicaLabels map[string]struct{}) []*targetspb.DroppedTarget {
	if len(droppedTargets) == 0 {
		return droppedTargets
	}

	// Sort targets globally based on synthesized deduplication labels, also considering replica labels and their values.
	sort.Slice(droppedTargets, func(i, j int) bool {
		return droppedTargets[i].Compare(droppedTargets[j]) < 0
	})

	// Remove targets based on synthesized deduplication labels, this time ignoring replica labels
	i := 0
	droppedTargets[i].DiscoveredLabels = removeReplicaLabelsProm(
		droppedTargets[i].DiscoveredLabels,
		replicaLabels,
	)
	for j := 1; j < len(droppedTargets); j++ {
		droppedTargets[j].DiscoveredLabels = removeReplicaLabelsProm(
			droppedTargets[j].DiscoveredLabels,
			replicaLabels,
		)
		if droppedTargets[i].Compare(droppedTargets[j]) != 0 {
			// Effectively retain targets[j] in the resulting slice.
			i++
			droppedTargets[i] = droppedTargets[j]
			continue
		}
	}

	return droppedTargets[:i+1]
}

func dedupActiveTargets(activeTargets []*targetspb.ActiveTarget, replicaLabels map[string]struct{}) []*targetspb.ActiveTarget {
	if len(activeTargets) == 0 {
		return activeTargets
	}

	// Sort each target's label names such that they are comparable.
	// Sort targets globally based on synthesized deduplication labels, also considering replica labels and their values.
	sort.Slice(activeTargets, func(i, j int) bool {
		return activeTargets[i].Compare(activeTargets[j]) < 0
	})

	// Remove targets based on synthesized deduplication labels, this time ignoring replica labels and last scrape.
	i := 0
	activeTargets[i].DiscoveredLabels = removeReplicaLabelsProm(
		activeTargets[i].DiscoveredLabels,
		replicaLabels,
	)
	activeTargets[i].Labels = removeReplicaLabelsProm(
		activeTargets[i].Labels,
		replicaLabels,
	)
	for j := 1; j < len(activeTargets); j++ {
		activeTargets[j].DiscoveredLabels = removeReplicaLabelsProm(
			activeTargets[j].DiscoveredLabels,
			replicaLabels,
		)
		activeTargets[j].Labels = removeReplicaLabelsProm(
			activeTargets[j].Labels,
			replicaLabels,
		)

		if activeTargets[i].Compare(activeTargets[j]) != 0 {
			// Effectively retain targets[j] in the resulting slice.
			i++
			activeTargets[i] = activeTargets[j]
			continue
		}

		if activeTargets[i].CompareState(activeTargets[j]) <= 0 {
			continue
		}

		// Swap if we found a younger target.
		activeTargets[i] = activeTargets[j]
	}

	return activeTargets[:i+1]
}

func removeReplicaLabelsProm(ls labelpbv2.LabelSetV2, replicaLabels map[string]struct{}) labelpbv2.LabelSetV2 {
	lbls := labels.NewBuilder(labels.Labels(ls))
	for rl := range replicaLabels {
		lbls.Del(rl)
	}
	return labelpbv2.LabelSetV2(lbls.Labels())
}

type targetsServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	targetspb.Targets_TargetsServer
	ctx context.Context

	warnings annotations.Annotations
	targets  *targetspb.TargetDiscovery
	mu       sync.Mutex
}

func (srv *targetsServer) Send(res *targetspb.TargetsResponse) error {
	if res.GetWarning() != "" {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		srv.warnings.Add(errors.New(res.GetWarning()))
		return nil
	}

	if res.GetTargets() == nil {
		return errors.New("no targets")
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.targets.ActiveTargets = append(srv.targets.ActiveTargets, res.GetTargets().ActiveTargets...)
	srv.targets.DroppedTargets = append(srv.targets.DroppedTargets, res.GetTargets().DroppedTargets...)

	return nil
}

func (srv *targetsServer) Context() context.Context {
	return srv.ctx
}
