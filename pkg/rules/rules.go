// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
)

var _ UnaryClient = &GRPCClient{}

// UnaryClient is gRPC rulespb.Rules client which expands streaming rules API. Useful for consumers that does not
// support streaming.
type UnaryClient interface {
	Rules(ctx context.Context, req *rulespb.RulesRequest) (*rulespb.RuleGroups, storage.Warnings, error)
}

// GRPCClient allows to retrieve rules from local gRPC streaming server implementation.
// TODO(bwplotka): Switch to native gRPC transparent client->server adapter once available.
type GRPCClient struct {
	proxy rulespb.RulesServer

	replicaLabels map[string]struct{}
}

func NewGRPCClient(rs rulespb.RulesServer) *GRPCClient {
	return NewGRPCClientWithDedup(rs, nil)
}

func NewGRPCClientWithDedup(rs rulespb.RulesServer, replicaLabels []string) *GRPCClient {
	c := &GRPCClient{
		proxy:         rs,
		replicaLabels: map[string]struct{}{},
	}

	for _, label := range replicaLabels {
		c.replicaLabels[label] = struct{}{}
	}
	return c
}

func (rr *GRPCClient) Rules(ctx context.Context, req *rulespb.RulesRequest) (*rulespb.RuleGroups, storage.Warnings, error) {
	resp := &rulesServer{ctx: ctx}

	if err := rr.proxy.Rules(req, resp); err != nil {
		return nil, nil, errors.Wrap(err, "proxy Rules")
	}

	// TODO(bwplotka): Move to SortInterface with equal method and heap.
	resp.groups = dedupGroups(resp.groups)
	for _, g := range resp.groups {
		g.Rules = dedupRules(g.Rules, rr.replicaLabels)
	}

	return &rulespb.RuleGroups{Groups: resp.groups}, resp.warnings, nil
}

// dedupRules re-sorts the set so that the same series with different replica
// labels are coming right after each other.
func dedupRules(rules []*rulespb.Rule, replicaLabels map[string]struct{}) []*rulespb.Rule {
	if len(rules) == 0 {
		return rules
	}

	// Remove replica labels and sort each rule's label names such that they are comparable.
	for _, r := range rules {
		removeReplicaLabels(r, replicaLabels)
		sort.Slice(r.GetLabels(), func(i, j int) bool {
			return r.GetLabels()[i].Name < r.GetLabels()[j].Name
		})
	}

	// Sort rules globally.
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].Compare(rules[j]) < 0
	})

	// Remove rules based on synthesized deduplication labels.
	i := 0
	for j := 1; j < len(rules); j++ {
		if rules[i].Compare(rules[j]) != 0 {
			// Effectively retain rules[j] in the resulting slice.
			i++
			rules[i] = rules[j]
			continue
		}

		// If rules are the same, ordering is still determined depending on type.
		switch {
		case rules[i].GetRecording() != nil && rules[j].GetRecording() != nil:
			if rules[i].GetRecording().Compare(rules[j].GetRecording()) <= 0 {
				continue
			}
		case rules[i].GetAlert() != nil && rules[j].GetAlert() != nil:
			if rules[i].GetAlert().Compare(rules[j].GetAlert()) <= 0 {
				continue
			}
		default:
			continue
		}

		// Swap if we found a younger recording rule or a younger firing alerting rule.
		rules[i] = rules[j]
	}
	return rules[:i+1]
}

func removeReplicaLabels(r *rulespb.Rule, replicaLabels map[string]struct{}) {
	lbls := r.GetLabels()
	newLabels := make(labels.Labels, 0, len(lbls))
	for _, l := range lbls {
		if _, ok := replicaLabels[l.Name]; !ok {
			newLabels = append(newLabels, l)
		}
	}
	r.SetLabels(newLabels)
}

func dedupGroups(groups []*rulespb.RuleGroup) []*rulespb.RuleGroup {
	if len(groups) == 0 {
		return nil
	}

	// Sort groups such that they appear next to each other.
	sort.Slice(groups, func(i, j int) bool { return groups[i].Compare(groups[j]) < 0 })

	i := 0
	for _, g := range groups[1:] {
		if g.Compare(groups[i]) == 0 {
			groups[i].Rules = append(groups[i].Rules, g.Rules...)
		} else {
			i++
			groups[i] = g
		}
	}
	return groups[:i+1]
}

type rulesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	rulespb.Rules_RulesServer
	ctx context.Context

	warnings []error
	groups   []*rulespb.RuleGroup
}

func (srv *rulesServer) Send(res *rulespb.RulesResponse) error {
	if res.GetWarning() != "" {
		srv.warnings = append(srv.warnings, errors.New(res.GetWarning()))
		return nil
	}

	if res.GetGroup() == nil {
		return errors.New("no group")
	}

	srv.groups = append(srv.groups, res.GetGroup())
	return nil
}

func (srv *rulesServer) Context() context.Context {
	return srv.ctx
}
