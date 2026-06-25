// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"sort"
	"sync"
	"text/template"
	"text/template/parse"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/extpromql"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

var _ UnaryClient = &GRPCClient{}

// UnaryClient is gRPC rulespb.Rules client which expands streaming rules API. Useful for consumers that does not
// support streaming.
type UnaryClient interface {
	Rules(ctx context.Context, req *rulespb.RulesRequest) (*rulespb.RuleGroups, annotations.Annotations, error)
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

func (rr *GRPCClient) Rules(ctx context.Context, req *rulespb.RulesRequest) (*rulespb.RuleGroups, annotations.Annotations, error) {
	span, ctx := tracing.StartSpan(ctx, "rules_request")
	defer span.Finish()

	resp := &rulesServer{ctx: ctx}

	if err := rr.proxy.Rules(req, resp); err != nil {
		return nil, nil, errors.Wrap(err, "proxy Rules")
	}

	var err error
	matcherSets := make([][]*labels.Matcher, len(req.MatcherString))
	for i, s := range req.MatcherString {
		matcherSets[i], err = extpromql.ParseMetricSelector(s)
		if err != nil {
			return nil, nil, errors.Wrap(err, "parser ParseMetricSelector")
		}
	}

	resp.groups = filterRulesByMatchers(resp.groups, matcherSets)
	resp.groups = filterRulesByNamesAndFile(resp.groups, req.RuleName, req.RuleGroup, req.File)

	// TODO(bwplotka): Move to SortInterface with equal method and heap.
	resp.groups = dedupGroups(resp.groups)
	for _, g := range resp.groups {
		g.Rules = dedupRules(g.Rules, rr.replicaLabels)
	}

	return &rulespb.RuleGroups{Groups: resp.groups}, resp.warnings, nil
}

// filters rules by group name, rule name or file.
func filterRulesByNamesAndFile(ruleGroups []*rulespb.RuleGroup, ruleName []string, ruleGroup []string, file []string) []*rulespb.RuleGroup {
	if len(ruleName) == 0 && len(ruleGroup) == 0 && len(file) == 0 {
		return ruleGroups
	}

	queryFormToSet := func(values []string) map[string]struct{} {
		set := make(map[string]struct{}, len(values))
		for _, v := range values {
			set[v] = struct{}{}
		}
		return set
	}

	rnSet := queryFormToSet(ruleName)
	rgSet := queryFormToSet(ruleGroup)
	fSet := queryFormToSet(file)

	rgs := make([]*rulespb.RuleGroup, 0, len(ruleGroups))
	for _, grp := range ruleGroups {
		if len(rgSet) > 0 {
			if _, ok := rgSet[grp.Name]; !ok {
				continue
			}
		}

		if len(fSet) > 0 {
			if _, ok := fSet[grp.File]; !ok {
				continue
			}
		}

		if len(rnSet) > 0 {
			ruleCount := 0
			for _, r := range grp.Rules {
				if _, ok := rnSet[r.GetName()]; ok {
					grp.Rules[ruleCount] = r
					ruleCount++
				}
			}
			grp.Rules = grp.Rules[:ruleCount]
		}
		if len(grp.Rules) > 0 {
			rgs = append(rgs, grp)
		}
	}
	return rgs
}

// filterRulesbyMatchers filters rules in a group according to given matcherSets.
func filterRulesByMatchers(ruleGroups []*rulespb.RuleGroup, matcherSets [][]*labels.Matcher) []*rulespb.RuleGroup {
	if len(matcherSets) == 0 {
		return ruleGroups
	}

	groupCount := 0
	for _, g := range ruleGroups {
		ruleCount := 0
		for _, r := range g.Rules {
			// Filter rules based on matcher.
			rl := r.GetLabels()
			if matches(matcherSets, rl) {
				g.Rules[ruleCount] = r
				ruleCount++
			}
		}
		g.Rules = g.Rules[:ruleCount]

		// Filter groups based on number of rules.
		if len(g.Rules) != 0 {
			ruleGroups[groupCount] = g
			groupCount++
		}
	}
	ruleGroups = ruleGroups[:groupCount]

	return ruleGroups
}

// matches returns whether the non-templated labels satisfy all the matchers in matcherSets.
func matches(matcherSets [][]*labels.Matcher, l labels.Labels) bool {
	if len(matcherSets) == 0 {
		return true
	}

	b := labels.NewBuilder(labels.EmptyLabels())
	labelTemplate := template.New("label")
	l.Range(func(label labels.Label) {
		t, err := labelTemplate.Parse(label.Value)
		// Label value is non-templated if it is one node of type NodeText.
		if err == nil && len(t.Root.Nodes) == 1 && t.Root.Nodes[0].Type() == parse.NodeText {
			b.Set(label.Name, label.Value)
		}
	})
	nonTemplatedLabels := b.Labels()

	for _, matchers := range matcherSets {
		for _, m := range matchers {
			if v := nonTemplatedLabels.Get(m.Name); !m.Matches(v) {
				return false
			}
		}
	}
	return true
}

// dedupRules removes rules that differ only by replica labels while preserving
// the order in which unique rules first appeared.
func dedupRules(rules []*rulespb.Rule, replicaLabels map[string]struct{}) []*rulespb.Rule {
	if len(rules) == 0 {
		return rules
	}

	if len(replicaLabels) > 0 {
		for _, r := range rules {
			removeReplicaLabels(r, replicaLabels)
		}
	}

	ordered := make([]int, len(rules))
	for i := range ordered {
		ordered[i] = i
	}

	// Sort indexes so equal rules are adjacent without changing output order.
	sort.Slice(ordered, func(i, j int) bool {
		if d := rules[ordered[i]].Compare(rules[ordered[j]]); d != 0 {
			return d < 0
		}
		return ordered[i] < ordered[j]
	})

	keep := make([]bool, len(rules))
	for i := 0; i < len(ordered); {
		first := ordered[i]
		best := first

		j := i + 1
		for ; j < len(ordered) && rules[ordered[i]].Compare(rules[ordered[j]]) == 0; j++ {
			if ordered[j] < first {
				first = ordered[j]
			}
			if preferredRule(rules[best], rules[ordered[j]]) != rules[best] {
				best = ordered[j]
			}
		}

		rules[first] = rules[best]
		keep[first] = true
		i = j
	}

	i := 0
	for j, r := range rules {
		if keep[j] {
			rules[i] = r
			i++
		}
	}
	return rules[:i]
}

func preferredRule(current, candidate *rulespb.Rule) *rulespb.Rule {
	switch {
	case current.GetRecording() != nil && candidate.GetRecording() != nil:
		if current.GetRecording().Compare(candidate.GetRecording()) <= 0 {
			return current
		}
	case current.GetAlert() != nil && candidate.GetAlert() != nil:
		if current.GetAlert().Compare(candidate.GetAlert()) <= 0 {
			return current
		}
	default:
		return current
	}
	return candidate
}

func removeReplicaLabels(r *rulespb.Rule, replicaLabels map[string]struct{}) {
	removeLabels := func(labelSet *labelpb.ZLabelSet) {
		i := 0
		for _, label := range labelSet.Labels {
			if _, ok := replicaLabels[label.Name]; ok {
				continue
			}
			labelSet.Labels[i] = label
			i++
		}
		labelSet.Labels = labelSet.Labels[:i]
		if len(labelSet.Labels) == 0 {
			labelSet.Labels = nil
		}
	}

	switch {
	case r.GetRecording() != nil:
		removeLabels(&r.GetRecording().Labels)
	case r.GetAlert() != nil:
		removeLabels(&r.GetAlert().Labels)
	}
}

func dedupGroups(groups []*rulespb.RuleGroup) []*rulespb.RuleGroup {
	if len(groups) == 0 {
		return nil
	}

	ordered := make([]int, len(groups))
	for i := range ordered {
		ordered[i] = i
	}

	// Sort indexes so equal groups are adjacent without changing output order.
	sort.Slice(ordered, func(i, j int) bool {
		if d := groups[ordered[i]].Compare(groups[ordered[j]]); d != 0 {
			return d < 0
		}
		return ordered[i] < ordered[j]
	})

	keep := make([]bool, len(groups))
	for i := 0; i < len(ordered); {
		first := ordered[i]

		j := i + 1
		for ; j < len(ordered) && groups[ordered[i]].Compare(groups[ordered[j]]) == 0; j++ {
			groups[first].Rules = append(groups[first].Rules, groups[ordered[j]].Rules...)
		}

		keep[first] = true
		i = j
	}

	i := 0
	for j, g := range groups {
		if keep[j] {
			groups[i] = g
			i++
		}
	}
	return groups[:i]
}

type rulesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	rulespb.Rules_RulesServer
	ctx context.Context

	warnings annotations.Annotations
	groups   []*rulespb.RuleGroup
	mu       sync.Mutex
}

func (srv *rulesServer) Send(res *rulespb.RulesResponse) error {
	if res.GetWarning() != "" {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		srv.warnings.Add(errors.New(res.GetWarning()))
		return nil
	}

	if res.GetGroup() == nil {
		return errors.New("no group")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.groups = append(srv.groups, res.GetGroup())
	return nil
}

func (srv *rulesServer) Context() context.Context {
	return srv.ctx
}
