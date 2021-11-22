// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"net/url"
	"strings"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// Prometheus implements rulespb.Rules gRPC that allows to fetch rules from Prometheus HTTP api/v1/rules endpoint.
type Prometheus struct {
	base   *url.URL
	client *promclient.Client

	extLabels func() labels.Labels
}

// NewPrometheus creates new rules.Prometheus.
func NewPrometheus(base *url.URL, client *promclient.Client, extLabels func() labels.Labels) *Prometheus {
	return &Prometheus{
		base:      base,
		client:    client,
		extLabels: extLabels,
	}
}

// Rules returns all specified rules from Prometheus.
func (p *Prometheus) Rules(r *rulespb.RulesRequest, s rulespb.Rules_RulesServer) error {
	var typeRules string
	if r.Type != rulespb.RulesRequest_ALL {
		typeRules = strings.ToLower(r.Type.String())
	}
	groups, err := p.client.RulesInGRPC(s.Context(), p.base, typeRules)
	if err != nil {
		return err
	}

	// Prometheus does not add external labels, so we need to add on our own.
	enrichRulesWithExtLabels(groups, p.extLabels())

	for _, g := range groups {
		if err := s.Send(&rulespb.RulesResponse{Result: &rulespb.RulesResponse_Group{Group: g}}); err != nil {
			return err
		}
	}
	return nil
}

// extLset has to be sorted.
func enrichRulesWithExtLabels(groups []*rulespb.RuleGroup, extLset labels.Labels) {
	for _, g := range groups {
		for _, r := range g.Rules {
			r.SetLabels(labelpb.ExtendSortedLabels(r.GetLabels(), extLset))
		}
	}
}
