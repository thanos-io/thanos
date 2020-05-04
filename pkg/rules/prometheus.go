// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"net/url"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
)

// Prometheus implements rulespb.Rules.
type Prometheus struct {
	base   *url.URL
	client *promclient.Client
}

// NewPrometheus creates new rules.Prometheus.
func NewPrometheus(base *url.URL, client *promclient.Client) *Prometheus {
	return &Prometheus{
		base:   base,
		client: client,
	}
}

// Rules returns all specified rules from Prometheus.
func (p *Prometheus) Rules(r *rulespb.RulesRequest, s rulespb.Rules_RulesServer) error {
	var typeRules string
	if r.Type != rulespb.RulesRequest_ALL {
		typeRules = r.Type.String()
	}
	groups, err := p.client.RulesInGRPC(s.Context(), p.base, typeRules)
	if err != nil {
		return err
	}

	for _, g := range groups {
		if err := s.Send(&rulespb.RulesResponse{Result: &rulespb.RulesResponse_Group{Group: g}}); err != nil {
			return err
		}
	}
	return nil
}
