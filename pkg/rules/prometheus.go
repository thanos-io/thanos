// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"net/url"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
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

func enrichRulesWithExtLabels(groups []*rulespb.RuleGroup, extLset labels.Labels) {
	for _, g := range groups {
		for i, r := range g.Rules {
			if a := r.GetAlert(); a != nil {
				a.Labels.Labels = storepb.ExtendLabels(a.Labels.Labels, extLset)
				g.Rules[i] = rulespb.NewAlertingRule(a)
				continue
			}
			if ru := r.GetRecording(); ru != nil {
				ru.Labels.Labels = storepb.ExtendLabels(ru.Labels.Labels, extLset)
				g.Rules[i] = rulespb.NewRecordingRule(ru)
			}
		}
	}
}
