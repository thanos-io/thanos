// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"github.com/prometheus/prometheus/rules"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func NewRulesRetriever(rs storepb.RulesServer) *rulesRetriever {
	return &rulesRetriever{
		rulesServer: rs,
	}
}

type rulesRetriever struct {
	rulesServer storepb.RulesServer
}

func (rr *rulesRetriever) RuleGroups() ([]*rules.Group, error) {
	return nil, nil
}
