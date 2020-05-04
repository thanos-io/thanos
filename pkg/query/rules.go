// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func NewRulesRetriever(rs storepb.RulesServer) *rulesRetriever {
	return &rulesRetriever{
		proxy: rs,
	}
}

type rulesRetriever struct {
	proxy storepb.RulesServer
}

func (rr *rulesRetriever) RuleGroups(ctx context.Context) ([]*storepb.RuleGroup, storage.Warnings, error) {
	resp := &rulesServer{ctx: ctx}

	if err := rr.proxy.Rules(&storepb.RulesRequest{
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	}, resp); err != nil {
		return nil, nil, errors.Wrap(err, "proxy RuleGroups()")
	}

	return resp.groups, resp.warnings, nil
}

type rulesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Rules_RulesServer
	ctx context.Context

	warnings []error
	groups   []*storepb.RuleGroup
}

func (srv *rulesServer) Send(res *storepb.RulesResponse) error {
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
