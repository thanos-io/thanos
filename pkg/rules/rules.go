// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// Retriever allows to retrieve rules from gRPC server implementation.
// TODO(bwplotka): Switch to native gRPC transparent client->server adapter once available.
type Retriever struct {
	proxy rulespb.RulesServer
}

func NewRetriever(rs rulespb.RulesServer) *Retriever {
	return &Retriever{
		proxy: rs,
	}
}

func (rr *Retriever) RuleGroups(ctx context.Context) ([]*rulespb.RuleGroup, storage.Warnings, error) {
	resp := &rulesServer{ctx: ctx}

	if err := rr.proxy.Rules(&rulespb.RulesRequest{
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	}, resp); err != nil {
		return nil, nil, errors.Wrap(err, "proxy RuleGroups()")
	}

	return resp.groups, resp.warnings, nil
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
