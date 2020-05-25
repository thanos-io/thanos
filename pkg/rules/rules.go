// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"

	"github.com/pkg/errors"
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
}

func NewGRPCClient(rs rulespb.RulesServer) *GRPCClient {
	return &GRPCClient{
		proxy: rs,
	}
}

func (rr *GRPCClient) Rules(ctx context.Context, req *rulespb.RulesRequest) (*rulespb.RuleGroups, storage.Warnings, error) {
	resp := &rulesServer{ctx: ctx}

	if err := rr.proxy.Rules(req, resp); err != nil {
		return nil, nil, errors.Wrap(err, "proxy RuleGroups()")
	}

	return &rulespb.RuleGroups{Groups: resp.groups}, resp.warnings, nil
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
