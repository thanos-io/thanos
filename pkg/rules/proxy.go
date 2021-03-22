// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Proxy implements rulespb.Rules gRPC that fanouts requests to given rulespb.Rules and deduplication on the way.
type Proxy struct {
	logger log.Logger
	rules  func() []rulespb.RulesClient
}

func RegisterRulesServer(rulesSrv rulespb.RulesServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		rulespb.RegisterRulesServer(s, rulesSrv)
	}
}

// NewProxy returns new rules.Proxy.
func NewProxy(logger log.Logger, rules func() []rulespb.RulesClient) *Proxy {
	return &Proxy{
		logger: logger,
		rules:  rules,
	}
}

func (s *Proxy) Rules(req *rulespb.RulesRequest, srv rulespb.Rules_RulesServer) error {
	var (
		g, gctx  = errgroup.WithContext(srv.Context())
		respChan = make(chan *rulespb.RuleGroup, 10)
		groups   []*rulespb.RuleGroup
	)

	for _, rulesClient := range s.rules() {
		rs := &rulesStream{
			client:  rulesClient,
			request: req,
			channel: respChan,
			server:  srv,
		}
		g.Go(func() error { return rs.receive(gctx) })
	}

	go func() {
		_ = g.Wait()
		close(respChan)
	}()

	for resp := range respChan {
		groups = append(groups, resp)
	}

	if err := g.Wait(); err != nil {
		level.Error(s.logger).Log("err", err)
		return err
	}

	for _, g := range groups {
		if err := srv.Send(rulespb.NewRuleGroupRulesResponse(g)); err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send rules response").Error())
		}
	}

	return nil
}

type rulesStream struct {
	client  rulespb.RulesClient
	request *rulespb.RulesRequest
	channel chan<- *rulespb.RuleGroup
	server  rulespb.Rules_RulesServer
}

func (stream *rulesStream) receive(ctx context.Context) error {
	rules, err := stream.client.Rules(ctx, stream.request)
	if err != nil {
		err = errors.Wrapf(err, "fetching rules from rules client %v", stream.client)

		if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
			return err
		}

		if serr := stream.server.Send(rulespb.NewWarningRulesResponse(err)); serr != nil {
			return serr
		}
		// Not an error if response strategy is warning.
		return nil
	}

	for {
		rule, err := rules.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			// An error happened in Recv(), hence the underlying stream is aborted
			// as per https://github.com/grpc/grpc-go/blob/7f2581f910fc21497091c4109b56d310276fc943/stream.go#L117-L125.
			// We must not continue receiving additional data from it and must return.
			err = errors.Wrapf(err, "receiving rules from rules client %v", stream.client)

			if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
				return err
			}

			if err := stream.server.Send(rulespb.NewWarningRulesResponse(err)); err != nil {
				return errors.Wrapf(err, "sending rules error to server %v", stream.server)
			}

			// Return no error if response strategy is warning.
			return nil
		}

		if w := rule.GetWarning(); w != "" {
			if err := stream.server.Send(rulespb.NewWarningRulesResponse(errors.New(w))); err != nil {
				return errors.Wrapf(err, "sending rules warning to server %v", stream.server)
			}
			// Client stream is not aborted, it is ok to receive additional data.
			continue
		}

		select {
		case stream.channel <- rule.GetGroup():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
