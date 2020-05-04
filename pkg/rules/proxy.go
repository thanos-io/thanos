// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"io"
	"sort"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Proxy implements rulespb.Rules that fanouts requests to given rulespb.Rules and deduplication on the way.
type Proxy struct {
	logger log.Logger
	rules  func() []rulespb.RulesClient
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

	groups = dedupGroups(groups)

	for _, g := range groups {
		if err := srv.Send(rulespb.NewRuleGroupRulesResponse(g)); err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send rules response").Error())
		}
	}

	return nil
}

func dedupGroups(groups []*rulespb.RuleGroup) []*rulespb.RuleGroup {
	// sort groups such that they appear next to each other.
	sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })

	// nothing to do if we have no or a single result, also no deduplication is necessary.
	if len(groups) < 2 {
		return groups
	}

	i := 0
	for _, g := range groups[1:] {
		if g.Name == groups[i].Name {
			groups[i].Rules = append(groups[i].Rules, g.Rules...)
		} else {
			i++
			groups[i] = g
		}
	}

	return groups[:i+1]
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

		if err := stream.server.Send(rulespb.NewWarningRulesResponse(err)); err != nil {
			return err
		}
	}

	for {
		rule, err := rules.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			err = errors.Wrapf(err, "receiving rules from rules client %v", stream.client)

			if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
				return err
			}

			if err := stream.server.Send(rulespb.NewWarningRulesResponse(err)); err != nil {
				return errors.Wrapf(err, "sending rules error to server %v", stream.server)
			}

			continue
		}

		if w := rule.GetWarning(); w != "" {
			if err := stream.server.Send(rulespb.NewWarningRulesResponse(errors.New(w))); err != nil {
				return errors.Wrapf(err, "sending rules warning to server %v", stream.server)
			}
			continue
		}

		select {
		case stream.channel <- rule.GetGroup():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
