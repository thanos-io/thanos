// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"context"
	"io"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// Proxy implements exemplarspb.Exemplars gRPC that fanouts requests to
// given exemplarspb.Exemplars.
type Proxy struct {
	logger         log.Logger
	exemplars      func() []*exemplarspb.ExemplarStore
	selectorLabels labels.Labels
}

// RegisterExemplarsServer register exemplars server.
func RegisterExemplarsServer(exemplarsSrv exemplarspb.ExemplarsServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		exemplarspb.RegisterExemplarsServer(s, exemplarsSrv)
	}
}

// NewProxy return new exemplars.Proxy.
func NewProxy(logger log.Logger, exemplars func() []*exemplarspb.ExemplarStore, selectorLabels labels.Labels) *Proxy {
	return &Proxy{
		logger:         logger,
		exemplars:      exemplars,
		selectorLabels: selectorLabels,
	}
}

type exemplarsStream struct {
	client  exemplarspb.ExemplarsClient
	request *exemplarspb.ExemplarsRequest
	channel chan<- *exemplarspb.ExemplarData
	server  exemplarspb.Exemplars_ExemplarsServer
}

func (s *Proxy) Exemplars(req *exemplarspb.ExemplarsRequest, srv exemplarspb.Exemplars_ExemplarsServer) error {
	span, ctx := tracing.StartSpan(srv.Context(), "proxy_exemplars")
	defer span.Finish()

	expr, err := parser.ParseExpr(req.Query)
	if err != nil {
		return err
	}

	match, selectors := selectorsMatchesExternalLabels(parser.ExtractSelectors(expr), s.selectorLabels)

	// There is no matched selectors for this thanos query.
	if !match {
		return nil
	}

	if len(selectors) == 0 {
		return status.Error(codes.InvalidArgument, errors.New("no matchers specified (excluding external labels)").Error())
	}

	var (
		g, gctx   = errgroup.WithContext(ctx)
		respChan  = make(chan *exemplarspb.ExemplarData, 10)
		exemplars []*exemplarspb.ExemplarData
	)

	queryParts := make([]string, 0)
	labelMatchers := make([]string, 0)
	for _, st := range s.exemplars() {
		queryParts = queryParts[:0]

	Matchers:
		for _, matchers := range selectors {
			matcherSet := make(map[string]struct{})
			for _, m := range matchers {
				for _, ls := range st.LabelSets {
					if lv := ls.Get(m.Name); lv != "" {
						if !m.Matches(lv) {
							continue Matchers
						} else {
							// If the current matcher matches one external label,
							// we don't add it to the current metric selector
							// as Prometheus' Exemplars API cannot handle external labels.
							continue
						}
					}
					matcherSet[m.String()] = struct{}{}
				}
			}

			labelMatchers = labelMatchers[:0]
			for m := range matcherSet {
				labelMatchers = append(labelMatchers, m)
			}

			queryParts = append(queryParts, "{"+strings.Join(labelMatchers, ", ")+"}")
		}

		// No matchers match this store.
		if len(queryParts) == 0 {
			continue
		}

		// Construct the query by concatenating metric selectors with '+'.
		// We cannot preserve the original query info, but the returned
		// results are the same.
		query := strings.Join(queryParts, "+")
		r := &exemplarspb.ExemplarsRequest{
			Start:                   req.Start,
			End:                     req.End,
			Query:                   query,
			PartialResponseStrategy: req.PartialResponseStrategy,
		}

		es := &exemplarsStream{

			client:  st.ExemplarsClient,
			request: r,
			channel: respChan,
			server:  srv,
		}
		g.Go(func() error { return es.receive(gctx) })
	}

	go func() {
		_ = g.Wait()
		close(respChan)
	}()

	for resp := range respChan {
		exemplars = append(exemplars, resp)
	}

	if err := g.Wait(); err != nil {
		level.Error(s.logger).Log("err", err)
		return err
	}

	for _, e := range exemplars {
		tracing.DoInSpan(srv.Context(), "send_exemplars_response", func(_ context.Context) {
			err = srv.Send(exemplarspb.NewExemplarsResponse(e))
		})
		if err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send exemplars response").Error())
		}
	}

	return nil
}

func (stream *exemplarsStream) receive(ctx context.Context) error {
	exemplars, err := stream.client.Exemplars(ctx, stream.request)
	if err != nil {
		err = errors.Wrapf(err, "fetching exemplars from exemplars client %v", stream.client)

		if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
			return err
		}

		if serr := stream.server.Send(exemplarspb.NewWarningExemplarsResponse(err)); serr != nil {
			return serr
		}
		// Not an error if response strategy is warning.
		return nil
	}

	for {
		exemplar, err := exemplars.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			err = errors.Wrapf(err, "receiving exemplars from exemplars client %v", stream.client)

			if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
				return err
			}

			if err := stream.server.Send(exemplarspb.NewWarningExemplarsResponse(err)); err != nil {
				return errors.Wrapf(err, "sending exemplars error to server %v", stream.server)
			}
			// Not an error if response strategy is warning.
			return nil
		}

		if w := exemplar.GetWarning(); w != "" {
			if err := stream.server.Send(exemplarspb.NewWarningExemplarsResponse(errors.New(w))); err != nil {
				return errors.Wrapf(err, "sending exemplars warning to server %v", stream.server)
			}
			continue
		}

		select {
		case stream.channel <- exemplar.GetData():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// matchesExternalLabels returns false if given matchers are not matching external labels.
// If true, matchesExternalLabels also returns Prometheus matchers without those matching external labels.
func matchesExternalLabels(ms []*labels.Matcher, externalLabels labels.Labels) (bool, []*labels.Matcher) {
	if externalLabels.IsEmpty() {
		return true, ms
	}

	var newMatchers []*labels.Matcher
	for i, tm := range ms {
		// Validate all matchers.
		extValue := externalLabels.Get(tm.Name)
		if extValue == "" {
			// Agnostic to external labels.
			ms = append(ms[:i], ms[i:]...)
			newMatchers = append(newMatchers, tm)
			continue
		}

		if !tm.Matches(extValue) {
			// External label does not match. This should not happen - it should be filtered out on query node,
			// but let's do that anyway here.
			return false, nil
		}
	}
	return true, newMatchers
}
