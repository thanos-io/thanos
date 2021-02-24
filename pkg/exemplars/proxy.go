// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"context"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Proxy implements exemplarspb.Exemplars gRPC that fanouts requests to
// given exemplarspb.Exemplars and de-duplication on the way.
type Proxy struct {
	logger    log.Logger
	exemplars func() []exemplarspb.ExemplarsClient
}

// RegisterExemplarsServer register exemplars server.
func RegisterExemplarsServer(exemplarsSrv exemplarspb.ExemplarsServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		exemplarspb.RegisterExemplarsServer(s, exemplarsSrv)
	}
}

// NewProxy return new exemplars.Proxy.
func NewProxy(logger log.Logger, exemplars func() []exemplarspb.ExemplarsClient) *Proxy {
	return &Proxy{
		logger:    logger,
		exemplars: exemplars,
	}
}

type exemplarsStream struct {
	client  exemplarspb.ExemplarsClient
	request *exemplarspb.ExemplarsRequest
	channel chan<- *exemplarspb.ExemplarData
	server  exemplarspb.Exemplars_ExemplarsServer
}

func (s *Proxy) Exemplars(req *exemplarspb.ExemplarsRequest, srv exemplarspb.Exemplars_ExemplarsServer) error {
	var (
		g, gctx   = errgroup.WithContext(srv.Context())
		respChan  = make(chan *exemplarspb.ExemplarData, 10)
		exemplars []*exemplarspb.ExemplarData
	)

	for _, exemplarsClient := range s.exemplars() {
		es := &exemplarsStream{
			client:  exemplarsClient,
			request: req,
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
		if err := srv.Send(exemplarspb.NewExemplarsResponse(e)); err != nil {
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

			continue
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
