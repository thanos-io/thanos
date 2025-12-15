// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package status

import (
	"context"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/thanos-io/thanos/pkg/status/statuspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// Proxy implements statuspb.Status gRPC that fanouts requests to given statuspb.Status servers.
type Proxy struct {
	statuspb.UnimplementedStatusServer

	logger        log.Logger
	statusClients func() []statuspb.StatusClient
}

// NewProxy returns new status.Proxy.
func NewProxy(logger log.Logger, statusClients func() []statuspb.StatusClient) *Proxy {
	return &Proxy{
		logger:        logger,
		statusClients: statusClients,
	}
}

// TSDBStatistics implements the statuspb.StatusServer interface.
func (s *Proxy) TSDBStatistics(req *statuspb.TSDBStatisticsRequest, srv statuspb.Status_TSDBStatisticsServer) error {
	span, ctx := tracing.StartSpan(srv.Context(), "proxy_tsdb_statistics")
	defer span.Finish()

	var (
		g, gctx    = errgroup.WithContext(ctx)
		respChan   = make(chan *statuspb.TSDBStatistics, 10)
		statistics []*statuspb.TSDBStatistics
		err        error
	)

	for _, client := range s.statusClients() {
		rs := &statsStream{
			client:  client,
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
		statistics = append(statistics, resp)
	}

	if err := g.Wait(); err != nil {
		level.Error(s.logger).Log("err", err)
		return err
	}

	for _, stat := range statistics {
		tracing.DoInSpan(srv.Context(), "send_tsdb_statistics_response", func(_ context.Context) {
			err = srv.Send(statuspb.NewTSDBStatisticsResponse(stat))
		})
		if err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send tsdb statistics response").Error())
		}
	}

	return nil
}

type statsStream struct {
	client  statuspb.StatusClient
	request *statuspb.TSDBStatisticsRequest
	channel chan<- *statuspb.TSDBStatistics
	server  statuspb.Status_TSDBStatisticsServer
}

func (stream *statsStream) receive(ctx context.Context) error {
	var (
		err         error
		statsClient statuspb.Status_TSDBStatisticsClient
	)

	tracing.DoInSpan(ctx, "receive_tsdb_statistics_stream_request", func(ctx context.Context) {
		statsClient, err = stream.client.TSDBStatistics(ctx, stream.request)
	})

	if err != nil {
		err = errors.Wrapf(err, "fetching tsdb statistics from status client %v", stream.client)

		if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
			return err
		}

		if serr := stream.server.Send(statuspb.NewWarningTSDBStatisticsResponse(err)); serr != nil {
			return serr
		}

		// Not an error if response strategy is warning.
		return nil
	}

	for {
		resp, err := statsClient.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			err = errors.Wrapf(err, "receiving tsdb statistics from status client %v", stream.client)

			if stream.request.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT {
				return err
			}

			if err := stream.server.Send(statuspb.NewWarningTSDBStatisticsResponse(err)); err != nil {
				return errors.Wrapf(err, "sending tsdb statistics error to server %v", stream.server)
			}

			// Not an error if response strategy is warning.
			return nil
		}

		if w := resp.GetWarning(); w != "" {
			if err := stream.server.Send(statuspb.NewWarningTSDBStatisticsResponse(errors.New(w))); err != nil {
				return errors.Wrapf(err, "sending tsdb statistics warning to server %v", stream.server)
			}
			continue
		}

		select {
		case stream.channel <- resp.GetStatistics():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
