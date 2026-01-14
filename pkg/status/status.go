// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package status

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/thanos/pkg/status/statuspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

var _ UnaryClient = &GRPCClient{}

// UnaryClient is a gRPC statuspb.Status client which expands the streaming status API. Useful for consumers that does not
// support streaming.
type UnaryClient interface {
	TSDBStatistics(context.Context, *statuspb.TSDBStatisticsRequest) (map[string]*statuspb.TSDBStatisticsEntry, annotations.Annotations, error)
}

type GRPCClient struct {
	proxy statuspb.StatusServer
}

func NewGRPCClient(ss statuspb.StatusServer) *GRPCClient {
	return &GRPCClient{
		proxy: ss,
	}
}

func (gc *GRPCClient) TSDBStatistics(ctx context.Context, req *statuspb.TSDBStatisticsRequest) (map[string]*statuspb.TSDBStatisticsEntry, annotations.Annotations, error) {
	span, ctx := tracing.StartSpan(ctx, "tsdb_statistics_grpc_request")
	defer span.Finish()

	srv := &tsdbStatisticsServer{
		ctx:            ctx,
		tsdbStatistics: map[string][]*statuspb.TSDBStatisticsEntry{},
	}
	if err := gc.proxy.TSDBStatistics(req, srv); err != nil {
		return nil, nil, errors.Wrap(err, "proxy TSDBStatistics")
	}

	// Collect and aggregate the results.
	tsdbStatistics := make(map[string]*statuspb.TSDBStatisticsEntry)
	for tenant, stats := range srv.tsdbStatistics {
		tenantStats, found := tsdbStatistics[tenant]
		if !found {
			tenantStats = &statuspb.TSDBStatisticsEntry{}
			tsdbStatistics[tenant] = tenantStats
		}

		for _, stat := range stats {
			tenantStats.Merge(stat)
		}
	}

	return tsdbStatistics, srv.warnings, nil
}

type tsdbStatisticsServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	statuspb.Status_TSDBStatisticsServer
	ctx context.Context

	mtx            sync.Mutex
	warnings       annotations.Annotations
	tsdbStatistics map[string][]*statuspb.TSDBStatisticsEntry
}

func (srv *tsdbStatisticsServer) Send(res *statuspb.TSDBStatisticsResponse) error {
	if res.GetWarning() != "" {
		srv.mtx.Lock()
		defer srv.mtx.Unlock()
		srv.warnings.Add(errors.New(res.GetWarning()))
		return nil
	}

	stats := res.GetStatistics()
	if stats == nil {
		return errors.New("no tsdb statistics")
	}

	srv.mtx.Lock()
	defer srv.mtx.Unlock()
	for tenant, tenantStats := range stats.Statistics {
		if _, found := srv.tsdbStatistics[tenant]; !found {
			srv.tsdbStatistics[tenant] = nil
		}
		srv.tsdbStatistics[tenant] = append(srv.tsdbStatistics[tenant], tenantStats)
	}

	return nil
}

func (srv *tsdbStatisticsServer) Context() context.Context {
	return srv.ctx
}

// RegisterStatusServer registers the status server.
func RegisterStatusServer(statusSrv statuspb.StatusServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		statuspb.RegisterStatusServer(s, statusSrv)
	}
}

// Server implements the corresponding protobuf interface to provide status
// information about the given component.
type Server struct {
	component            string
	tsdbStatisticsGetter TSDBStatisticsGetter
}

var _ = statuspb.StatusServer(&Server{})

// TSDBStatisticsGetter is an interface to retrieve TSDB statistics.
type TSDBStatisticsGetter interface {
	// TSDBStatistics returns the TSDB statistics matching the given label matchers against external labels.
	// When matchers is empty, it returns statistics for all TSDBs.
	TSDBStatistics(limit int, matchers []storepb.LabelMatcher) (map[string]tsdb.Stats, error)
}

type TSDBStatisticsGetterFunc func(int, []storepb.LabelMatcher) (map[string]tsdb.Stats, error)

func (f TSDBStatisticsGetterFunc) TSDBStatistics(limit int, matchers []storepb.LabelMatcher) (map[string]tsdb.Stats, error) {
	return f(limit, matchers)
}

// NewServer creates a new server instance for the given component
// and with the specified options.
func NewServer(
	component string,
	options ...ServerOptionFunc,
) *Server {
	srv := &Server{
		component: component,
		tsdbStatisticsGetter: TSDBStatisticsGetterFunc(func(int, []storepb.LabelMatcher) (map[string]tsdb.Stats, error) {
			return nil, errors.New("TSDB statistics not implemented")
		}),
	}

	for _, o := range options {
		o(srv)
	}

	return srv
}

// ServerOptionFunc represents a functional option to configure the status server.
type ServerOptionFunc func(*Server)

func WithTSDBStatisticsGetter(tsg TSDBStatisticsGetter) func(*Server) {
	return func(s *Server) {
		s.tsdbStatisticsGetter = tsg
	}
}

// TSDBStatistics implements the statuspb.StatusServer interface.
func (srv *Server) TSDBStatistics(r *statuspb.TSDBStatisticsRequest, s statuspb.Status_TSDBStatisticsServer) error {
	tsdbStats, err := srv.tsdbStatisticsGetter.TSDBStatistics(int(r.Limit), r.Matchers)
	if err != nil {
		return err
	}

	stats := map[string]*statuspb.TSDBStatisticsEntry{}
	for tenant, stat := range tsdbStats {
		tenantStats, found := stats[tenant]
		if !found {
			tenantStats = &statuspb.TSDBStatisticsEntry{}
			stats[tenant] = tenantStats
		}

		tenantStats.HeadStatistics.NumSeries += stat.NumSeries
		tenantStats.HeadStatistics.NumLabelPairs += int64(stat.IndexPostingStats.NumLabelPairs)
		//TODO: ChunkCount is retrieved from the prometheus_tsdb_head_chunks metric.

		if tenantStats.HeadStatistics.MinTime <= 0 || tenantStats.HeadStatistics.MinTime < stat.MinTime {
			tenantStats.HeadStatistics.MinTime = stat.MinTime
		}
		if tenantStats.HeadStatistics.MaxTime < stat.MaxTime {
			tenantStats.HeadStatistics.MaxTime = stat.MaxTime
		}

		tenantStats.SeriesCountByMetricName = toStatistic(stat.IndexPostingStats.CardinalityMetricsStats)
		tenantStats.LabelValueCountByLabelName = toStatistic(stat.IndexPostingStats.CardinalityLabelStats)
		tenantStats.MemoryInBytesByLabelName = toStatistic(stat.IndexPostingStats.LabelValueStats)
		tenantStats.SeriesCountByLabelValuePair = toStatistic(stat.IndexPostingStats.LabelValuePairsStats)
	}

	if err := s.Send(&statuspb.TSDBStatisticsResponse{
		Result: &statuspb.TSDBStatisticsResponse_Statistics{
			Statistics: &statuspb.TSDBStatistics{
				Statistics: stats,
			},
		},
	}); err != nil {
		return err
	}

	return nil
}

func toStatistic(stats []index.Stat) []statuspb.Statistic {
	ret := make([]statuspb.Statistic, len(stats))
	for i, v := range stats {
		ret[i] = statuspb.Statistic{
			Name:  v.Name,
			Value: v.Count,
		}
	}

	return ret
}
