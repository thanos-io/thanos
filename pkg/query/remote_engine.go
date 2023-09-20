// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"io"
	"math"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-io/promql-engine/api"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// Opts are the options for a PromQL query.
type Opts struct {
	AutoDownsample        bool
	ReplicaLabels         []string
	Timeout               time.Duration
	EnablePartialResponse bool
}

// Client is a query client that executes PromQL queries.
type Client struct {
	querypb.QueryClient
	address   string
	tsdbInfos infopb.TSDBInfos
}

// NewClient creates a new Client.
func NewClient(queryClient querypb.QueryClient, address string, tsdbInfos infopb.TSDBInfos) Client {
	return Client{
		QueryClient: queryClient,
		address:     address,
		tsdbInfos:   tsdbInfos,
	}
}

func (c Client) GetAddress() string { return c.address }

func (c Client) LabelSets() []labels.Labels {
	return c.tsdbInfos.LabelSets()
}

type remoteEndpoints struct {
	logger     log.Logger
	getClients func() []Client
	opts       Opts
}

func NewRemoteEndpoints(logger log.Logger, getClients func() []Client, opts Opts) api.RemoteEndpoints {
	return remoteEndpoints{
		logger:     logger,
		getClients: getClients,
		opts:       opts,
	}
}

func (r remoteEndpoints) Engines() []api.RemoteEngine {
	clients := r.getClients()
	engines := make([]api.RemoteEngine, len(clients))
	for i := range clients {
		engines[i] = newRemoteEngine(r.logger, clients[i], r.opts)
	}
	return engines
}

type remoteEngine struct {
	opts   Opts
	logger log.Logger

	client Client

	mintOnce      sync.Once
	mint          int64
	maxtOnce      sync.Once
	maxt          int64
	labelSetsOnce sync.Once
	labelSets     []labels.Labels
}

func newRemoteEngine(logger log.Logger, queryClient Client, opts Opts) api.RemoteEngine {
	return &remoteEngine{
		logger: logger,
		client: queryClient,
		opts:   opts,
	}
}

// MinT returns the minimum timestamp that is safe to query in the remote engine.
// In order to calculate it, we find the highest min time for each label set, and we return
// the lowest of those values.
// Calculating the MinT this way makes remote queries resilient to cases where one tsdb replica would delete
// a block due to retention before other replicas did the same.
// See https://github.com/thanos-io/promql-engine/issues/187.
func (r *remoteEngine) MinT() int64 {
	r.mintOnce.Do(func() {
		var (
			hashBuf               = make([]byte, 0, 128)
			highestMintByLabelSet = make(map[uint64]int64)
		)
		for _, lset := range r.infosWithoutReplicaLabels() {
			key, _ := labelpb.ZLabelsToPromLabels(lset.Labels.Labels).HashWithoutLabels(hashBuf)
			lsetMinT, ok := highestMintByLabelSet[key]
			if !ok {
				highestMintByLabelSet[key] = lset.MinTime
				continue
			}

			if lset.MinTime > lsetMinT {
				highestMintByLabelSet[key] = lset.MinTime
			}
		}

		var mint int64 = math.MaxInt64
		for _, m := range highestMintByLabelSet {
			if m < mint {
				mint = m
			}
		}
		r.mint = mint
	})

	return r.mint
}

func (r *remoteEngine) MaxT() int64 {
	r.maxtOnce.Do(func() {
		r.maxt = r.client.tsdbInfos.MaxT()
	})
	return r.maxt
}

func (r *remoteEngine) LabelSets() []labels.Labels {
	r.labelSetsOnce.Do(func() {
		r.labelSets = r.infosWithoutReplicaLabels().LabelSets()
	})
	return r.labelSets
}

func (r *remoteEngine) infosWithoutReplicaLabels() infopb.TSDBInfos {
	replicaLabelSet := make(map[string]struct{})
	for _, lbl := range r.opts.ReplicaLabels {
		replicaLabelSet[lbl] = struct{}{}
	}

	// Strip replica labels from the result.
	infos := make(infopb.TSDBInfos, 0, len(r.client.tsdbInfos))
	var builder labels.ScratchBuilder
	for _, info := range r.client.tsdbInfos {
		builder.Reset()
		for _, lbl := range info.Labels.Labels {
			if _, ok := replicaLabelSet[lbl.Name]; ok {
				continue
			}
			builder.Add(lbl.Name, lbl.Value)
		}
		infos = append(infos, infopb.NewTSDBInfo(
			info.MinTime,
			info.MaxTime,
			labelpb.ZLabelsFromPromLabels(builder.Labels())),
		)
	}

	return infos
}

func (r *remoteEngine) NewRangeQuery(_ context.Context, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return &remoteQuery{
		logger: r.logger,
		client: r.client,
		opts:   r.opts,

		qs:       qs,
		start:    start,
		end:      end,
		interval: interval,
	}, nil
}

type remoteQuery struct {
	logger log.Logger
	client Client
	opts   Opts

	qs       string
	start    time.Time
	end      time.Time
	interval time.Duration

	cancel context.CancelFunc
}

func (r *remoteQuery) Exec(ctx context.Context) *promql.Result {
	start := time.Now()

	qctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	defer cancel()

	var maxResolution int64
	if r.opts.AutoDownsample {
		maxResolution = int64(r.interval.Seconds() / 5)
	}

	request := &querypb.QueryRangeRequest{
		Query:                 r.qs,
		StartTimeSeconds:      r.start.Unix(),
		EndTimeSeconds:        r.end.Unix(),
		IntervalSeconds:       int64(r.interval.Seconds()),
		TimeoutSeconds:        int64(r.opts.Timeout.Seconds()),
		EnablePartialResponse: r.opts.EnablePartialResponse,
		// TODO (fpetkovski): Allow specifying these parameters at query time.
		// This will likely require a change in the remote engine interface.
		ReplicaLabels:        r.opts.ReplicaLabels,
		MaxResolutionSeconds: maxResolution,
		EnableDedup:          true,
	}
	qry, err := r.client.QueryRange(qctx, request)
	if err != nil {
		return &promql.Result{Err: err}
	}

	result := make(promql.Matrix, 0)
	for {
		msg, err := qry.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return &promql.Result{Err: err}
		}

		if warn := msg.GetWarnings(); warn != "" {
			return &promql.Result{Err: errors.New(warn)}
		}

		ts := msg.GetTimeseries()
		if ts == nil {
			continue
		}
		series := promql.Series{
			Metric:     labelpb.ZLabelsToPromLabels(ts.Labels),
			Floats:     make([]promql.FPoint, 0, len(ts.Samples)),
			Histograms: make([]promql.HPoint, 0, len(ts.Histograms)),
		}
		for _, s := range ts.Samples {
			series.Floats = append(series.Floats, promql.FPoint{
				T: s.Timestamp,
				F: s.Value,
			})
		}
		for _, hp := range ts.Histograms {
			series.Histograms = append(series.Histograms, promql.HPoint{
				T: hp.Timestamp,
				H: prompb.FloatHistogramProtoToFloatHistogram(hp),
			})
		}
		result = append(result, series)
	}
	level.Debug(r.logger).Log("Executed query", "query", r.qs, "time", time.Since(start))

	return &promql.Result{Value: result}
}

func (r *remoteQuery) Close() { r.Cancel() }

func (r *remoteQuery) Statement() parser.Statement { return nil }

func (r *remoteQuery) Stats() *stats.Statistics { return nil }

func (r *remoteQuery) String() string { return r.qs }

func (r *remoteQuery) Cancel() {
	if r.cancel != nil {
		r.cancel()
	}
}
