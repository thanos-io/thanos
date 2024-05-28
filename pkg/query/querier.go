// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type seriesStatsReporter func(seriesStats storepb.SeriesStatsCounter)

var NoopSeriesStatsReporter seriesStatsReporter = func(_ storepb.SeriesStatsCounter) {}

func NewAggregateStatsReporter(stats *[]storepb.SeriesStatsCounter) seriesStatsReporter {
	var mutex sync.Mutex
	return func(s storepb.SeriesStatsCounter) {
		mutex.Lock()
		defer mutex.Unlock()
		*stats = append(*stats, s)
	}
}

// QueryableCreator returns implementation of promql.Queryable that fetches data from the proxy store API endpoints.
// If deduplication is enabled, all data retrieved from it will be deduplicated along all replicaLabels by default.
// When the replicaLabels argument is not empty it overwrites the global replicaLabels flag. This allows specifying
// replicaLabels at query time.
// maxResolutionMillis controls downsampling resolution that is allowed (specified in milliseconds).
// partialResponse controls `partialResponseDisabled` option of StoreAPI and partial response behavior of proxy.
type QueryableCreator func(
	deduplicate bool,
	replicaLabels []string,
	storeDebugMatchers [][]*labels.Matcher,
	maxResolutionMillis int64,
	partialResponse,
	skipChunks bool,
	shardInfo *storepb.ShardInfo,
	seriesStatsReporter seriesStatsReporter,
) storage.Queryable

// NewQueryableCreator creates QueryableCreator.
// NOTE(bwplotka): Proxy assumes to be replica_aware, see thanos.store.info.StoreInfo.replica_aware field.
func NewQueryableCreator(
	logger log.Logger,
	reg prometheus.Registerer,
	proxy storepb.StoreServer,
	maxConcurrentSelects int,
	selectTimeout time.Duration,
) QueryableCreator {
	gf := gate.NewGateFactory(extprom.WrapRegistererWithPrefix("concurrent_selects_", reg), maxConcurrentSelects, gate.Selects)

	return func(
		deduplicate bool,
		replicaLabels []string,
		storeDebugMatchers [][]*labels.Matcher,
		maxResolutionMillis int64,
		partialResponse,
		skipChunks bool,
		shardInfo *storepb.ShardInfo,
		seriesStatsReporter seriesStatsReporter,
	) storage.Queryable {
		return &queryable{
			logger:              logger,
			replicaLabels:       replicaLabels,
			storeDebugMatchers:  storeDebugMatchers,
			proxy:               proxy,
			deduplicate:         deduplicate,
			maxResolutionMillis: maxResolutionMillis,
			partialResponse:     partialResponse,
			skipChunks:          skipChunks,
			gateProviderFn: func() gate.Gate {
				return gf.New()
			},
			maxConcurrentSelects: maxConcurrentSelects,
			selectTimeout:        selectTimeout,
			shardInfo:            shardInfo,
			seriesStatsReporter:  seriesStatsReporter,
		}
	}
}

type queryable struct {
	logger               log.Logger
	replicaLabels        []string
	storeDebugMatchers   [][]*labels.Matcher
	proxy                storepb.StoreServer
	deduplicate          bool
	maxResolutionMillis  int64
	partialResponse      bool
	skipChunks           bool
	gateProviderFn       func() gate.Gate
	maxConcurrentSelects int
	selectTimeout        time.Duration
	shardInfo            *storepb.ShardInfo
	seriesStatsReporter  seriesStatsReporter
}

// Querier returns a new storage querier against the underlying proxy store API.
func (q *queryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return newQuerier(q.logger, mint, maxt, q.replicaLabels, q.storeDebugMatchers, q.proxy, q.deduplicate, q.maxResolutionMillis, q.partialResponse, q.skipChunks, q.gateProviderFn(), q.selectTimeout, q.shardInfo, q.seriesStatsReporter), nil
}

type querier struct {
	logger                  log.Logger
	mint, maxt              int64
	replicaLabels           []string
	storeDebugMatchers      [][]*labels.Matcher
	proxy                   storepb.StoreServer
	deduplicate             bool
	maxResolutionMillis     int64
	partialResponseStrategy storepb.PartialResponseStrategy
	skipChunks              bool
	selectGate              gate.Gate
	selectTimeout           time.Duration
	shardInfo               *storepb.ShardInfo
	seriesStatsReporter     seriesStatsReporter
}

// newQuerier creates implementation of storage.Querier that fetches data from the proxy
// store API endpoints.
func newQuerier(
	logger log.Logger,
	mint,
	maxt int64,
	replicaLabels []string,
	storeDebugMatchers [][]*labels.Matcher,
	proxy storepb.StoreServer,
	deduplicate bool,
	maxResolutionMillis int64,
	partialResponse,
	skipChunks bool,
	selectGate gate.Gate,
	selectTimeout time.Duration,
	shardInfo *storepb.ShardInfo,
	seriesStatsReporter seriesStatsReporter,
) *querier {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	rl := make(map[string]struct{})
	for _, replicaLabel := range replicaLabels {
		rl[replicaLabel] = struct{}{}
	}

	partialResponseStrategy := storepb.PartialResponseStrategy_ABORT
	if partialResponse {
		partialResponseStrategy = storepb.PartialResponseStrategy_WARN
	}
	return &querier{
		logger:        logger,
		selectGate:    selectGate,
		selectTimeout: selectTimeout,

		mint:                    mint,
		maxt:                    maxt,
		replicaLabels:           replicaLabels,
		storeDebugMatchers:      storeDebugMatchers,
		proxy:                   proxy,
		deduplicate:             deduplicate,
		maxResolutionMillis:     maxResolutionMillis,
		partialResponseStrategy: partialResponseStrategy,
		skipChunks:              skipChunks,
		shardInfo:               shardInfo,
		seriesStatsReporter:     seriesStatsReporter,
	}
}

func (q *querier) isDedupEnabled() bool {
	return q.deduplicate && len(q.replicaLabels) > 0
}

type seriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer
	ctx context.Context

	seriesSet      []storepb.Series
	seriesSetStats storepb.SeriesStatsCounter
	warnings       annotations.Annotations
}

func (s *seriesServer) Send(r *storepb.SeriesResponse) error {
	if r.GetWarning() != "" {
		s.warnings.Add(errors.New(r.GetWarning()))
		return nil
	}

	if r.GetSeries() != nil {
		s.seriesSet = append(s.seriesSet, *r.GetSeries())
		s.seriesSetStats.Count(r.GetSeries())
		return nil
	}

	// Unsupported field, skip.
	return nil
}

func (s *seriesServer) Context() context.Context {
	return s.ctx
}

// aggrsFromFunc infers aggregates of the underlying data based on the wrapping
// function of a series selection.
func aggrsFromFunc(f string) []storepb.Aggr {
	if f == "min" || strings.HasPrefix(f, "min_") {
		return []storepb.Aggr{storepb.Aggr_MIN}
	}
	if f == "max" || strings.HasPrefix(f, "max_") {
		return []storepb.Aggr{storepb.Aggr_MAX}
	}
	if f == "count" || strings.HasPrefix(f, "count_") {
		return []storepb.Aggr{storepb.Aggr_COUNT}
	}
	// f == "sum" falls through here since we want the actual samples.
	if strings.HasPrefix(f, "sum_") {
		return []storepb.Aggr{storepb.Aggr_SUM}
	}
	if f == "increase" || f == "rate" || f == "irate" || f == "resets" {
		return []storepb.Aggr{storepb.Aggr_COUNTER}
	}
	// In the default case, we retrieve count and sum to compute an average.
	return []storepb.Aggr{storepb.Aggr_COUNT, storepb.Aggr_SUM}
}

func (q *querier) Select(ctx context.Context, _ bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.SeriesSet {
	if hints == nil {
		hints = &storage.SelectHints{
			Start: q.mint,
			End:   q.maxt,
		}
	} else {
		// NOTE(GiedriusS): need to make a copy here
		// because the PromQL engine sorts these and
		// we later on call String() the whole request (including this slice).
		grouping := make([]string, 0, len(hints.Grouping))
		grouping = append(grouping, hints.Grouping...)
		hints.Grouping = grouping
	}

	matchers := make([]string, len(ms))
	for i, m := range ms {
		matchers[i] = m.String()
	}
	tenant := ctx.Value(tenancy.TenantKey)
	// The context gets canceled as soon as query evaluation is completed by the engine.
	// We want to prevent this from happening for the async store API calls we make while preserving tracing context.
	// TODO(bwplotka): Does the above still is true? It feels weird to leave unfinished calls behind query API.
	ctx = tracing.CopyTraceContext(context.Background(), ctx)
	ctx = context.WithValue(ctx, tenancy.TenantKey, tenant)
	ctx, cancel := context.WithTimeout(ctx, q.selectTimeout)
	span, ctx := tracing.StartSpan(ctx, "querier_select", opentracing.Tags{
		"minTime":  hints.Start,
		"maxTime":  hints.End,
		"matchers": "{" + strings.Join(matchers, ",") + "}",
	})

	promise := make(chan storage.SeriesSet, 1)
	go func() {
		defer close(promise)

		var err error
		tracing.DoInSpan(ctx, "querier_select_gate_ismyturn", func(ctx context.Context) {
			err = q.selectGate.Start(ctx)
		})
		if err != nil {
			promise <- storage.ErrSeriesSet(errors.Wrap(err, "failed to wait for turn"))
			return
		}
		defer q.selectGate.Done()

		span, ctx := tracing.StartSpan(ctx, "querier_select_select_fn")
		defer span.Finish()

		set, stats, err := q.selectFn(ctx, hints, ms...)
		if err != nil {
			promise <- storage.ErrSeriesSet(err)
			return
		}
		q.seriesStatsReporter(stats)

		promise <- set
	}()

	return &lazySeriesSet{create: func() (storage.SeriesSet, bool) {
		defer cancel()
		defer span.Finish()

		// Only gets called once, for the first Next() call of the series set.
		set, ok := <-promise
		if !ok {
			return storage.ErrSeriesSet(errors.New("channel closed before a value received")), false
		}
		return set, set.Next()
	}}
}

func (q *querier) selectFn(ctx context.Context, hints *storage.SelectHints, ms ...*labels.Matcher) (storage.SeriesSet, storepb.SeriesStatsCounter, error) {
	sms, err := storepb.PromMatchersToMatchers(ms...)
	if err != nil {
		return nil, storepb.SeriesStatsCounter{}, errors.Wrap(err, "convert matchers")
	}

	aggrs := aggrsFromFunc(hints.Func)

	// TODO(bwplotka): Pass it using the SeriesRequest instead of relying on context.
	ctx = context.WithValue(ctx, store.StoreMatcherKey, q.storeDebugMatchers)

	// TODO(bwplotka): Use inprocess gRPC when we want to stream responses.
	// Currently streaming won't help due to nature of the both PromQL engine which
	// pulls all series before computations anyway.
	resp := &seriesServer{ctx: ctx}
	req := storepb.SeriesRequest{
		MinTime:                 hints.Start,
		MaxTime:                 hints.End,
		Matchers:                sms,
		MaxResolutionWindow:     q.maxResolutionMillis,
		Aggregates:              aggrs,
		ShardInfo:               q.shardInfo,
		PartialResponseStrategy: q.partialResponseStrategy,
		SkipChunks:              q.skipChunks,
	}
	if q.isDedupEnabled() {
		// Soft ask to sort without replica labels and push them at the end of labelset.
		req.WithoutReplicaLabels = q.replicaLabels
	}

	if err := q.proxy.Series(&req, resp); err != nil {
		return nil, storepb.SeriesStatsCounter{}, errors.Wrap(err, "proxy Series()")
	}
	warns := annotations.New().Merge(resp.warnings)

	if !q.isDedupEnabled() {
		return NewPromSeriesSet(
			newStoreSeriesSet(resp.seriesSet),
			q.mint,
			q.maxt,
			aggrs,
			warns,
		), resp.seriesSetStats, nil
	}

	// TODO(bwplotka): Move to deduplication on chunk level inside promSeriesSet, similar to what we have in dedup.NewDedupChunkMerger().
	// This however require big refactor, caring about correct AggrChunk to iterator conversion and counter reset apply.
	// For now we apply simple logic that splits potential overlapping chunks into separate replica series, so we can split the work.
	set := NewPromSeriesSet(
		dedup.NewOverlapSplit(newStoreSeriesSet(resp.seriesSet)),
		q.mint,
		q.maxt,
		aggrs,
		warns,
	)

	return dedup.NewSeriesSet(set, hints.Func), resp.seriesSetStats, nil
}

// LabelValues returns all potential values for a label name.
func (q *querier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	span, ctx := tracing.StartSpan(ctx, "querier_label_values")
	defer span.Finish()

	// TODO(bwplotka): Pass it using the SeriesRequest instead of relying on context.
	ctx = context.WithValue(ctx, store.StoreMatcherKey, q.storeDebugMatchers)

	pbMatchers, err := storepb.PromMatchersToMatchers(matchers...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "converting prom matchers to storepb matchers")
	}
	req := &storepb.LabelValuesRequest{
		Label:                   name,
		PartialResponseStrategy: q.partialResponseStrategy,
		Start:                   q.mint,
		End:                     q.maxt,
		Matchers:                pbMatchers,
	}

	if q.isDedupEnabled() {
		req.WithoutReplicaLabels = q.replicaLabels
	}

	resp, err := q.proxy.LabelValues(ctx, req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "proxy LabelValues()")
	}

	var warns annotations.Annotations
	for _, w := range resp.Warnings {
		warns.Add(errors.New(w))
	}

	return resp.Values, warns, nil
}

// LabelNames returns all the unique label names present in the block in sorted order constrained
// by the given matchers.
func (q *querier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	span, ctx := tracing.StartSpan(ctx, "querier_label_names")
	defer span.Finish()

	// TODO(bwplotka): Pass it using the SeriesRequest instead of relying on context.
	ctx = context.WithValue(ctx, store.StoreMatcherKey, q.storeDebugMatchers)

	pbMatchers, err := storepb.PromMatchersToMatchers(matchers...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "converting prom matchers to storepb matchers")
	}

	req := &storepb.LabelNamesRequest{
		PartialResponseStrategy: q.partialResponseStrategy,
		Start:                   q.mint,
		End:                     q.maxt,
		Matchers:                pbMatchers,
	}

	if q.isDedupEnabled() {
		req.WithoutReplicaLabels = q.replicaLabels
	}

	resp, err := q.proxy.LabelNames(ctx, req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "proxy LabelNames()")
	}

	var warns annotations.Annotations
	for _, w := range resp.Warnings {
		warns.Add(errors.New(w))
	}

	return resp.Names, warns, nil
}

func (q *querier) Close() error { return nil }
