// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"sort"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// QueryableCreator returns implementation of promql.Queryable that fetches data from the proxy store API endpoints.
// If deduplication is enabled, all data retrieved from it will be deduplicated along all replicaLabels by default.
// When the replicaLabels argument is not empty it overwrites the global replicaLabels flag. This allows specifying
// replicaLabels at query time.
// maxResolutionMillis controls downsampling resolution that is allowed (specified in milliseconds).
// partialResponse controls `partialResponseDisabled` option of StoreAPI and partial response behaviour of proxy.
type QueryableCreator func(deduplicate bool, replicaLabels []string, maxResolutionMillis int64, partialResponse, skipChunks bool) storage.Queryable

// NewQueryableCreator creates QueryableCreator.
func NewQueryableCreator(logger log.Logger, proxy storepb.StoreServer) QueryableCreator {
	return func(deduplicate bool, replicaLabels []string, maxResolutionMillis int64, partialResponse, skipChunks bool) storage.Queryable {
		return &queryable{
			logger:              logger,
			replicaLabels:       replicaLabels,
			proxy:               proxy,
			deduplicate:         deduplicate,
			maxResolutionMillis: maxResolutionMillis,
			partialResponse:     partialResponse,
			skipChunks:          skipChunks,
		}
	}
}

type queryable struct {
	logger              log.Logger
	replicaLabels       []string
	proxy               storepb.StoreServer
	deduplicate         bool
	maxResolutionMillis int64
	partialResponse     bool
	skipChunks          bool
}

// Querier returns a new storage querier against the underlying proxy store API.
func (q *queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(ctx, q.logger, mint, maxt, q.replicaLabels, q.proxy, q.deduplicate, int64(q.maxResolutionMillis), q.partialResponse, q.skipChunks), nil
}

type querier struct {
	ctx                 context.Context
	logger              log.Logger
	cancel              func()
	mint, maxt          int64
	replicaLabels       map[string]struct{}
	proxy               storepb.StoreServer
	deduplicate         bool
	maxResolutionMillis int64
	partialResponse     bool
	skipChunks          bool
}

// newQuerier creates implementation of storage.Querier that fetches data from the proxy
// store API endpoints.
func newQuerier(
	ctx context.Context,
	logger log.Logger,
	mint, maxt int64,
	replicaLabels []string,
	proxy storepb.StoreServer,
	deduplicate bool,
	maxResolutionMillis int64,
	partialResponse bool,
	skipChunks bool,
) *querier {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	ctx, cancel := context.WithCancel(ctx)

	rl := make(map[string]struct{})
	for _, replicaLabel := range replicaLabels {
		rl[replicaLabel] = struct{}{}
	}
	return &querier{
		ctx:                 ctx,
		logger:              logger,
		cancel:              cancel,
		mint:                mint,
		maxt:                maxt,
		replicaLabels:       rl,
		proxy:               proxy,
		deduplicate:         deduplicate,
		maxResolutionMillis: maxResolutionMillis,
		partialResponse:     partialResponse,
		skipChunks:          skipChunks,
	}
}

func (q *querier) isDedupEnabled() bool {
	return q.deduplicate && len(q.replicaLabels) > 0
}

type seriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer
	ctx context.Context

	seriesSet []storepb.Series
	warnings  []string
}

func (s *seriesServer) Send(r *storepb.SeriesResponse) error {
	if r.GetWarning() != "" {
		s.warnings = append(s.warnings, r.GetWarning())
		return nil
	}

	if r.GetSeries() == nil {
		return errors.New("no seriesSet")
	}
	s.seriesSet = append(s.seriesSet, *r.GetSeries())
	return nil
}

func (s *seriesServer) Context() context.Context {
	return s.ctx
}

type resAggr int

const (
	resAggrAvg resAggr = iota
	resAggrCount
	resAggrSum
	resAggrMin
	resAggrMax
	resAggrCounter
)

// aggrsFromFunc infers aggregates of the underlying data based on the wrapping
// function of a series selection.
func aggrsFromFunc(f string) ([]storepb.Aggr, resAggr) {
	if f == "min" || strings.HasPrefix(f, "min_") {
		return []storepb.Aggr{storepb.Aggr_MIN}, resAggrMin
	}
	if f == "max" || strings.HasPrefix(f, "max_") {
		return []storepb.Aggr{storepb.Aggr_MAX}, resAggrMax
	}
	if f == "count" || strings.HasPrefix(f, "count_") {
		return []storepb.Aggr{storepb.Aggr_COUNT}, resAggrCount
	}
	// f == "sum" falls through here since we want the actual samples.
	if strings.HasPrefix(f, "sum_") {
		return []storepb.Aggr{storepb.Aggr_SUM}, resAggrSum
	}
	if f == "increase" || f == "rate" {
		return []storepb.Aggr{storepb.Aggr_COUNTER}, resAggrCounter
	}
	// In the default case, we retrieve count and sum to compute an average.
	return []storepb.Aggr{storepb.Aggr_COUNT, storepb.Aggr_SUM}, resAggrAvg
}

func (q *querier) Select(params *storage.SelectParams, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	if params == nil {
		params = &storage.SelectParams{
			Start: q.mint,
			End:   q.maxt,
		}
	}

	matchers := make([]string, len(ms))
	for i, m := range ms {
		matchers[i] = m.String()
	}
	span, ctx := tracing.StartSpan(q.ctx, "querier_select", opentracing.Tags{
		"minTime":  params.Start,
		"maxTime":  params.End,
		"matchers": "{" + strings.Join(matchers, ",") + "}",
	})
	defer span.Finish()

	sms, err := translateMatchers(ms...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "convert matchers")
	}

	queryAggrs, resAggr := aggrsFromFunc(params.Func)

	resp := &seriesServer{ctx: ctx}
	if err := q.proxy.Series(&storepb.SeriesRequest{
		MinTime:                 params.Start,
		MaxTime:                 params.End,
		Matchers:                sms,
		MaxResolutionWindow:     q.maxResolutionMillis,
		Aggregates:              queryAggrs,
		PartialResponseDisabled: !q.partialResponse,
		SkipChunks:              q.skipChunks,
	}, resp); err != nil {
		return nil, nil, errors.Wrap(err, "proxy Series()")
	}

	var warns storage.Warnings
	for _, w := range resp.warnings {
		warns = append(warns, errors.New(w))
	}

	if !q.isDedupEnabled() {
		// Return data without any deduplication.
		return &promSeriesSet{
			mint: q.mint,
			maxt: q.maxt,
			set:  newStoreSeriesSet(resp.seriesSet),
			aggr: resAggr,
		}, warns, nil
	}

	// TODO(fabxc): this could potentially pushed further down into the store API
	// to make true streaming possible.
	sortDedupLabels(resp.seriesSet, q.replicaLabels)

	set := &promSeriesSet{
		mint: q.mint,
		maxt: q.maxt,
		set:  newStoreSeriesSet(resp.seriesSet),
		aggr: resAggr,
	}

	// The merged series set assembles all potentially-overlapping time ranges
	// of the same series into a single one. The series are ordered so that equal series
	// from different replicas are sequential. We can now deduplicate those.
	return newDedupSeriesSet(set, q.replicaLabels), warns, nil
}

// sortDedupLabels re-sorts the set so that the same series with different replica
// labels are coming right after each other.
func sortDedupLabels(set []storepb.Series, replicaLabels map[string]struct{}) {
	for _, s := range set {
		// Move the replica labels to the very end.
		sort.Slice(s.Labels, func(i, j int) bool {
			if _, ok := replicaLabels[s.Labels[i].Name]; ok {
				return false
			}
			if _, ok := replicaLabels[s.Labels[j].Name]; ok {
				return true
			}
			return s.Labels[i].Name < s.Labels[j].Name
		})
	}
	// With the re-ordered label sets, re-sorting all series aligns the same series
	// from different replicas sequentially.
	sort.Slice(set, func(i, j int) bool {
		return storepb.CompareLabels(set[i].Labels, set[j].Labels) < 0
	})
}

// LabelValues returns all potential values for a label name.
func (q *querier) LabelValues(name string) ([]string, storage.Warnings, error) {
	span, ctx := tracing.StartSpan(q.ctx, "querier_label_values")
	defer span.Finish()

	resp, err := q.proxy.LabelValues(ctx, &storepb.LabelValuesRequest{Label: name, PartialResponseDisabled: !q.partialResponse})
	if err != nil {
		return nil, nil, errors.Wrap(err, "proxy LabelValues()")
	}

	var warns storage.Warnings
	for _, w := range resp.Warnings {
		warns = append(warns, errors.New(w))
	}

	return resp.Values, warns, nil
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (q *querier) LabelNames() ([]string, storage.Warnings, error) {
	span, ctx := tracing.StartSpan(q.ctx, "querier_label_names")
	defer span.Finish()

	resp, err := q.proxy.LabelNames(ctx, &storepb.LabelNamesRequest{PartialResponseDisabled: !q.partialResponse})
	if err != nil {
		return nil, nil, errors.Wrap(err, "proxy LabelNames()")
	}

	var warns storage.Warnings
	for _, w := range resp.Warnings {
		warns = append(warns, errors.New(w))
	}

	return resp.Names, warns, nil
}

func (q *querier) Close() error {
	q.cancel()
	return nil
}
