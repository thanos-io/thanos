package query

import (
	"context"
	"sort"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type selectOnlyQuerier struct {
	ctx          context.Context
	logger       log.Logger
	cancel       func()
	mint, maxt   int64
	replicaLabel string
	proxy        storepb.StoreServer
	opts         Options
	warningReporter warningReporter
}

// newSelectOnlyQuerier creates implementation of storage.Querier.Select method that fetches data from the proxy
// store API endpoints.
func newSelectOnlyQuerier(
	ctx context.Context,
	mint, maxt int64,
	replicaLabel string,
	proxy storepb.StoreServer,
	warningReporter warningReporter,
	opts Options,
) *selectOnlyQuerier {
	ctx, cancel := context.WithCancel(ctx)
	return &selectOnlyQuerier{
		ctx:          ctx,
		cancel:       cancel,
		mint:         mint,
		maxt:         maxt,
		replicaLabel: replicaLabel,
		proxy:        proxy,
		opts:         opts,
		warningReporter: warningReporter,
	}
}

func (q *selectOnlyQuerier) isDedupEnabled() bool {
	return q.opts.Deduplicate && q.replicaLabel != ""
}

// SeriesServer is a naive in-mem implementation of gRPC client for
// streaming gRPC method that aggregate everything in memory.
type SeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer
	ctx context.Context

	seriesSet []storepb.Series
	warnings  []string
}

func NewSeriesServer(ctx context.Context) *SeriesServer {
	return &SeriesServer{ctx: ctx}
}

func (s *SeriesServer) Response() ([]storepb.Series, []string) {
	return s.seriesSet, s.warnings
}

func (s *SeriesServer) Send(r *storepb.SeriesResponse) error {
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

func (s *SeriesServer) Context() context.Context {
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
	if f == "sum" || strings.HasPrefix(f, "sum_") {
		return []storepb.Aggr{storepb.Aggr_SUM}, resAggrSum
	}
	if f == "increase" || f == "rate" {
		return []storepb.Aggr{storepb.Aggr_COUNTER}, resAggrCounter
	}
	// In the default case, we retrieve count and sum to compute an average.
	return []storepb.Aggr{storepb.Aggr_COUNT, storepb.Aggr_SUM}, resAggrAvg
}

func (q *selectOnlyQuerier) Select(params *storage.SelectParams, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	span, ctx := tracing.StartSpan(q.ctx, "querier_select")
	defer span.Finish()

	sms, err := TranslateMatchers(ms...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "convert matchers")
	}

	queryAggrs, resAggr := aggrsFromFunc(params.Func)

	resp := &SeriesServer{ctx: ctx}
	if err := q.proxy.Series(&storepb.SeriesRequest{
		MinTime:                 q.mint,
		MaxTime:                 q.maxt,
		Matchers:                sms,
		MaxResolutionWindow:     q.opts.Resolution.Window,
		Resolution:              q.opts.Resolution,
		Aggregates:              queryAggrs,
		PartialResponseDisabled: q.opts.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT,
		PartialResponseStrategy: q.opts.PartialResponseStrategy,
	}, resp); err != nil {
		return nil, nil, errors.Wrap(err, "proxy Series()")
	}

	ss, warns := resp.Response()
	for _, w := range warns {
		q.warningReporter(errors.New(w))
	}

	if !q.isDedupEnabled() {
		// Return data without any deduplication.
		return promSeriesSet{
			mint: q.mint,
			maxt: q.maxt,
			set:  NewStoreSeriesSet(ss),
			aggr: resAggr,
		}, nil, nil
	}

	// TODO(fabxc): this could potentially pushed further down into the store API
	// to make true streaming possible.
	sortDedupLabels(ss, q.replicaLabel)

	set := promSeriesSet{
		mint: q.mint,
		maxt: q.maxt,
		set:  NewStoreSeriesSet(ss),
		aggr: resAggr,
	}

	// The merged series set assembles all potentially-overlapping time ranges
	// of the same series into a single one. The series are ordered so that equal series
	// from different replicas are sequential. We can now deduplicate those.
	return newDedupSeriesSet(set, q.replicaLabel), nil, nil
}

// sortDedupLabels resorts the set so that the same series with different replica
// labels are coming right after each other.
func sortDedupLabels(set []storepb.Series, replicaLabel string) {
	for _, s := range set {
		// Move the replica label to the very end.
		sort.Slice(s.Labels, func(i, j int) bool {
			if s.Labels[i].Name == replicaLabel {
				return false
			}
			if s.Labels[j].Name == replicaLabel {
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

func (q *selectOnlyQuerier) LabelValues(name string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (q *selectOnlyQuerier) LabelNames() ([]string, error) {
	return nil, errors.New("not implemented")
}

func (q *selectOnlyQuerier) Close() error {
	q.cancel()
	return nil
}
