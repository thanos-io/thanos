package query

import (
	"context"
	"sort"

	"github.com/go-kit/kit/log"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

var optCtxKey = struct{}{}

// PartialErrReporter allows to report partial errors. Partial error occurs when only part of the results are ready and
// another is not available because of the failure. We still want to return partial result, but with some notification.
// NOTE: It is required to be thread-safe.
type PartialErrReporter func(error)

// Option specifies change in the opts struct.
type Option func(*opts)

type opts struct {
	deduplicate        bool
	partialErrReporter PartialErrReporter
}

func defaultOpts() *opts {
	return &opts{
		deduplicate:        false,
		partialErrReporter: func(error) {},
	}
}

// WithDeduplication returns option that sets deduplication flag to true.
func WithDeduplication() Option {
	return func(o *opts) {
		o.deduplicate = true
	}
}

// WithPartialErrReporter return option that sets the given PartialErrReporter to opts.
func WithPartialErrReporter(errReporter PartialErrReporter) Option {
	return func(o *opts) {
		o.partialErrReporter = errReporter
	}
}

// ContextWithOpts returns context with given options.
// NOTE: This is the only way we can pass additional arguments to our custom querier which is hidden behind standard Prometheus
// promql engine.
func ContextWithOpts(ctx context.Context, opts ...Option) context.Context {
	o := defaultOpts()
	for _, opt := range opts {
		opt(o)
	}

	return context.WithValue(ctx, optCtxKey, o)
}

func optsFromContext(ctx context.Context) *opts {
	o, ok := ctx.Value(optCtxKey).(*opts)
	if !ok {
		return defaultOpts()
	}

	return o
}

// Queryable allows to open a querier against proxy store API.
type Queryable struct {
	logger       log.Logger
	replicaLabel string
	proxy        storepb.StoreServer
}

// NewQueryable creates implementation of promql.Queryable that fetches data from the proxy store API endpoints.
// All data retrieved from it will be deduplicated along the replicaLabel by default.
func NewQueryable(logger log.Logger, proxy storepb.StoreServer, replicaLabel string) *Queryable {
	return &Queryable{
		logger:       logger,
		replicaLabel: replicaLabel,
		proxy:        proxy,
	}
}

// Querier returns a new storage querier against the underlying proxy store API.
func (q *Queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(ctx, q.logger, mint, maxt, q.replicaLabel, q.proxy), nil
}

type querier struct {
	logger       log.Logger
	ctx          context.Context
	cancel       func()
	mint, maxt   int64
	replicaLabel string

	proxy storepb.StoreServer
}

// newQuerier creates implementation of storage.Querier that fetches data from the proxy
// store API endpoints.
func newQuerier(
	ctx context.Context,
	logger log.Logger,
	mint, maxt int64,
	replicaLabel string,
	proxy storepb.StoreServer,
) *querier {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &querier{
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		mint:         mint,
		maxt:         maxt,
		replicaLabel: replicaLabel,
		proxy:        proxy,
	}
}

func (q *querier) isDedupEnabled(o *opts) bool {
	return o.deduplicate && q.replicaLabel != ""
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

func (q *querier) Select(ms ...*labels.Matcher) (storage.SeriesSet, error) {
	opts := optsFromContext(q.ctx)

	span, ctx := tracing.StartSpan(q.ctx, "querier_select")
	defer span.Finish()

	sms, err := translateMatchers(ms...)
	if err != nil {
		return nil, errors.Wrap(err, "convert matchers")
	}

	resp := &seriesServer{ctx: ctx}
	if err := q.proxy.Series(&storepb.SeriesRequest{
		MinTime:            q.mint,
		MaxTime:            q.maxt,
		Matchers:           sms,
		MaxAggregateWindow: 0, // we always query raw data for now.
		Aggregates:         []storepb.Aggr{storepb.Aggr_RAW},
	}, resp); err != nil {
		return nil, errors.Wrap(err, "proxy Series()")
	}

	for _, w := range resp.warnings {
		opts.partialErrReporter(errors.New(w))
	}

	if q.isDedupEnabled(opts) {
		// TODO(fabxc): this could potentially pushed further down into the store API
		// to make true streaming possible.
		sortDedupLabels(resp.seriesSet, q.replicaLabel)
	}

	set := promSeriesSet{
		mint: q.mint,
		maxt: q.maxt,
		set:  newStoreSeriesSet(resp.seriesSet),
	}

	if !q.isDedupEnabled(opts) {
		// Return data without any deduplication.
		return set, nil
	}
	// The merged series set assembles all potentially-overlapping time ranges
	// of the same series into a single one. The series are ordered so that equal series
	// from different replicas are sequential. We can now deduplicate those.
	return newDedupSeriesSet(set, q.replicaLabel), nil
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

func (q *querier) LabelValues(name string) ([]string, error) {
	opts := optsFromContext(q.ctx)

	span, ctx := tracing.StartSpan(q.ctx, "querier_label_values")
	defer span.Finish()

	resp, err := q.proxy.LabelValues(ctx, &storepb.LabelValuesRequest{Label: name})
	if err != nil {
		return nil, errors.Wrap(err, "proxy LabelValues()")
	}

	for _, w := range resp.Warnings {
		opts.partialErrReporter(errors.New(w))
	}

	return resp.Values, nil
}

func (q *querier) Close() error {
	q.cancel()
	return nil
}
