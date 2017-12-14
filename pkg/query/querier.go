package query

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/go-kit/kit/log"

	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/strutil"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/sync/errgroup"
)

var optCtxKey = struct{}{}

// PartialErrReporter allows to report partial errors. Partial error occurs when only part of the results are ready and
// another is not available because of the failure. We still want to return partial result, but with some notification.
// NOTE: It is required to be thread-safe.
type PartialErrReporter func(error)

// Option specifies change in the opts struct.
type Option func(*opts)

type opts struct {
	disabledDeduplication bool
	partialErrReporter    PartialErrReporter
}

func defaultOpts() *opts {
	return &opts{
		partialErrReporter: func(error) {},
	}
}

// WithDisabledDeduplication returns option that sets disabledDeduplication flag to true.
func WithDisabledDeduplication() Option {
	return func(o *opts) {
		o.disabledDeduplication = true
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

	return context.WithValue(ctx, optCtxKey, opts)
}

func optsFromContext(ctx context.Context) *opts {
	o, ok := ctx.Value(optCtxKey).(*opts)
	if !ok {
		return defaultOpts()
	}

	return o
}

// StoreInfo holds meta information about a store used by query.
type StoreInfo interface {
	// Client to access the store.
	Client() storepb.StoreClient

	// Labels returns store labels that should be appended to every metric returned by this store.
	Labels() []storepb.Label
}

// Queryable allows to open a querier against a dynamic set of stores.
type Queryable struct {
	logger       log.Logger
	stores       func() []StoreInfo
	replicaLabel string
}

// NewQueryable creates implementation of promql.Queryable that fetches data from the given
// store API endpoints.
// All data retrieved from store nodes will be deduplicated along the replicaLabel by default.
func NewQueryable(logger log.Logger, stores func() []StoreInfo, replicaLabel string) *Queryable {
	return &Queryable{
		logger:       logger,
		stores:       stores,
		replicaLabel: replicaLabel,
	}
}

// Querier returns a new storage querier against the underlying stores.
func (q *Queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(ctx, q.logger, q.stores(), mint, maxt, q.replicaLabel), nil
}

type querier struct {
	logger       log.Logger
	ctx          context.Context
	cancel       func()
	mint, maxt   int64
	stores       []StoreInfo
	replicaLabel string
}

// newQuerier creates implementation of storage.Querier that fetches data from the given
// store API endpoints.
func newQuerier(
	ctx context.Context,
	logger log.Logger,
	stores []StoreInfo,
	mint, maxt int64,
	replicaLabel string,
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
		stores:       stores,
		replicaLabel: replicaLabel,
	}
}

// matchStore returns true iff the given store may hold data for the given label matchers.
func storeMatches(s StoreInfo, matchers ...*labels.Matcher) bool {
	for _, m := range matchers {
		for _, l := range s.Labels() {
			if l.Name != m.Name {
				continue
			}
			if !m.Matches(l.Value) {
				return false
			}
		}
	}
	return true
}

func (q *querier) Select(ms ...*labels.Matcher) (storage.SeriesSet, error) {
	var (
		mtx sync.Mutex
		all []storepb.SeriesSet
		// TODO(fabxc): errgroup will fail the whole query on the first encountered error.
		// Add support for partial results/errors.
		g errgroup.Group
	)
	opts := optsFromContext(q.ctx)

	span, ctx := tracing.StartSpan(q.ctx, "querier_select")
	defer span.Finish()

	sms, err := translateMatchers(ms...)
	if err != nil {
		return nil, errors.Wrap(err, "convert matchers")
	}
	for _, s := range q.stores {
		// We might be able to skip the store if its meta information indicates
		// it cannot have series matching our query.
		if !storeMatches(s, ms...) {
			continue
		}
		store := s

		g.Go(func() error {
			set, err := q.selectSingle(ctx, store.Client(), sms...)
			if err != nil {
				level.Error(q.logger).Log("msg", "single select failed. Ignoring this result.", "err", err)
				opts.partialErrReporter(err)
				return nil
			}
			mtx.Lock()
			all = append(all, set)
			mtx.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, errors.Wrap(err, "query stores")
	}
	set := promSeriesSet{
		mint: q.mint,
		maxt: q.maxt,
		set:  storepb.MergeSeriesSets(all...),
	}

	if opts.disabledDeduplication || q.replicaLabel == "" {
		// Return data without any deduplication.
		return set, nil
	}

	// The merged series set assembles all potentially-overlapping time ranges
	// of the same series into a single one. The series are ordered so that equal series
	// from different replicas are sequential. We can now deduplicate those.
	return newDedupSeriesSet(set, q.replicaLabel), nil
}

func (q *querier) selectSingle(ctx context.Context, client storepb.StoreClient, ms ...storepb.LabelMatcher) (storepb.SeriesSet, error) {
	sc, err := client.Series(ctx, &storepb.SeriesRequest{
		MinTime:  q.mint,
		MaxTime:  q.maxt,
		Matchers: ms,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}
	var set []storepb.Series

	for {
		r, err := sc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "receive series")
		}
		set = append(set, r.Series)
	}
	res := newStoreSeriesSet(set)

	if q.replicaLabel == "" {
		return res, nil
	}
	// Resort the result so that the same series with different replica
	// labels are coming right after each other.
	// TODO(fabxc): this could potentially pushed further down into the store API
	// to make true streaming possible.
	for _, s := range set {
		// Move the replica label to the very end.
		sort.Slice(s.Labels, func(i, j int) bool {
			if s.Labels[i].Name == q.replicaLabel {
				return false
			}
			if s.Labels[j].Name == q.replicaLabel {
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
	return res, nil
}

func (q *querier) LabelValues(name string) ([]string, error) {
	var (
		mtx sync.Mutex
		all [][]string
		// TODO(bplotka): errgroup will fail the whole query on the first encountered error.
		// Add support for partial results/errors.
		g errgroup.Group
	)
	opts := optsFromContext(q.ctx)

	span, ctx := tracing.StartSpan(q.ctx, "querier_label_values")
	defer span.Finish()

	for _, s := range q.stores {
		store := s

		g.Go(func() error {
			values, err := q.labelValuesSingle(ctx, store.Client(), name)
			if err != nil {
				level.Error(q.logger).Log("msg", "single labelValues failed. Ignoring this result.", "err", err)
				opts.partialErrReporter(err)
				return nil
			}

			mtx.Lock()
			all = append(all, values)
			mtx.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return strutil.MergeUnsortedSlices(all...), nil
}

func (q *querier) labelValuesSingle(ctx context.Context, client storepb.StoreClient, name string) ([]string, error) {
	resp, err := client.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label: name,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}
	return resp.Values, nil
}

func (q *querier) Close() error {
	q.cancel()
	return nil
}
