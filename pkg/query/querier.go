package query

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/go-kit/kit/log"

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

// StoreInfo holds meta information about a store used by query.
type StoreInfo struct {
	Addr string

	// Client to access the store.
	Client storepb.StoreClient

	// Labels that apply to all date exposed by the backing store.
	Labels []storepb.Label

	// Minimum and maximum time range of data in the store.
	MinTime, MaxTime int64
}

func (s *StoreInfo) String() string {
	return s.Addr
}

// Queryable allows to open a querier against a dynamic set of stores.
type Queryable struct {
	logger       log.Logger
	stores       func() []*StoreInfo
	replicaLabel string
}

// NewQueryable creates implementation of promql.Queryable that fetches data from the given
// store API endpoints.
// All data retrieved from store nodes will be deduplicated along the replicaLabel by default.
func NewQueryable(logger log.Logger, stores func() []*StoreInfo, replicaLabel string) *Queryable {
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
	stores       []*StoreInfo
	replicaLabel string
}

// newQuerier creates implementation of storage.Querier that fetches data from the given
// store API endpoints.
func newQuerier(
	ctx context.Context,
	logger log.Logger,
	stores []*StoreInfo,
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

// matchStore returns true if the given store may hold data for the given label matchers.
func storeMatches(s *StoreInfo, mint, maxt int64, matchers ...*labels.Matcher) bool {
	if mint > s.MaxTime || maxt < s.MinTime {
		return false
	}
	for _, m := range matchers {
		for _, l := range s.Labels {
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

func (q *querier) isDedupEnabled(o *opts) bool {
	return o.deduplicate && q.replicaLabel != ""
}

func (q *querier) Select(ms ...*labels.Matcher) (storage.SeriesSet, error) {
	var (
		mtx sync.Mutex
		all []storepb.SeriesSet
		g   errgroup.Group
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
		if !storeMatches(s, q.mint, q.maxt, ms...) {
			continue
		}
		store := s

		g.Go(func() error {
			set, warnings, err := q.selectSingle(ctx, store.Client, sms...)
			if err != nil {
				opts.partialErrReporter(errors.Wrapf(err, "querying store failed"))
				return nil
			}

			for _, w := range warnings {
				opts.partialErrReporter(errors.New(w))
			}

			if q.isDedupEnabled(opts) {
				// TODO(fabxc): this could potentially pushed further down into the store API
				// to make true streaming possible.
				sortDedupLabels(set, q.replicaLabel)
			}

			mtx.Lock()
			all = append(all, newStoreSeriesSet(set))
			mtx.Unlock()

			return nil
		})
	}
	_ = g.Wait()
	set := promSeriesSet{
		mint: q.mint,
		maxt: q.maxt,
		set:  storepb.MergeSeriesSets(all...),
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
func (q *querier) selectSingle(
	ctx context.Context,
	client storepb.StoreClient,
	ms ...storepb.LabelMatcher,
) ([]storepb.Series, []string, error) {
	sc, err := client.Series(ctx, &storepb.SeriesRequest{
		MinTime:  q.mint,
		MaxTime:  q.maxt,
		Matchers: ms,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "fetch series")
	}
	var (
		set      []storepb.Series
		warnings []string
	)

	for {
		r, err := sc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, errors.Wrap(err, "receive series")
		}

		if w := r.GetWarning(); w != "" {
			warnings = append(warnings, w)
			continue
		}

		set = append(set, *r.GetSeries())
	}

	return set, warnings, nil
}

func (q *querier) LabelValues(name string) ([]string, error) {
	var (
		mtx sync.Mutex
		all [][]string
		g   errgroup.Group
	)
	opts := optsFromContext(q.ctx)

	span, ctx := tracing.StartSpan(q.ctx, "querier_label_values")
	defer span.Finish()

	for _, s := range q.stores {
		store := s

		g.Go(func() error {
			values, warnings, err := q.labelValuesSingle(ctx, store.Client, name)
			if err != nil {
				opts.partialErrReporter(errors.Wrap(err, "querying store failed"))
				return nil
			}

			for _, w := range warnings {
				opts.partialErrReporter(errors.New(w))
			}

			mtx.Lock()
			all = append(all, values)
			mtx.Unlock()

			return nil
		})
	}
	_ = g.Wait()
	return strutil.MergeUnsortedSlices(all...), nil
}

func (q *querier) labelValuesSingle(ctx context.Context, client storepb.StoreClient, name string) ([]string, []string, error) {
	resp, err := client.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label: name,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "fetch series")
	}
	return resp.Values, resp.Warnings, nil
}

func (q *querier) Close() error {
	q.cancel()
	return nil
}
