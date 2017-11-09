package query

import (
	"context"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/pkg/errors"
	promlabels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var _ promql.Queryable = (*Queryable)(nil)

// StoreInfo holds meta information about a store.
type StoreInfo interface {
	// Conn returns connection to the store API.
	Conn() *grpc.ClientConn

	// Ready returns true if store is ready. In our case it is ready, only after labels when set for the first time.
	Ready() bool

	// Labels returns store labels that should be appended to every metric returned by this store.
	Labels() []storepb.Label
}

// Queryable allows to open a querier against a dynamic set of stores.
type Queryable struct {
	logger         log.Logger
	storeAddresses []string
	stores         func() []StoreInfo
}

// NewQueryable creates implementation of promql.Queryable that fetches data from the given
// store API endpoints.
func NewQueryable(logger log.Logger, stores func() []StoreInfo) *Queryable {
	return &Queryable{
		logger: logger,
		stores: stores,
	}
}

func (q *Queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(q.logger, ctx, q.stores(), mint, maxt), nil
}

type querier struct {
	logger     log.Logger
	ctx        context.Context
	cancel     func()
	mint, maxt int64
	stores     []StoreInfo
}

// newQuerier creates implementation of storage.Querier that fetches data from the given
// store API endpoints.
func newQuerier(logger log.Logger, ctx context.Context, stores []StoreInfo, mint, maxt int64) *querier {
	ctx, cancel := context.WithCancel(ctx)
	return &querier{
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
		mint:   mint,
		maxt:   maxt,
		stores: stores,
	}
}

func (q *querier) Select(ms ...*promlabels.Matcher) storage.SeriesSet {
	var (
		mtx sync.Mutex
		all []tsdb.SeriesSet
		// TODO(fabxc): errgroup will fail the whole query on the first encountered error.
		// Add support for partial results/errors.
		g errgroup.Group
	)

	sms, err := translateMatchers(ms...)
	if err != nil {
		return promSeriesSet{set: errSeriesSet{err: err}}
	}
	for _, s := range q.stores {
		store := s

		g.Go(func() error {
			set, err := q.selectSingle(store.Conn(), sms...)
			if err != nil {
				return err
			}
			mtx.Lock()
			all = append(all, set)
			mtx.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return promSeriesSet{errSeriesSet{err: err}}
	}

	return promSeriesSet{set: mergeAllSeriesSets(all...)}
}

func (q *querier) selectSingle(conn *grpc.ClientConn, ms ...storepb.LabelMatcher) (tsdb.SeriesSet, error) {
	c := storepb.NewStoreClient(conn)

	resp, err := c.Series(q.ctx, &storepb.SeriesRequest{
		MinTime:  q.mint,
		MaxTime:  q.maxt,
		Matchers: ms,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}
	return &storeSeriesSet{series: resp.Series, i: -1, mint: q.mint, maxt: q.maxt}, nil
}

func (q *querier) LabelValues(name string) ([]string, error) {
	var (
		mtx sync.Mutex
		all []string
		// TODO(bplotka): errgroup will fail the whole query on the first encountered error.
		// Add support for partial results/errors.
		g errgroup.Group
	)

	for _, s := range q.stores {
		g.Go(func() error {
			values, err := q.labelValuesSingle(s.Conn(), name)
			if err != nil {
				return err
			}

			mtx.Lock()
			all = append(all, values...)
			mtx.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return dedupStrings(all), nil
}

func (q *querier) labelValuesSingle(conn *grpc.ClientConn, name string) ([]string, error) {
	c := storepb.NewStoreClient(conn)

	resp, err := c.LabelValues(q.ctx, &storepb.LabelValuesRequest{
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
