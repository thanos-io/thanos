package query

import (
	"context"
	"sync"
	"time"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

type Instant interface {
	QueryInstant(ctx context.Context, qs string, ts time.Time, opts Options) (promql.Value, []error, error)
}

type Range interface {
	QueryRange(ctx context.Context, qs string, start, end time.Time, interval time.Duration, opts Options) (promql.Value, []error, error)
}

type API struct {
	engine *promql.Engine

	replicaLabel string
	storeAPI     storepb.StoreServer
}

func NewAPI(engine *promql.Engine, replicaLabel string, storeAPI storepb.StoreServer) *API {
	return &API{
		engine:       engine,
		replicaLabel: replicaLabel,
		storeAPI:     storeAPI,
	}
}

func (a *API) QueryInstant(ctx context.Context, qs string, ts time.Time, opts Options) (promql.Value, []error, error) {
	var (
		warnmtx         sync.Mutex
		warnings        []error
		warningReporter = func(err error) {
			warnmtx.Lock()
			warnings = append(warnings, err)
			warnmtx.Unlock()
		}
	)

	q, err := a.engine.NewInstantQuery(
		storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
			// Current imported promql.Engine version does not use anything than Select.
			// TODO(bwplotka): Consider fixing upstream composed interface to reflect that? It has still LabelNames
			return newSelectOnlyQuerier(ctx, mint, maxt, a.replicaLabel, a.storeAPI, warningReporter, opts), nil
		}), qs, ts,
	)
	if err != nil {
		return nil, nil, err
	}

	res := q.Exec(ctx)
	q.Close()

	// NOTE: `res.Warnings` is removed anyway in newest TSDB.
	return res.Value, warnings, res.Err
}

func (a *API) QueryRange(ctx context.Context, qs string, start, end time.Time, interval time.Duration, opts Options) (promql.Value, []error, error) {
	var (
		warnmtx         sync.Mutex
		warnings        []error
		warningReporter = func(err error) {
			warnmtx.Lock()
			warnings = append(warnings, err)
			warnmtx.Unlock()
		}
	)

	q, err := a.engine.NewRangeQuery(
		storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
			// Current imported promql.Engine version does not use anything than Select.
			return newSelectOnlyQuerier(ctx, mint, maxt, a.replicaLabel, a.storeAPI, warningReporter, opts), nil
		}), qs, start, end, interval,
	)
	if err != nil {
		return nil, nil, err
	}

	res := q.Exec(ctx)
	q.Close()

	// NOTE: `res.Warnings` is removed anyway in newest TSDB.
	return res.Value, warnings, res.Err
}
