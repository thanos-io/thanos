// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pushdown

import (
	"context"
	"encoding/json"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/pushdown/querypb"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type queryData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
}

type BucketQueryable struct {
	BucketStore         *store.BucketStore
	replicaLabels       map[string]struct{}
	maxSourceResolution int64
	Engine              *promql.Engine
}

type bucketQuerier struct {
	bq                  *BucketQueryable
	ctx                 context.Context
	replicaLabels       map[string]struct{}
	maxSourceResolution int64

	mint, maxt int64
}

func (querier *bucketQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if hints == nil {
		hints = &storage.SelectHints{
			Start: querier.mint,
			End:   querier.maxt,
		}
	}

	aggrs := query.AggrsFromFunc(hints.Func)

	seriesSets, err := querier.bq.BucketStore.Select(querier.ctx, sortSeries, hints, aggrs, querier.maxSourceResolution, matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	var seriesResp []storepb.Series

	set := storepb.MergeSeriesSets(seriesSets...)
	for set.Next() {
		var series storepb.Series

		lset, chks := set.At()

		series.Chunks = chks
		series.Labels = labelpb.ZLabelsFromPromLabels(lset)

		seriesResp = append(seriesResp, series)
	}

	query.SortDedupLabels(seriesResp, querier.replicaLabels)
	ss := query.NewPromSeriesSet(
		hints.Start,
		hints.End,
		aggrs,
		nil,
		query.NewStoreSeriesSet(seriesResp),
	)

	return dedup.NewSeriesSet(ss, querier.replicaLabels, len(aggrs) == 1 && aggrs[0] == storepb.Aggr_COUNTER)
}

func (b *bucketQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (b *bucketQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (b *bucketQuerier) Close() error {
	return nil
}

func (b *BucketQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &bucketQuerier{
		bq:                  b,
		ctx:                 ctx,
		replicaLabels:       b.replicaLabels,
		maxSourceResolution: b.maxSourceResolution,
		mint:                mint,
		maxt:                maxt,
	}, nil
}

func (s *BucketQueryable) Query(ctx context.Context, r *querypb.QueryRequest) (*querypb.QueryResponse, error) {
	dedupedReplicaLabels := map[string]struct{}{}
	for _, rl := range r.ReplicaLabels {
		dedupedReplicaLabels[rl] = struct{}{}
	}

	start := time.Unix(0, int64(r.StartNs))
	end := time.Unix(0, int64(r.EndNs))

	q, err := s.Engine.NewRangeQuery(&BucketQueryable{
		BucketStore:         s.BucketStore,
		replicaLabels:       dedupedReplicaLabels,
		maxSourceResolution: r.MaxSourceResolution,
	}, r.Query, start, end, time.Duration(r.Interval))
	if err != nil {
		return nil, err
	}
	defer q.Close()

	gctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	result := q.Exec(gctx)
	if result.Err != nil {
		return nil, result.Err
	}

	qData := queryData{
		Result:     result.Value,
		ResultType: result.Value.Type(),
	}

	marshalledResp, err := json.Marshal(qData)
	if err != nil {
		return nil, err
	}

	return &querypb.QueryResponse{Response: string(marshalledResp)}, nil
}
