// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package promqlaggr is a PromQL engine that wraps storepb.SeriesSet for adhoc aggregations controled by storepb.QueryHints.
package promqlaggr

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type seriesQueryable struct {
	storage.LabelQuerier // Fake, not used.

	s                 storepb.SeriesSet
	downsamplingAggrs []storepb.Aggr
}

func (s *seriesQueryable) Close() error { return nil }

func (s *seriesQueryable) Querier(_ context.Context, _, _ int64) (storage.Querier, error) {
	return s, nil
}

func (s *seriesQueryable) Select(_ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storepb.NewPromSeriesSet(math.MinInt64, math.MaxInt64, s.s, s.downsamplingAggrs, nil)
}

func AggregatedSeriesSet(ctx context.Context, s storepb.SeriesSet, mint, maxt int64, hints []storepb.QueryHints, downsamplingAggrs []storepb.Aggr) storepb.SeriesSet {
	// TODO(bwplotka): Super hacky, change PromQL upstream to allow us to avoid extra query parsing.
	// TODO(bwplotka): Extra encoding/decoding is pain, optimize for this better.

	// YOLO.
	q := "name{}" // Name does not matter.
	for _, h := range hints {
		if h.Range > 0 {
			q += fmt.Sprintf("[%vms]", h.Range)
		}
		if h.Func != "" {
			q = fmt.Sprintf("%s(%s)", h.Func, q)
		}
		if len(h.Grouping) > 0 {
			if h.By {
				q += " by "
			} else {
				q += " without "
			}
			q += "( " + strings.Join(h.Grouping, ", ") + " )"
		}
	}
	fmt.Printf("pushed query on block\n: %v", q)

	// Range query?
	var (
		e         = promql.NewEngine(promql.EngineOpts{Timeout: 999999 * time.Second, MaxSamples: math.MaxInt32})
		qr        promql.Query
		err       error
		queryable = &seriesQueryable{s: s, downsamplingAggrs: downsamplingAggrs}
	)

	if hints[0].Step == 0 {
		qr, err = e.NewInstantQuery(queryable, q, timestamp.Time(maxt)) // Time does not matter.
	} else {
		qr, err = e.NewRangeQuery(queryable, q, timestamp.Time(mint), timestamp.Time(maxt), time.Duration(hints[0].Step*1000))
	}
	if err != nil {
		return storepb.ErrSeriesSet(err)
	}

	r := qr.Exec(ctx)
	if r.Err != nil {
		return storepb.ErrSeriesSet(r.Err)
	}

	if len(r.Warnings) > 0 {
		return storepb.ErrSeriesSet(errors.Errorf("warnings %v", r.Warnings))
	}

	// TODO(bwplotka): This is terrible, we literally buffer all results. Eyes are bleeding...
	return bufferedValueToSeriesSet(r.Value)
}

// Worst idea ever.
func bufferedValueToSeriesSet(v parser.Value) storepb.SeriesSet {
	m, ok := v.(promql.Matrix)
	if !ok {
		vec, ok := v.(promql.Vector)
		if !ok {
			return storepb.ErrSeriesSet(errors.Errorf("why not matrix or vector (: Actual type: %v", v.Type()))
		}
		return &vectorSeriesSet{s: vec, i: -1}
	}
	return &matrixSeriesSet{m: m, i: -1}
}

type vectorSeriesSet struct {
	s []promql.Sample
	i int
}

func (q *vectorSeriesSet) Next() bool {
	if q.i >= len(q.s)-1 {
		return false
	}
	q.i++

	return true
}

func (q *vectorSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	s := q.s[q.i]

	chk := chunkenc.NewXORChunk()
	app, _ := chk.Appender()
	app.Append(s.T, s.V)

	return s.Metric, []storepb.AggrChunk{{Raw: &storepb.Chunk{
		Type: storepb.Chunk_XOR,
		Data: chk.Bytes(),
	}}}
}

func (q *vectorSeriesSet) Err() error { return nil }

type matrixSeriesSet struct {
	m promql.Matrix
	i int
}

func (q *matrixSeriesSet) Next() bool {
	if q.i >= q.m.Len()-1 {
		return false
	}
	q.i++

	return true
}

func (q *matrixSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	s := q.m[q.i]

	var a []storepb.AggrChunk

	chk := chunkenc.NewXORChunk()
	app, _ := chk.Appender()
	for _, sample := range s.Points {
		if chk.NumSamples() >= 120 {
			a = append(a, storepb.AggrChunk{
				Raw: &storepb.Chunk{
					Type: storepb.Chunk_XOR,
					Data: chk.Bytes(),
				},
			})

			chk = chunkenc.NewXORChunk()
			app, _ = chk.Appender()
		}
		app.Append(sample.T, sample.V)
	}
	a = append(a, storepb.AggrChunk{
		Raw: &storepb.Chunk{
			Type: storepb.Chunk_XOR,
			Data: chk.Bytes(),
		},
	})

	return s.Metric, a
}

func (q *matrixSeriesSet) Err() error { return nil }
