// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package querysharding

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type shardQueryable struct {
	storage.Queryable
	m *storepb.ShardMatcher
}

func (s shardQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := s.Queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return shardQuerier{q, s.m}, nil
}

type shardQuerier struct {
	storage.Querier
	m *storepb.ShardMatcher
}

func (q shardQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.SeriesSet {
	return &shardSet{SeriesSet: q.Querier.Select(ctx, sortSeries, hints, ms...), m: q.m}
}

type shardSet struct {
	storage.SeriesSet
	m *storepb.ShardMatcher
}

func (s *shardSet) Next() bool {
	for s.SeriesSet.Next() {
		if s.m.MatchesLabels(s.SeriesSet.At().Labels()) {
			return true
		}
	}
	return false
}

func renderRange(v promql.Matrix) []string {
	var out []string
	for _, s := range v {
		var sb strings.Builder
		sb.WriteString(s.Metric.String())
		sb.WriteString(" =>")
		for _, p := range s.Floats {
			fmt.Fprintf(&sb, " %d:%.6g", p.T, p.F)
		}
		out = append(out, sb.String())
	}
	sort.Strings(out)
	return out
}

// TestShardedQueryMatchesUnsharded runs every shard of an analyzed query on the
// Prometheus engine and checks that the union of the shard results equals the
// result of the unsharded query.
func TestShardedQueryMatchesUnsharded(t *testing.T) {
	st := teststorage.New(t)
	t.Cleanup(func() { st.Close() })
	app := st.Appender(context.Background())
	for _, name := range []string{"a", "b", "c"} {
		for _, job := range []string{"j1", "j2", "j3"} {
			for _, inst := range []string{"i1", "i2", "i3", "i4"} {
				for _, pod := range []string{"p1", "p2"} {
					lbls := labels.FromStrings("__name__", name, "job", job, "instance", inst, "pod", pod)
					for i := 0; i < 60; i++ {
						_, err := app.Append(0, lbls, int64(i)*15000, float64(i*(len(name)+len(job)+len(inst)+len(pod)+int(inst[1])+int(job[1]))))
						require.NoError(t, err)
					}
				}
			}
		}
	}
	require.NoError(t, app.Commit())

	engine := promql.NewEngine(promql.EngineOpts{Timeout: time.Minute, MaxSamples: 1000000, LookbackDelta: 5 * time.Minute})
	queries := []string{
		// Aggregating without labels drops the metric name, so series that
		// only differ by name belong to the same group.
		`sum without (pod) ({__name__=~"a|b"})`,
		`max without (pod) ({__name__=~"a|b|c"})`,
		`count without (pod, instance) ({__name__=~"a|b"})`,
		`sum without (pod) ({job="j1"})`,
		`sum without (pod) (a)`,
		`sum by (job) ({__name__=~"a|b"})`,
		`sum by (job) (a) / on (job) sum by (job) (b)`,
	}
	for _, q := range queries {
		for _, shards := range []int64{2, 3, 5} {
			t.Run(fmt.Sprintf("%s/%d", q, shards), func(t *testing.T) {
				an, err := NewQueryAnalyzer().Analyze(q)
				require.NoError(t, err)
				require.True(t, an.IsShardable())
				run := func(qa storage.Queryable) promql.Matrix {
					qry, err := engine.NewRangeQuery(context.Background(), qa, nil, q, time.Unix(300, 0), time.Unix(800, 0), 30*time.Second)
					require.NoError(t, err)
					res := qry.Exec(context.Background())
					require.NoError(t, res.Err)
					m, err := res.Matrix()
					require.NoError(t, err)
					return m
				}
				want := renderRange(run(st))
				var all promql.Matrix
				buffers := &sync.Pool{New: func() any { b := make([]byte, 0, 100); return &b }}
				for i := int64(0); i < shards; i++ {
					info := &storepb.ShardInfo{TotalShards: shards, ShardIndex: i, By: an.ShardBy(), Labels: an.ShardingLabels()}
					all = append(all, run(shardQueryable{st, info.Matcher(buffers)})...)
				}
				got := renderRange(all)
				require.Equal(t, want, got, "labels=%v by=%v", an.ShardingLabels(), an.ShardBy())
			})
		}
	}
}
