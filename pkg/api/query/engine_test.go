// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/thanos/pkg/extpromql"
)

type emptyRemoteEndpoints struct{}

func (emptyRemoteEndpoints) Engines(int64, int64) []api.RemoteEngine { return nil }

func TestQueryFactoryExperimentalFunctions(t *testing.T) {
	previous := extpromql.DefaultParserOptions
	t.Cleanup(func() { extpromql.DefaultParserOptions = previous })

	queryable := storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
		return storage.NoopQuerier(), nil
	})
	for _, enabled := range []bool{false, true} {
		extpromql.DefaultParserOptions = parser.Options{EnableExperimentalFunctions: enabled}
		for _, mode := range []PromqlQueryMode{PromqlQueryModeLocal, PromqlQueryModeDistributed} {
			factory := NewQueryFactory(prometheus.NewRegistry(), log.NewNopLogger(), time.Minute, 5*time.Minute, time.Minute, false, nil, mode, true)
			for _, engineType := range []PromqlEngineType{PromqlEnginePrometheus, PromqlEngineThanos} {
				query, err := factory.makeInstantQuery(context.Background(), engineType, queryable, emptyRemoteEndpoints{},
					planOrQuery{query: `sort_by_label(vector(1), "job")`}, &engine.QueryOpts{}, time.Unix(0, 0))
				if !enabled {
					if err == nil {
						query.Close()
						t.Fatalf("%s/%s accepted an experimental function when disabled", mode, engineType)
					}
					continue
				}
				testutil.Ok(t, err)
				result := query.Exec(context.Background())
				testutil.Ok(t, result.Err)
				vector, err := result.Vector()
				testutil.Ok(t, err)
				testutil.Equals(t, 1, len(vector))
				testutil.Equals(t, float64(1), vector[0].F)
				query.Close()
			}
		}
	}
}
