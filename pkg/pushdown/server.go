// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pushdown

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/thanos-io/thanos/pkg/pushdown/querypb"
	"github.com/thanos-io/thanos/pkg/store"
	grpc "google.golang.org/grpc"
)

func RegisterQueryServer(bs *store.BucketStore, maxSamples int, timeout time.Duration, logger log.Logger, reg prometheus.Registerer) func(*grpc.Server) {
	eng := promql.NewEngine(promql.EngineOpts{
		Logger:     logger,
		Reg:        reg,
		Timeout:    timeout,
		MaxSamples: maxSamples,
	})

	return func(s *grpc.Server) {
		querypb.RegisterQueryServer(s, &BucketQueryable{
			BucketStore: bs,
			Engine:      eng,
		})
	}
}
