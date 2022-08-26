// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package status

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/thanos-io/thanos/pkg/api"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
)

type TenantStats struct {
	Tenant string
	Stats  *tsdb.Stats
}

// TSDBStatus has information of cardinality statistics from postings.
// TODO(fpetkovski): replace with upstream struct after dependency update.
type TSDBStatus struct {
	Tenant        string `json:"tenant"`
	v1.TSDBStatus `json:","`
}

type GetStatsFunc func(r *http.Request, statsByLabelName string) ([]TenantStats, *api.ApiError)

type Options struct {
	GetStats GetStatsFunc
	Registry *prometheus.Registry
}

type StatusAPI struct {
	getTSDBStats GetStatsFunc
	registry     *prometheus.Registry
}

func New(opts Options) *StatusAPI {
	return &StatusAPI{
		getTSDBStats: opts.GetStats,
		registry:     opts.Registry,
	}
}

func (sapi *StatusAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	instr := api.GetInstr(tracer, logger, ins, logMiddleware, false)
	r.Get("/api/v1/status/tsdb", instr("tsdb_status", sapi.httpServeStats))
}

func (sapi *StatusAPI) httpServeStats(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	stats, sterr := sapi.getTSDBStats(r, labels.MetricName)
	if sterr != nil {
		return nil, nil, sterr, func() {}
	}

	result := make([]TSDBStatus, 0, len(stats))
	if len(stats) == 0 {
		return result, nil, nil, func() {}
	}

	metrics, err := sapi.registry.Gather()
	if err != nil {
		return nil, []error{err}, nil, func() {}
	}

	tenantChunks := make(map[string]int64)
	for _, mF := range metrics {
		if *mF.Name != "prometheus_tsdb_head_chunks" {
			continue
		}

		for _, metric := range mF.Metric {
			for _, lbl := range metric.Label {
				if *lbl.Name == "tenant" {
					tenantChunks[*lbl.Value] = int64(metric.Gauge.GetValue())
				}
			}
		}
	}

	for _, s := range stats {
		var chunkCount int64
		if c, ok := tenantChunks[s.Tenant]; ok {
			chunkCount = c
		}
		result = append(result, TSDBStatus{
			Tenant: s.Tenant,
			TSDBStatus: v1.TSDBStatus{
				HeadStats: v1.HeadStats{
					NumSeries:     s.Stats.NumSeries,
					ChunkCount:    chunkCount,
					MinTime:       s.Stats.MinTime,
					MaxTime:       s.Stats.MaxTime,
					NumLabelPairs: s.Stats.IndexPostingStats.NumLabelPairs,
				},
				SeriesCountByMetricName:     v1.TSDBStatsFromIndexStats(s.Stats.IndexPostingStats.CardinalityMetricsStats),
				LabelValueCountByLabelName:  v1.TSDBStatsFromIndexStats(s.Stats.IndexPostingStats.CardinalityLabelStats),
				MemoryInBytesByLabelName:    v1.TSDBStatsFromIndexStats(s.Stats.IndexPostingStats.LabelValueStats),
				SeriesCountByLabelValuePair: v1.TSDBStatsFromIndexStats(s.Stats.IndexPostingStats.LabelValuePairsStats),
			},
		})
	}
	return result, nil, nil, func() {}
}
