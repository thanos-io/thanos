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
	"fmt"
	"math"
	"net/http"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/thanos-io/thanos/pkg/api"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
)

// Stat holds the information about individual cardinality.
type Stat struct {
	Name  string `json:"name"`
	Value uint64 `json:"value"`
}

func convertStats(stats []index.Stat) []Stat {
	result := make([]Stat, 0, len(stats))
	for _, item := range stats {
		item := Stat{Name: item.Name, Value: item.Count}
		result = append(result, item)
	}
	return result
}

// TSDBStatus has information of cardinality statistics from postings.
type TSDBStatus struct {
	HeadStats                   v1.HeadStats `json:"headStats"`
	SeriesCountByMetricName     []Stat       `json:"seriesCountByMetricName"`
	LabelValueCountByLabelName  []Stat       `json:"labelValueCountByLabelName"`
	MemoryInBytesByLabelName    []Stat       `json:"memoryInBytesByLabelName"`
	SeriesCountByLabelValuePair []Stat       `json:"seriesCountByLabelValuePair"`
}

type GetStatsFunc func(r *http.Request, statsByLabelName string) (*tsdb.Stats, error)

type Options struct {
	GetStats GetStatsFunc
	Registry *prometheus.Registry
}

// TODO(fpetkovski): replace with upstream struct after dependency update.
type StatusAPI struct {
	getTSDBStats GetStatsFunc
	options      Options
}

func New(opts Options) *StatusAPI {
	return &StatusAPI{
		getTSDBStats: opts.GetStats,
		options:      opts,
	}
}

func (sapi *StatusAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	instr := api.GetInstr(tracer, logger, ins, logMiddleware, false)
	r.Get("/api/v1/status/tsdb", instr("tsdb_status", sapi.httpServeStats))
}

func (sapi *StatusAPI) httpServeStats(r *http.Request) (interface{}, []error, *api.ApiError) {
	s, err := sapi.getTSDBStats(r, labels.MetricName)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: err}
	}

	if s == nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: fmt.Errorf("unknown tenant")}
	}

	metrics, err := sapi.options.Registry.Gather()
	if err != nil {
		return nil, []error{err}, nil
	}

	chunkCount := int64(math.NaN())
	for _, mF := range metrics {
		if *mF.Name == "prometheus_tsdb_head_chunks" {
			m := *mF.Metric[0]
			if m.Gauge != nil {
				chunkCount = int64(m.Gauge.GetValue())
				break
			}
		}
	}

	return TSDBStatus{
		HeadStats: v1.HeadStats{
			NumSeries:     s.NumSeries,
			ChunkCount:    chunkCount,
			MinTime:       s.MinTime,
			MaxTime:       s.MaxTime,
			NumLabelPairs: s.IndexPostingStats.NumLabelPairs,
		},
		SeriesCountByMetricName:     convertStats(s.IndexPostingStats.CardinalityMetricsStats),
		LabelValueCountByLabelName:  convertStats(s.IndexPostingStats.CardinalityLabelStats),
		MemoryInBytesByLabelName:    convertStats(s.IndexPostingStats.LabelValueStats),
		SeriesCountByLabelValuePair: convertStats(s.IndexPostingStats.LabelValuePairsStats),
	}, nil, nil

}
