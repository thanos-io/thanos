package adapter

import (
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/go-kit/kit/log/level"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/promql"

	"github.com/NYTimes/gziphandler"
	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/storage"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// PrometheusAdapter is a sneaky component that is able to export Thanos data directly via Prometheus native federate API.
// Experimental and not recommended: Done only for compatibility and migration purposes.
// Why not recommended?
// * Federate endpoint is not recommended as it tries to replicate metric database with requirement of highly available network (double scrape).
// Also all warnings (e.g related to partial responses) are only logged, as federate API has no way of handling those.
type PrometheusAdapter struct {
	logger log.Logger
	// TODO(bwplotka): Move to storepb.StoreClient, once Querier will allow deduplication on StoreAPI.
	// Operating on StoreAPI would allow to extract this into separate service.
	queryable storage.Queryable

	now func() time.Time
}

// NewAPI returns an initialized API type.
func NewPrometheus(
	logger log.Logger,
	queryable storage.Queryable,
) *PrometheusAdapter {
	return &PrometheusAdapter{
		logger:    logger,
		queryable: queryable,

		now: time.Now,
	}
}

// Register the API's endpoints in the given router.
func (a *PrometheusAdapter) Register(r *route.Router, tracer opentracing.Tracer, ins extpromhttp.InstrumentationMiddleware) {
	r.Get("/federate", ins.NewHandler("federate", tracing.HTTPMiddleware(tracer, "federate", a.logger, gziphandler.GzipHandler(http.HandlerFunc(a.federation)))))
}

func (a *PrometheusAdapter) federation(w http.ResponseWriter, req *http.Request) {
	if err := req.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("error parsing form values: %v", err), http.StatusBadRequest)
		return
	}

	var matcherSets [][]*labels.Matcher
	for _, s := range req.Form["match[]"] {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		matcherSets = append(matcherSets, matchers)
	}

	var (
		mint   = timestamp.FromTime(a.now().Add(-promql.LookbackDelta))
		maxt   = timestamp.FromTime(a.now())
		format = expfmt.Negotiate(req.Header)
		enc    = expfmt.NewEncoder(w, format)
	)
	w.Header().Set("Content-Type", string(format))

	q, err := a.queryable.Querier(req.Context(), mint, maxt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer runutil.CloseWithLogOnErr(a.logger, q, "close federate querier")

	vec := make(promql.Vector, 0, 8000)

	params := &storage.SelectParams{
		Start: mint,
		End:   maxt,
	}

	var sets []storage.SeriesSet
	for _, mset := range matcherSets {
		s, wrns, err := q.Select(params, mset...)
		if wrns != nil {
			level.Warn(a.logger).Log("msg", "federation select returned warnings", "warnings", wrns)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sets = append(sets, s)
	}

	set := storage.NewMergeSeriesSet(sets, nil)
	it := storage.NewBuffer(int64(promql.LookbackDelta / 1e6))
	for set.Next() {
		s := set.At()

		it.Reset(s.Iterator())

		var t int64
		var v float64

		ok := it.Seek(maxt)
		if ok {
			t, v = it.Values()
		} else {
			t, v, ok = it.PeekBack(1)
			if !ok {
				continue
			}
		}

		// The exposition formats do not support stale markers, so drop them. This
		// is good enough for staleness handling of federated data, as the
		// interval-based limits on staleness will do the right thing for supported
		// use cases (which is to say federating aggregated time series).
		if value.IsStaleNaN(v) {
			continue
		}

		vec = append(vec, promql.Sample{
			Metric: s.Labels(),
			Point:  promql.Point{T: t, V: v},
		})
	}
	if set.Err() != nil {
		http.Error(w, set.Err().Error(), http.StatusInternalServerError)
		return
	}

	sort.Sort(byName(vec))

	var (
		lastMetricName string
		protMetricFam  *dto.MetricFamily
	)
	for _, s := range vec {
		nameSeen := false
		protMetric := &dto.Metric{
			Untyped: &dto.Untyped{},
		}

		for _, l := range s.Metric {
			if l.Value == "" {
				// No value means unset. Never consider those labels.
				// This is also important to protect against nameless metrics.
				continue
			}
			if l.Name == labels.MetricName {
				nameSeen = true
				if l.Value == lastMetricName {
					// We already have the name in the current MetricFamily,
					// and we ignore nameless metrics.
					continue
				}
				// Need to start a new MetricFamily. Ship off the old one (if any) before
				// creating the new one.
				if protMetricFam != nil {
					if err := enc.Encode(protMetricFam); err != nil {
						level.Error(a.logger).Log("msg", "federation failed", "err", err)
						return
					}
				}
				protMetricFam = &dto.MetricFamily{
					Type: dto.MetricType_UNTYPED.Enum(),
					Name: proto.String(l.Value),
				}
				lastMetricName = l.Value
				continue
			}
			protMetric.Label = append(protMetric.Label, &dto.LabelPair{
				Name:  proto.String(l.Name),
				Value: proto.String(l.Value),
			})
		}
		if !nameSeen {
			level.Warn(a.logger).Log("msg", "ignoring nameless metric during federation", "metric", s.Metric)
			continue
		}

		protMetric.TimestampMs = proto.Int64(s.T)
		protMetric.Untyped.Value = proto.Float64(s.V)

		protMetricFam.Metric = append(protMetricFam.Metric, protMetric)
	}
	// Still have to ship off the last MetricFamily, if any.
	if protMetricFam != nil {
		if err := enc.Encode(protMetricFam); err != nil {
			level.Error(a.logger).Log("msg", "federation failed", "err", err)
		}
	}
}

// byName makes a model.Vector sortable by metric name.
type byName promql.Vector

func (vec byName) Len() int      { return len(vec) }
func (vec byName) Swap(i, j int) { vec[i], vec[j] = vec[j], vec[i] }

func (vec byName) Less(i, j int) bool {
	ni := vec[i].Metric.Get(labels.MetricName)
	nj := vec[j].Metric.Get(labels.MetricName)
	return ni < nj
}
