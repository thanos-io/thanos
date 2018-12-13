package ui

import (
	"net/http"
	"sync"
	"sort"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"


  "github.com/improbable-eng/thanos/pkg/query"

  "github.com/gogo/protobuf/proto"

  dto "github.com/prometheus/client_model/go"
  "github.com/prometheus/client_golang/prometheus"
  
  "github.com/prometheus/common/route"
  "github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
  "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/storage"
)

type FederationQuery struct {
  queryableCreate query.QueryableCreator
  logger          log.Logger
  now func()      model.Time
}

func NewQueryFederation(logger log.Logger, flagsMap map[string]string, q query.QueryableCreator) *FederationQuery {
	return &FederationQuery{
    queryableCreate: q,
    logger:          logger,
		now:             model.Now,
	}
}

func (f *FederationQuery) Register(r *route.Router) {
  instrf := prometheus.InstrumentHandlerFunc
  r.Get("/federate", instrf("federate", f.federate))
}


func (f *FederationQuery) federate(w http.ResponseWriter, r *http.Request) {
  r.ParseForm()

	var matcherSets [][]*labels.Matcher
	for _, s := range r.Form["match[]"] {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		matcherSets = append(matcherSets, matchers)
	}

	var (
		mint   = timestamp.FromTime(f.now().Time().Add(-promql.LookbackDelta))
		maxt   = timestamp.FromTime(f.now().Time())
		format = expfmt.Negotiate(r.Header)
		enc    = expfmt.NewEncoder(w, format)
	)

	w.Header().Set("Content-Type", string(format))

  var (
		warnmtx             sync.Mutex
		warnings            []error
		enableDeduplication = true
	)
	partialErrReporter := func(err error) {
		warnmtx.Lock()
		warnings = append(warnings, err)
		warnmtx.Unlock()
	}
  // TODO: make the deduplication configurable
  q, err := f.queryableCreate(enableDeduplication, 0, partialErrReporter).Querier(r.Context(), mint, maxt)
	if err != nil {
    // TODO: make add relevant metrics
		// federationErrors.Inc()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer q.Close()

	vec := make(promql.Vector, 0, 8000)

	var sets []storage.SeriesSet
	for _, mset := range matcherSets {
		s, err := q.Select(&storage.SelectParams{}, mset...)
		if err != nil {
			// federationErrors.Inc()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sets = append(sets, s)
	}
	set := storage.NewMergeSeriesSet(sets)
	for set.Next() {
		s := set.At()

    it := storage.NewBuffer(s.Iterator(), int64(promql.LookbackDelta/1e6))

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
		// federationErrors.Inc()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sort.Sort(byName(vec))

	// externalLabels := h.config.GlobalConfig.ExternalLabels.Clone()
	// if _, ok := externalLabels[model.InstanceLabel]; !ok {
	// 	externalLabels[model.InstanceLabel] = ""
	// }
	// externalLabelNames := make(model.LabelNames, 0, len(externalLabels))
	// for ln := range externalLabels {
	// 	externalLabelNames = append(externalLabelNames, ln)
	// }
	// sort.Sort(externalLabelNames)
  //
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
						// federationErrors.Inc()
						level.Error(f.logger).Log("msg", "federation failed", "err", err)
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
			level.Warn(f.logger).Log("msg", "Ignoring nameless metric during federation", "metric", s.Metric)
			continue
		}

		protMetric.TimestampMs = proto.Int64(s.T)
		protMetric.Untyped.Value = proto.Float64(s.V)

		protMetricFam.Metric = append(protMetricFam.Metric, protMetric)
	}
	// Still have to ship off the last MetricFamily, if any.
	if protMetricFam != nil {
		if err := enc.Encode(protMetricFam); err != nil {
			// federationErrors.Inc()
			level.Error(f.logger).Log("msg", "federation failed", "err", err)
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
