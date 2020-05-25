// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	qapi "github.com/thanos-io/thanos/pkg/query/api"
	"github.com/thanos-io/thanos/pkg/rules"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// API is a very simple API used by Thanos Ruler.
type API struct {
	logger     log.Logger
	now        func() time.Time
	ruleGroups rules.UnaryClient
	alerts     alertsRetriever
	reg        prometheus.Registerer
}

type alertsRetriever interface {
	Active() []*rulespb.AlertInstance
}

// NewAPI creates an Thanos ruler API.
func NewAPI(
	logger log.Logger,
	reg prometheus.Registerer,
	ruleGroups rules.UnaryClient,
	activeAlerts alertsRetriever,
) *API {
	return &API{
		logger:     logger,
		now:        time.Now,
		ruleGroups: ruleGroups,
		alerts:     activeAlerts,
		reg:        reg,
	}
}

func (api *API) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware) {
	instr := func(name string, f qapi.ApiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			qapi.SetCORS(w)
			if data, warnings, err := f(r); err != nil {
				qapi.RespondError(w, err, data)
			} else if data != nil {
				qapi.Respond(w, data, warnings)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})
		return ins.NewHandler(name, tracing.HTTPMiddleware(tracer, name, logger, gziphandler.GzipHandler(hf)))
	}

	r.Get("/alerts", instr("alerts", func(r *http.Request) (interface{}, []error, *qapi.ApiError) {
		return struct{ Alerts []*rulespb.AlertInstance }{Alerts: api.alerts.Active()}, nil, nil
	}))
	r.Get("/rules", instr("rules", qapi.NewRulesHandler(api.ruleGroups)))
}
