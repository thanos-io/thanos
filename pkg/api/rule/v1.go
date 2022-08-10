// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"

	"github.com/thanos-io/thanos/pkg/api"
	qapi "github.com/thanos-io/thanos/pkg/api/query"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/rules"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
)

// RuleAPI is a very simple API used by Thanos Ruler.
type RuleAPI struct {
	baseAPI     *api.BaseAPI
	logger      log.Logger
	ruleGroups  rules.UnaryClient
	alerts      alertsRetriever
	reg         prometheus.Registerer
	disableCORS bool
}

type alertsRetriever interface {
	Active() []*rulespb.AlertInstance
}

// NewRuleAPI creates an Thanos ruler API.
func NewRuleAPI(
	logger log.Logger,
	reg prometheus.Registerer,
	ruleGroups rules.UnaryClient,
	activeAlerts alertsRetriever,
	disableCORS bool,
	flagsMap map[string]string,
) *RuleAPI {
	return &RuleAPI{
		baseAPI:     api.NewBaseAPI(logger, disableCORS, flagsMap),
		logger:      logger,
		ruleGroups:  ruleGroups,
		alerts:      activeAlerts,
		reg:         reg,
		disableCORS: disableCORS,
	}
}

func (rapi *RuleAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	rapi.baseAPI.Register(r, tracer, logger, ins, logMiddleware)

	instr := api.GetInstr(tracer, logger, ins, logMiddleware, rapi.disableCORS)

	r.Get("/alerts", instr("alerts", func(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
		return struct{ Alerts []*rulespb.AlertInstance }{Alerts: rapi.alerts.Active()}, nil, nil, func() {}
	}))
	r.Get("/rules", instr("rules", qapi.NewRulesHandler(rapi.ruleGroups, false)))
}
