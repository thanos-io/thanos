// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"net/http"
	"strconv"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-kit/kit/log"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/rules"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	qapi "github.com/thanos-io/thanos/pkg/query/api"
	thanosrule "github.com/thanos-io/thanos/pkg/rule"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type API struct {
	logger        log.Logger
	now           func() time.Time
	ruleRetriever RulesRetriever
	reg           prometheus.Registerer
}

func NewAPI(
	logger log.Logger,
	reg prometheus.Registerer,
	ruleRetriever RulesRetriever,
) *API {
	return &API{
		logger:        logger,
		now:           time.Now,
		ruleRetriever: ruleRetriever,
		reg:           reg,
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

	r.Get("/alerts", instr("alerts", api.alerts))
	r.Get("/rules", instr("rules", api.rules))
}

type RulesRetriever interface {
	RuleGroups() []thanosrule.Group
	AlertingRules() []thanosrule.AlertingRule
}

func (api *API) rules(r *http.Request) (interface{}, []error, *qapi.ApiError) {
	res := &RuleDiscovery{}
	for _, grp := range api.ruleRetriever.RuleGroups() {
		apiRuleGroup := &RuleGroup{
			Name:                    grp.Name(),
			File:                    grp.OriginalFile(),
			Interval:                grp.Interval().Seconds(),
			Rules:                   []rule{},
			PartialResponseStrategy: grp.PartialResponseStrategy.String(),
		}

		for _, r := range grp.Rules() {
			var enrichedRule rule

			lastError := ""
			if r.LastError() != nil {
				lastError = r.LastError().Error()
			}

			switch rule := r.(type) {
			case *rules.AlertingRule:
				enrichedRule = alertingRule{
					Name:                    rule.Name(),
					Query:                   rule.Query().String(),
					Duration:                rule.Duration().Seconds(),
					Labels:                  rule.Labels(),
					Annotations:             rule.Annotations(),
					Alerts:                  rulesAlertsToAPIAlerts(grp.PartialResponseStrategy, rule.ActiveAlerts()),
					Health:                  rule.Health(),
					LastError:               lastError,
					Type:                    "alerting",
					PartialResponseStrategy: grp.PartialResponseStrategy.String(),
				}
			case *rules.RecordingRule:
				enrichedRule = recordingRule{
					Name:      rule.Name(),
					Query:     rule.Query().String(),
					Labels:    rule.Labels(),
					Health:    rule.Health(),
					LastError: lastError,
					Type:      "recording",
				}
			default:
				err := errors.Errorf("rule %q: unsupported type %T", r.Name(), rule)
				return nil, nil, &qapi.ApiError{Typ: qapi.ErrorInternal, Err: err}
			}

			apiRuleGroup.Rules = append(apiRuleGroup.Rules, enrichedRule)
		}
		res.RuleGroups = append(res.RuleGroups, apiRuleGroup)
	}

	return res, nil, nil
}

func (api *API) alerts(r *http.Request) (interface{}, []error, *qapi.ApiError) {
	var alerts []*Alert
	for _, alertingRule := range api.ruleRetriever.AlertingRules() {
		alerts = append(
			alerts,
			rulesAlertsToAPIAlerts(alertingRule.PartialResponseStrategy, alertingRule.ActiveAlerts())...,
		)
	}
	res := &AlertDiscovery{Alerts: alerts}

	return res, nil, nil
}

type AlertDiscovery struct {
	Alerts []*Alert `json:"alerts"`
}

type Alert struct {
	Labels                  labels.Labels `json:"labels"`
	Annotations             labels.Labels `json:"annotations"`
	State                   string        `json:"state"`
	ActiveAt                *time.Time    `json:"activeAt,omitempty"`
	Value                   string        `json:"value"`
	PartialResponseStrategy string        `json:"partial_response_strategy"`
}

func rulesAlertsToAPIAlerts(s storepb.PartialResponseStrategy, rulesAlerts []*rules.Alert) []*Alert {
	apiAlerts := make([]*Alert, len(rulesAlerts))
	for i, ruleAlert := range rulesAlerts {
		apiAlerts[i] = &Alert{
			PartialResponseStrategy: s.String(),
			Labels:                  ruleAlert.Labels,
			Annotations:             ruleAlert.Annotations,
			State:                   ruleAlert.State.String(),
			ActiveAt:                &ruleAlert.ActiveAt,
			Value:                   strconv.FormatFloat(ruleAlert.Value, 'e', -1, 64),
		}
	}

	return apiAlerts
}

type RuleDiscovery struct {
	RuleGroups []*RuleGroup `json:"groups"`
}

type RuleGroup struct {
	Name string `json:"name"`
	File string `json:"file"`
	// In order to preserve rule ordering, while exposing type (alerting or recording)
	// specific properties, both alerting and recording rules are exposed in the
	// same array.
	Rules                   []rule  `json:"rules"`
	Interval                float64 `json:"interval"`
	PartialResponseStrategy string  `json:"partial_response_strategy"`
}

type rule interface{}

type alertingRule struct {
	Name                    string           `json:"name"`
	Query                   string           `json:"query"`
	Duration                float64          `json:"duration"`
	Labels                  labels.Labels    `json:"labels"`
	Annotations             labels.Labels    `json:"annotations"`
	Alerts                  []*Alert         `json:"alerts"`
	Health                  rules.RuleHealth `json:"health"`
	LastError               string           `json:"lastError,omitempty"`
	Type                    string           `json:"type"`
	PartialResponseStrategy string           `json:"partial_response_strategy"`
}

type recordingRule struct {
	Name      string           `json:"name"`
	Query     string           `json:"query"`
	Labels    labels.Labels    `json:"labels,omitempty"`
	Health    rules.RuleHealth `json:"health"`
	LastError string           `json:"lastError,omitempty"`
	// Type of a recordingRule is always "recording".
	Type string `json:"type"`
}
