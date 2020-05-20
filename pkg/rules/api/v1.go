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
	"github.com/thanos-io/thanos/pkg/rules/rulespb"

	"github.com/go-kit/kit/log"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/rules"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	qapi "github.com/thanos-io/thanos/pkg/query/api"
	"github.com/thanos-io/thanos/pkg/rules/manager"
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
	RuleGroups() []manager.Group
	AlertingRules() []manager.AlertingRule
}

func (api *API) rules(*http.Request) (interface{}, []error, *qapi.ApiError) {
	res := &rulespb.RuleGroups{}
	for _, grp := range api.ruleRetriever.RuleGroups() {
		apiRuleGroup := &rulespb.RuleGroup{
			Name:                    grp.Name(),
			File:                    grp.OriginalFile(),
			Interval:                grp.Interval().Seconds(),
			PartialResponseStrategy: grp.PartialResponseStrategy,
		}

		for _, r := range grp.Rules() {
			lastError := ""
			if r.LastError() != nil {
				lastError = r.LastError().Error()
			}

			switch rule := r.(type) {
			case *rules.AlertingRule:
				apiRuleGroup.Rules = append(apiRuleGroup.Rules, &rulespb.Rule{
					Result: &rulespb.Rule_Alert{Alert: &rulespb.Alert{
						State:                     rulespb.AlertState(rule.State()),
						Name:                      rule.Name(),
						Query:                     rule.Query().String(),
						DurationSeconds:           rule.Duration().Seconds(),
						Labels:                    &rulespb.PromLabels{Labels: storepb.PromLabelsToLabels(rule.Labels())},
						Annotations:               &rulespb.PromLabels{Labels: storepb.PromLabelsToLabels(rule.Annotations())},
						Alerts:                    rulesAlertsToAPIAlerts(grp.PartialResponseStrategy, rule.ActiveAlerts()),
						Health:                    string(rule.Health()),
						LastError:                 lastError,
						EvaluationDurationSeconds: rule.GetEvaluationDuration().Seconds(),
						LastEvaluation:            rule.GetEvaluationTimestamp(),
					}}})
			case *rules.RecordingRule:
				apiRuleGroup.Rules = append(apiRuleGroup.Rules, &rulespb.Rule{
					Result: &rulespb.Rule_Recording{Recording: &rulespb.RecordingRule{
						Name:                      rule.Name(),
						Query:                     rule.Query().String(),
						Labels:                    &rulespb.PromLabels{Labels: storepb.PromLabelsToLabels(rule.Labels())},
						Health:                    string(rule.Health()),
						LastError:                 lastError,
						EvaluationDurationSeconds: rule.GetEvaluationDuration().Seconds(),
						LastEvaluation:            rule.GetEvaluationTimestamp(),
					}}})
			default:
				err := errors.Errorf("rule %q: unsupported type %T", r.Name(), rule)
				return nil, nil, &qapi.ApiError{Typ: qapi.ErrorInternal, Err: err}
			}
		}
		res.Groups = append(res.Groups, apiRuleGroup)
	}

	return res, nil, nil
}

func (api *API) alerts(*http.Request) (interface{}, []error, *qapi.ApiError) {
	var alerts []*rulespb.AlertInstance
	for _, alertingRule := range api.ruleRetriever.AlertingRules() {
		alerts = append(
			alerts,
			rulesAlertsToAPIAlerts(alertingRule.PartialResponseStrategy, alertingRule.ActiveAlerts())...,
		)
	}
	return struct{ Alerts []*rulespb.AlertInstance }{Alerts: alerts}, nil, nil
}

func rulesAlertsToAPIAlerts(s storepb.PartialResponseStrategy, rulesAlerts []*rules.Alert) []*rulespb.AlertInstance {
	apiAlerts := make([]*rulespb.AlertInstance, len(rulesAlerts))
	for i, ruleAlert := range rulesAlerts {
		apiAlerts[i] = &rulespb.AlertInstance{
			PartialResponseStrategy: s,
			Labels:                  &rulespb.PromLabels{Labels: storepb.PromLabelsToLabels(ruleAlert.Labels)},
			Annotations:             &rulespb.PromLabels{Labels: storepb.PromLabelsToLabels(ruleAlert.Annotations)},
			State:                   rulespb.AlertState(ruleAlert.State),
			ActiveAt:                &ruleAlert.ActiveAt,
			Value:                   strconv.FormatFloat(ruleAlert.Value, 'e', -1, 64),
		}
	}

	return apiAlerts
}
