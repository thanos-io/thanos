// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-kit/kit/log"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
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

func (api *API) rules(*http.Request) (interface{}, []error, *qapi.ApiError) {
	res := &storepb.RuleGroups{}
	for _, grp := range api.ruleRetriever.RuleGroups() {
		apiRuleGroup := &storepb.RuleGroup{
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
				apiRuleGroup.Rules = append(apiRuleGroup.Rules, &storepb.Rule{
					Result: &storepb.Rule_Alert{Alert: &storepb.Alert{
						State:                     storepb.AlertState(rule.State()),
						Name:                      rule.Name(),
						Query:                     rule.Query().String(),
						DurationSeconds:           rule.Duration().Seconds(),
						Labels:                    &storepb.PromLabels{Labels: storepb.PromLabelsToLabels(rule.Labels())},
						Annotations:               &storepb.PromLabels{Labels: storepb.PromLabelsToLabels(rule.Annotations())},
						Alerts:                    rulesAlertsToAPIAlerts(grp.PartialResponseStrategy, rule.ActiveAlerts()),
						Health:                    string(rule.Health()),
						LastError:                 lastError,
						EvaluationDurationSeconds: rule.GetEvaluationDuration().Seconds(),
						LastEvaluation:            rule.GetEvaluationTimestamp(),
					}}})
			case *rules.RecordingRule:
				apiRuleGroup.Rules = append(apiRuleGroup.Rules, &storepb.Rule{
					Result: &storepb.Rule_Recording{Recording: &storepb.RecordingRule{
						Name:                      rule.Name(),
						Query:                     rule.Query().String(),
						Labels:                    &storepb.PromLabels{Labels: storepb.PromLabelsToLabels(rule.Labels())},
						Health:                    string(rule.Health()),
						LastError:                 lastError,
						EvaluationDurationSeconds: rule.GetEvaluationDuration().Seconds(),
						LastEvaluation:            rule.GetEvaluationTimestamp(),
					}}})
			default:
				err := fmt.Errorf("rule %q: unsupported type %T", r.Name(), rule)
				return nil, nil, &qapi.ApiError{Typ: qapi.ErrorInternal, Err: err}
			}
		}
		res.Groups = append(res.Groups, apiRuleGroup)
	}

	return res, nil, nil
}

func (api *API) alerts(*http.Request) (interface{}, []error, *qapi.ApiError) {
	var alerts []*storepb.AlertInstance
	for _, alertingRule := range api.ruleRetriever.AlertingRules() {
		alerts = append(
			alerts,
			rulesAlertsToAPIAlerts(alertingRule.PartialResponseStrategy, alertingRule.ActiveAlerts())...,
		)
	}
	return struct{ Alerts []*storepb.AlertInstance }{Alerts: alerts}, nil, nil
}

func rulesAlertsToAPIAlerts(s storepb.PartialResponseStrategy, rulesAlerts []*rules.Alert) []*storepb.AlertInstance {
	apiAlerts := make([]*storepb.AlertInstance, len(rulesAlerts))
	for i, ruleAlert := range rulesAlerts {
		apiAlerts[i] = &storepb.AlertInstance{
			PartialResponseStrategy: s,
			Labels:                  &storepb.PromLabels{Labels: storepb.PromLabelsToLabels(ruleAlert.Labels)},
			Annotations:             &storepb.PromLabels{Labels: storepb.PromLabelsToLabels(ruleAlert.Annotations)},
			State:                   storepb.AlertState(ruleAlert.State),
			ActiveAt:                &ruleAlert.ActiveAt,
			Value:                   strconv.FormatFloat(ruleAlert.Value, 'e', -1, 64),
		}
	}

	return apiAlerts
}
