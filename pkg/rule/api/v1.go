package v1

import (
	"fmt"
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	qapi "github.com/improbable-eng/thanos/pkg/query/api"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/rules"
)

type API struct {
	logger         log.Logger
	now            func() time.Time
	rulesRetriever rulesRetriever
}

func NewAPI(
	logger log.Logger,
	rr rulesRetriever,
) *API {
	return &API{
		logger:         logger,
		now:            time.Now,
		rulesRetriever: rr,
	}
}

func (api *API) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger) {
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
		return prometheus.InstrumentHandler(name, tracing.HTTPMiddleware(tracer, name, logger, gziphandler.GzipHandler(hf)))
	}

	r.Get("/alerts", instr("alerts", api.alerts))
	r.Get("/rules", instr("rules", api.rules))

}

type rulesRetriever interface {
	RuleGroups() []*rules.Group
	AlertingRules() []*rules.AlertingRule
}

func (api *API) rules(r *http.Request) (interface{}, []error, *qapi.ApiError) {
	ruleGroups := api.rulesRetriever.RuleGroups()
	res := &RuleDiscovery{RuleGroups: make([]*RuleGroup, len(ruleGroups))}
	for i, grp := range ruleGroups {
		apiRuleGroup := &RuleGroup{
			Name:     grp.Name(),
			File:     grp.File(),
			Interval: grp.Interval().Seconds(),
			Rules:    []rule{},
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
					Name:        rule.Name(),
					Query:       rule.Query().String(),
					Duration:    rule.Duration().Seconds(),
					Labels:      rule.Labels(),
					Annotations: rule.Annotations(),
					Alerts:      rulesAlertsToAPIAlerts(rule.ActiveAlerts()),
					Health:      rule.Health(),
					LastError:   lastError,
					Type:        "alerting",
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
				err := fmt.Errorf("failed to assert type of rule '%v'", rule.Name())
				return nil, nil, &qapi.ApiError{qapi.ErrorInternal, err}
			}

			apiRuleGroup.Rules = append(apiRuleGroup.Rules, enrichedRule)
		}
		res.RuleGroups[i] = apiRuleGroup
	}
	return res, nil, nil
}

func (api *API) alerts(r *http.Request) (interface{}, []error, *qapi.ApiError) {
	alertingRules := api.rulesRetriever.AlertingRules()
	alerts := []*Alert{}

	for _, alertingRule := range alertingRules {
		alerts = append(
			alerts,
			rulesAlertsToAPIAlerts(alertingRule.ActiveAlerts())...,
		)
	}

	res := &AlertDiscovery{Alerts: alerts}

	return res, nil, nil
}

type AlertDiscovery struct {
	Alerts []*Alert `json:"alerts"`
}

type Alert struct {
	Labels      labels.Labels `json:"labels"`
	Annotations labels.Labels `json:"annotations"`
	State       string        `json:"state"`
	ActiveAt    *time.Time    `json:"activeAt,omitempty"`
	Value       float64       `json:"value"`
}

func rulesAlertsToAPIAlerts(rulesAlerts []*rules.Alert) []*Alert {
	apiAlerts := make([]*Alert, len(rulesAlerts))
	for i, ruleAlert := range rulesAlerts {
		apiAlerts[i] = &Alert{
			Labels:      ruleAlert.Labels,
			Annotations: ruleAlert.Annotations,
			State:       ruleAlert.State.String(),
			ActiveAt:    &ruleAlert.ActiveAt,
			Value:       ruleAlert.Value,
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
	Rules    []rule  `json:"rules"`
	Interval float64 `json:"interval"`
}

type rule interface{}

type alertingRule struct {
	Name        string           `json:"name"`
	Query       string           `json:"query"`
	Duration    float64          `json:"duration"`
	Labels      labels.Labels    `json:"labels"`
	Annotations labels.Labels    `json:"annotations"`
	Alerts      []*Alert         `json:"alerts"`
	Health      rules.RuleHealth `json:"health"`
	LastError   string           `json:"lastError,omitempty"`
	Type        string           `json:"type"`
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
