package v1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
)

type status string

const (
	statusSuccess status = "success"
	statusError          = "error"
)

type errorType string

const (
	errorTimeout  = "timeout"
	errorCanceled = "canceled"
	errorExec     = "execution"
	errorBadData  = "bad_data"
	errorInternal = "internal"
	errorNotFound = "not_found"
)

var corsHeaders = map[string]string{
	"Access-Control-Allow-Headers":  "Accept, Accept-Encoding, Authorization, Content-Type, Origin",
	"Access-Control-Allow-Methods":  "GET, OPTIONS",
	"Access-Control-Allow-Origin":   "*",
	"Access-Control-Expose-Headers": "Date",
}

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

func (api *API) respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		level.Error(api.logger).Log("msg", "error marshaling json response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = 422
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	case errorNotFound:
		code = http.StatusNotFound
	default:
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if n, err := w.Write(b); err != nil {
		level.Error(api.logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

//type apiFunc func(r *http.Request) apiFuncResult
type apiFunc func(r *http.Request) (interface{}, []error, *apiError)

// API can register a set of endpoints in a router and handle
// them using the provided storage and query engine.
type API struct {
	logger         log.Logger
	now            func() time.Time
	rulesRetriever rulesRetriever
}

// NewAPI returns an initialized API type.
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

// Register the API's endpoints in the given router.
func (api *API) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger) {
	instr := func(name string, f apiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if data, warnings, err := f(r); err != nil {
				respondError(w, err, data)
			} else if data != nil {
				respond(w, data, warnings)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})
		return prometheus.InstrumentHandler(name, tracing.HTTPMiddleware(tracer, name, logger, gziphandler.GzipHandler(hf)))
	}

	r.Get("/alerts", instr("alerts", api.alerts))
	r.Get("/rules", instr("rules", api.rules))

}

func (api *API) respond(w http.ResponseWriter, data interface{}, warnings storage.Warnings) {
	statusMessage := statusSuccess
	var warningStrings []string
	for _, warning := range warnings {
		warningStrings = append(warningStrings, warning.Error())
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:   statusMessage,
		Data:     data,
		Warnings: warningStrings,
	})
	if err != nil {
		level.Error(api.logger).Log("msg", "error marshaling json response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(api.logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

type rulesRetriever interface {
	RuleGroups() []*rules.Group
	AlertingRules() []*rules.AlertingRule
}

func (api *API) rules(r *http.Request) (interface{}, []error, *apiError) {
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
				return nil, nil, &apiError{errorInternal, err}
			}

			apiRuleGroup.Rules = append(apiRuleGroup.Rules, enrichedRule)
		}
		res.RuleGroups[i] = apiRuleGroup
	}
	return res, nil, nil
}

func (api *API) alerts(r *http.Request) (interface{}, []error, *apiError) {
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

// Alert has info for an alert.
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

// RuleDiscovery has info for all rules
type RuleDiscovery struct {
	RuleGroups []*RuleGroup `json:"groups"`
}

type apiFuncResult struct {
	data      interface{}
	err       *apiError
	warnings  storage.Warnings
	finalizer func()
}

// RuleGroup has info for rules which are part of a group
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

func (api *API) options(r *http.Request) (interface{}, []error, *apiError) {
	return nil, nil, nil
}

func respond(w http.ResponseWriter, data interface{}, warnings []error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := &response{
		Status: statusSuccess,
		Data:   data,
	}
	for _, warn := range warnings {
		resp.Warnings = append(resp.Warnings, warn.Error())
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = 422
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	default:
		code = http.StatusInternalServerError
	}
	w.WriteHeader(code)

	_ = json.NewEncoder(w).Encode(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
}
