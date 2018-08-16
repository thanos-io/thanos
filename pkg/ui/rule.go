package ui

import (
	"html/template"
	"net/http"
	"sort"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/rules"
)

type Rule struct {
	*BaseUI

	ruleManager *rules.Manager
	queryURL    string
}

func NewRuleUI(logger log.Logger, ruleManager *rules.Manager, queryURL string) *Rule {
	return &Rule{
		BaseUI:      NewBaseUI(logger, "rule_menu.html", ruleTmplFuncs(queryURL)),
		ruleManager: ruleManager,
		queryURL:    queryURL,
	}
}

func ruleTmplFuncs(queryURL string) template.FuncMap {
	return template.FuncMap{
		"alertStateToClass": func(as rules.AlertState) string {
			switch as {
			case rules.StateInactive:
				return "success"
			case rules.StatePending:
				return "warning"
			case rules.StateFiring:
				return "danger"
			default:
				panic("unknown alert state")
			}
		},
		"queryURL": func() string { return queryURL },
	}
}

func (ru *Rule) alerts(w http.ResponseWriter, r *http.Request) {
	alerts := ru.ruleManager.AlertingRules()
	alertsSorter := byAlertStateAndNameSorter{alerts: alerts}
	sort.Sort(alertsSorter)

	alertStatus := AlertStatus{
		AlertingRules: alertsSorter.alerts,
		AlertStateToRowClass: map[rules.AlertState]string{
			rules.StateInactive: "success",
			rules.StatePending:  "warning",
			rules.StateFiring:   "danger",
		},
	}
	ru.executeTemplate(w, "alerts.html", alertStatus)
}

func (ru *Rule) rules(w http.ResponseWriter, r *http.Request) {
	ru.executeTemplate(w, "rules.html", ru.ruleManager)
}

func (ru *Rule) Register(r *route.Router) {
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/alerts", http.StatusFound)
	})

	instrf := prometheus.InstrumentHandlerFunc

	r.Get("/alerts", instrf("alerts", ru.alerts))
	r.Get("/rules", instrf("rules", ru.rules))

	r.Get("/static/*filepath", instrf("static", ru.serveStaticAsset))
}

// AlertStatus bundles alerting rules and the mapping of alert states to row classes.
type AlertStatus struct {
	AlertingRules        []*rules.AlertingRule
	AlertStateToRowClass map[rules.AlertState]string
}

type byAlertStateAndNameSorter struct {
	alerts []*rules.AlertingRule
}

func (s byAlertStateAndNameSorter) Len() int {
	return len(s.alerts)
}

func (s byAlertStateAndNameSorter) Less(i, j int) bool {
	return s.alerts[i].State() > s.alerts[j].State() ||
		(s.alerts[i].State() == s.alerts[j].State() &&
			s.alerts[i].Name() < s.alerts[j].Name())
}

func (s byAlertStateAndNameSorter) Swap(i, j int) {
	s.alerts[i], s.alerts[j] = s.alerts[j], s.alerts[i]
}
