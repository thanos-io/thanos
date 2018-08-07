package ui

import (
	"bytes"
	"fmt"
	"html/template"
	"io"
	"math"
	"net/http"
	"path/filepath"
	"regexp"
	"sort"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/rules"
)

type UI struct {
	logger log.Logger

	ruleManager *rules.Manager
	queryURL    string
}

func New(logger log.Logger, ruleManager *rules.Manager, queryURL string) *UI {
	return &UI{
		logger:      logger,
		ruleManager: ruleManager,
		queryURL:    queryURL,
	}
}

// web/web.go
func (h *UI) alerts(w http.ResponseWriter, r *http.Request) {
	alerts := h.ruleManager.AlertingRules()
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
	h.executeTemplate(w, "alerts.html", alertStatus)
}

func (h *UI) rules(w http.ResponseWriter, r *http.Request) {
	h.executeTemplate(w, "rules.html", h.ruleManager)
}

func (u *UI) Register(r *route.Router) {
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/alerts", http.StatusFound)
	})

	instrf := prometheus.InstrumentHandlerFunc

	r.Get("/alerts", instrf("alerts", u.alerts))
	r.Get("/rules", instrf("rules", u.rules))

	r.Get("/static/*filepath", instrf("static", u.serveStaticAsset))
}

func (u *UI) tmplFuncs() template.FuncMap {
	return template.FuncMap{
		"queryURL":     func() string { return u.queryURL },
		"pathPrefix":   func() string { return "" },
		"buildVersion": func() string { return version.Revision },
		"reReplaceAll": func(pattern, repl, text string) string {
			re := regexp.MustCompile(pattern)
			return re.ReplaceAllString(text, repl)
		},
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
		"humanizeDuration": func(v float64) string {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return fmt.Sprintf("%.4g", v)
			}
			if v == 0 {
				return fmt.Sprintf("%.4gs", v)
			}
			if math.Abs(v) >= 1 {
				sign := ""
				if v < 0 {
					sign = "-"
					v = -v
				}
				seconds := int64(v) % 60
				minutes := (int64(v) / 60) % 60
				hours := (int64(v) / 60 / 60) % 24
				days := (int64(v) / 60 / 60 / 24)
				// For days to minutes, we display seconds as an integer.
				if days != 0 {
					return fmt.Sprintf("%s%dd %dh %dm %ds", sign, days, hours, minutes, seconds)
				}
				if hours != 0 {
					return fmt.Sprintf("%s%dh %dm %ds", sign, hours, minutes, seconds)
				}
				if minutes != 0 {
					return fmt.Sprintf("%s%dm %ds", sign, minutes, seconds)
				}
				// For seconds, we display 4 significant digts.
				return fmt.Sprintf("%s%.4gs", sign, v)
			}
			prefix := ""
			for _, p := range []string{"m", "u", "n", "p", "f", "a", "z", "y"} {
				if math.Abs(v) >= 1 {
					break
				}
				prefix = p
				v *= 1000
			}
			return fmt.Sprintf("%.4g%ss", v, prefix)
		},
	}
}

func (u *UI) getTemplate(name string) (string, error) {
	baseTmpl, err := Asset("pkg/rule/ui/templates/_base.html")
	if err != nil {
		return "", fmt.Errorf("error reading base template: %s", err)
	}
	pageTmpl, err := Asset(filepath.Join("pkg/rule/ui/templates", name))
	if err != nil {
		return "", fmt.Errorf("error reading page template %s: %s", name, err)
	}
	return string(baseTmpl) + string(pageTmpl), nil
}

func (u *UI) executeTemplate(w http.ResponseWriter, name string, data interface{}) {
	text, err := u.getTemplate(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	t, err := template.New("").Funcs(u.tmplFuncs()).Parse(text)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		level.Warn(u.logger).Log("msg", "template expansion failed", "err", err)
	}
}

func (u *UI) serveStaticAsset(w http.ResponseWriter, req *http.Request) {
	fp := route.Param(req.Context(), "filepath")
	fp = filepath.Join("pkg/rule/ui/static", fp)

	info, err := AssetInfo(fp)
	if err != nil {
		level.Warn(u.logger).Log("msg", "Could not get file info", "err", err, "file", fp)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	file, err := Asset(fp)
	if err != nil {
		if err != io.EOF {
			level.Warn(u.logger).Log("msg", "Could not get file", "err", err, "file", fp)
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}

	http.ServeContent(w, req, info.Name(), info.ModTime(), bytes.NewReader(file))
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
