// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package thanosrule

import (
	"crypto/sha256"
	"fmt"
	html_template "html/template"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/rules"
	tsdberrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/util/strutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"gopkg.in/yaml.v2"
)

const tmpRuleDir = ".tmp-rules"

type Group struct {
	*rules.Group
	originalFile            string
	PartialResponseStrategy storepb.PartialResponseStrategy
}

func (g Group) OriginalFile() string {
	return g.originalFile
}

// GetRules returns a list of rules, which is used in UI rules page.
func (g *Group) GetRules() []rules.Rule {
	var rs []rules.Rule
	for _, rule := range g.Rules() {
		switch r := rule.(type) {
		case *rules.AlertingRule:
			rs = append(rs, &AlertingRule{r, g.PartialResponseStrategy})
		case *rules.RecordingRule:
			rs = append(rs, &RecordingRule{r, g.PartialResponseStrategy})
		}
	}
	return rs
}

// GetAlertingRules returns a list of alerting rules, which is used in UI alerts page.
func (g Group) GetAlertingRules() []*AlertingRule {
	var alerts []*AlertingRule
	for _, rule := range g.Rules() {
		if r, ok := rule.(*rules.AlertingRule); ok {
			alerts = append(alerts, &AlertingRule{r, g.PartialResponseStrategy})
		}
	}
	sort.Slice(alerts, func(i, j int) bool {
		return alerts[i].State() > alerts[j].State() ||
			(alerts[i].State() == alerts[j].State() &&
				alerts[i].Name() < alerts[j].Name())
	})
	return alerts
}

type AlertingRule struct {
	*rules.AlertingRule
	PartialResponseStrategy storepb.PartialResponseStrategy
}

// HTMLSnippet returns a human-readable string representation of alerting rules,
// decorated with HTML elements for use the web frontend.
func (r *AlertingRule) HTMLSnippet(pathPrefix string) html_template.HTML {
	alertMetric := model.Metric{
		model.MetricNameLabel: "ALERTS",
		"alertname":           model.LabelValue(r.Name()),
	}

	labelsMap := make(map[string]string, len(r.Labels()))
	for _, l := range r.Labels() {
		labelsMap[l.Name] = html_template.HTMLEscapeString(l.Value)
	}

	annotations := r.Annotations()
	annotationsMap := make(map[string]string, len(annotations))
	for _, l := range annotations {
		annotationsMap[l.Name] = html_template.HTMLEscapeString(l.Value)
	}

	ar := ruleFormat{
		Alert:                   fmt.Sprintf("<a href=%q>%s</a>", pathPrefix+strutil.TableLinkForExpression(alertMetric.String()), r.Name()),
		Expr:                    fmt.Sprintf("<a href=%q>%s</a>", pathPrefix+strutil.TableLinkForExpression(r.Query().String()), html_template.HTMLEscapeString(r.Query().String())),
		For:                     model.Duration(r.HoldDuration()),
		Labels:                  labelsMap,
		Annotations:             annotationsMap,
		PartialResponseStrategy: storepb.PartialResponseStrategy_name[int32(r.PartialResponseStrategy)],
	}

	byt, err := yaml.Marshal(ar)
	if err != nil {
		return html_template.HTML(fmt.Sprintf("error marshaling alerting rule: %q", html_template.HTMLEscapeString(err.Error())))
	}
	return html_template.HTML(byt)
}

// RecordingRule represents a recording rule in ruler UI.
type RecordingRule struct {
	*rules.RecordingRule
	PartialResponseStrategy storepb.PartialResponseStrategy
}

// HTMLSnippet returns a human-readable string representation of recording rules,
// decorated with HTML elements for use the web frontend.
func (r *RecordingRule) HTMLSnippet(pathPrefix string) html_template.HTML {
	ruleExpr := r.Query().String()
	labels := make(map[string]string, len(r.Labels()))
	for _, l := range r.Labels() {
		labels[l.Name] = html_template.HTMLEscapeString(l.Value)
	}

	rr := ruleFormat{
		Record:                  fmt.Sprintf(`<a href="%s">%s</a>`, pathPrefix+strutil.TableLinkForExpression(r.Name()), r.Name()),
		Expr:                    fmt.Sprintf(`<a href="%s">%s</a>`, pathPrefix+strutil.TableLinkForExpression(ruleExpr), html_template.HTMLEscapeString(ruleExpr)),
		Labels:                  labels,
		PartialResponseStrategy: storepb.PartialResponseStrategy_name[int32(r.PartialResponseStrategy)],
	}

	byt, err := yaml.Marshal(rr)
	if err != nil {
		return html_template.HTML(fmt.Sprintf("error marshaling recording rule: %q", html_template.HTMLEscapeString(err.Error())))
	}

	return html_template.HTML(byt)
}

type ruleFormat struct {
	Record                  string            `yaml:"record,omitempty"`
	Alert                   string            `yaml:"alert,omitempty"`
	Expr                    string            `yaml:"expr"`
	For                     model.Duration    `yaml:"for,omitempty"`
	Labels                  map[string]string `yaml:"labels,omitempty"`
	Annotations             map[string]string `yaml:"annotations,omitempty"`
	PartialResponseStrategy string            `yaml:"partial_response_strategy,omitempty"`
}

type RuleGroups struct {
	Groups []RuleGroup `yaml:"groups"`
}

type RuleGroup struct {
	rulefmt.RuleGroup
	PartialResponseStrategy *storepb.PartialResponseStrategy
}

type Manager struct {
	workDir string
	mgrs    map[storepb.PartialResponseStrategy]*rules.Manager

	mtx       sync.RWMutex
	ruleFiles map[string]string
}

func NewManager(dataDir string) *Manager {
	return &Manager{
		workDir:   filepath.Join(dataDir, tmpRuleDir),
		mgrs:      make(map[storepb.PartialResponseStrategy]*rules.Manager),
		ruleFiles: make(map[string]string),
	}
}

func (m *Manager) SetRuleManager(s storepb.PartialResponseStrategy, mgr *rules.Manager) {
	m.mgrs[s] = mgr
}

func (m *Manager) RuleGroups() []Group {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	var res []Group
	for s, r := range m.mgrs {
		for _, group := range r.RuleGroups() {
			res = append(res, Group{
				Group:                   group,
				PartialResponseStrategy: s,
				originalFile:            m.ruleFiles[group.File()],
			})
		}
	}
	return res
}

func (m *Manager) AlertingRules() []AlertingRule {
	var res []AlertingRule
	for s, r := range m.mgrs {
		for _, r := range r.AlertingRules() {
			res = append(res, AlertingRule{AlertingRule: r, PartialResponseStrategy: s})
		}
	}
	return res
}

func (r *RuleGroup) UnmarshalYAML(unmarshal func(interface{}) error) error {
	rs := struct {
		String string `yaml:"partial_response_strategy"`
	}{}

	errMsg := fmt.Sprintf("failed to unmarshal 'partial_response_strategy'. Possible values are %s", strings.Join(storepb.PartialResponseStrategyValues, ","))
	if err := unmarshal(&rs); err != nil {
		return errors.Wrap(err, errMsg)
	}

	rg := rulefmt.RuleGroup{}
	if err := unmarshal(&rg); err != nil {
		return errors.Wrap(err, "failed to unmarshal rulefmt.RuleGroup")
	}

	p, ok := storepb.PartialResponseStrategy_value[strings.ToUpper(rs.String)]
	if !ok {
		if rs.String != "" {
			return errors.Errorf("%s. Got: %s", errMsg, rs.String)
		}

		// NOTE: For Rule default is abort as this is recommended for alerting.
		p = storepb.PartialResponseStrategy_value[storepb.PartialResponseStrategy_ABORT.String()]
	}

	ps := storepb.PartialResponseStrategy(p)
	r.RuleGroup = rg
	r.PartialResponseStrategy = &ps
	return nil
}

func (r RuleGroup) MarshalYAML() (interface{}, error) {
	var ps *string
	if r.PartialResponseStrategy != nil {
		str := r.PartialResponseStrategy.String()
		ps = &str
	}

	rs := struct {
		RuleGroup               rulefmt.RuleGroup `yaml:",inline"`
		PartialResponseStrategy *string           `yaml:"partial_response_strategy,omitempty"`
	}{
		RuleGroup:               r.RuleGroup,
		PartialResponseStrategy: ps,
	}
	return rs, nil
}

// Update updates rules from given files to all managers we hold. We decide which groups should go where, based on
// special field in RuleGroup file.
func (m *Manager) Update(evalInterval time.Duration, files []string) error {
	var (
		errs            tsdberrors.MultiError
		filesByStrategy = map[storepb.PartialResponseStrategy][]string{}
		ruleFiles       = map[string]string{}
	)

	if err := os.RemoveAll(m.workDir); err != nil {
		return errors.Wrapf(err, "failed to remove %s", m.workDir)
	}
	if err := os.MkdirAll(m.workDir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "failed to create %s", m.workDir)
	}

	for _, fn := range files {
		b, err := ioutil.ReadFile(fn)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var rg RuleGroups
		if err := yaml.Unmarshal(b, &rg); err != nil {
			errs = append(errs, errors.Wrap(err, fn))
			continue
		}

		// NOTE: This is very ugly, but we need to reparse it into tmp dir without the field to have to reuse
		// rules.Manager. The problem is that it uses yaml.UnmarshalStrict for some reasons.
		groupsByStrategy := map[storepb.PartialResponseStrategy]*rulefmt.RuleGroups{}
		for _, rg := range rg.Groups {
			if _, ok := groupsByStrategy[*rg.PartialResponseStrategy]; !ok {
				groupsByStrategy[*rg.PartialResponseStrategy] = &rulefmt.RuleGroups{}
			}

			groupsByStrategy[*rg.PartialResponseStrategy].Groups = append(
				groupsByStrategy[*rg.PartialResponseStrategy].Groups,
				rg.RuleGroup,
			)
		}

		for s, rg := range groupsByStrategy {
			b, err := yaml.Marshal(rg)
			if err != nil {
				errs = append(errs, errors.Wrapf(err, "%s: failed to marshal rule groups", fn))
				continue
			}

			newFn := filepath.Join(m.workDir, fmt.Sprintf("%s.%x.%s", filepath.Base(fn), sha256.Sum256([]byte(fn)), s.String()))
			if err := ioutil.WriteFile(newFn, b, os.ModePerm); err != nil {
				errs = append(errs, errors.Wrap(err, newFn))
				continue
			}

			filesByStrategy[s] = append(filesByStrategy[s], newFn)
			ruleFiles[newFn] = fn
		}
	}

	m.mtx.Lock()
	for s, fs := range filesByStrategy {
		mgr, ok := m.mgrs[s]
		if !ok {
			errs = append(errs, errors.Errorf("no manager found for %v", s))
			continue
		}
		// We add external labels in `pkg/alert.Queue`.
		// TODO(bwplotka): Investigate if we should put ext labels here or not.
		if err := mgr.Update(evalInterval, fs, nil); err != nil {
			errs = append(errs, errors.Wrapf(err, "strategy %s", s))
			continue
		}
	}
	m.ruleFiles = ruleFiles
	m.mtx.Unlock()

	return errs.Err()
}
