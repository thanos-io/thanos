// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/rules"
	tsdberrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"gopkg.in/yaml.v2"
)

const tmpRuleDir = ".tmp-rules"

type Group struct {
	*rules.Group
	originalFile            string
	PartialResponseStrategy storepb.PartialResponseStrategy
}

func (g Group) toProto() *rulespb.RuleGroup {
	ret := &rulespb.RuleGroup{
		Name:                    g.Name(),
		File:                    g.originalFile,
		Interval:                g.Interval().Seconds(),
		PartialResponseStrategy: g.PartialResponseStrategy,
	}

	for _, r := range g.Rules() {
		lastError := ""
		if r.LastError() != nil {
			lastError = r.LastError().Error()
		}

		switch rule := r.(type) {
		case *rules.AlertingRule:
			ret.Rules = append(ret.Rules, &rulespb.Rule{
				Result: &rulespb.Rule_Alert{Alert: &rulespb.Alert{
					State:                     rulespb.AlertState(rule.State()),
					Name:                      rule.Name(),
					Query:                     rule.Query().String(),
					DurationSeconds:           rule.Duration().Seconds(),
					Labels:                    rulespb.PromLabels{Labels: storepb.PromLabelsToLabels(rule.Labels())},
					Annotations:               rulespb.PromLabels{Labels: storepb.PromLabelsToLabels(rule.Annotations())},
					Alerts:                    ActiveAlertsToProto(g.PartialResponseStrategy, rule),
					Health:                    string(rule.Health()),
					LastError:                 lastError,
					EvaluationDurationSeconds: rule.GetEvaluationDuration().Seconds(),
					LastEvaluation:            rule.GetEvaluationTimestamp(),
				}}})
		case *rules.RecordingRule:
			ret.Rules = append(ret.Rules, &rulespb.Rule{
				Result: &rulespb.Rule_Recording{Recording: &rulespb.RecordingRule{
					Name:                      rule.Name(),
					Query:                     rule.Query().String(),
					Labels:                    rulespb.PromLabels{Labels: storepb.PromLabelsToLabels(rule.Labels())},
					Health:                    string(rule.Health()),
					LastError:                 lastError,
					EvaluationDurationSeconds: rule.GetEvaluationDuration().Seconds(),
					LastEvaluation:            rule.GetEvaluationTimestamp(),
				}}})
		default:
			// We cannot do much, let's panic, API will recover.
			panic(fmt.Sprintf("rule %q: unsupported type %T", r.Name(), rule))
		}
	}
	return ret
}

func ActiveAlertsToProto(s storepb.PartialResponseStrategy, a *rules.AlertingRule) []*rulespb.AlertInstance {
	active := a.ActiveAlerts()
	ret := make([]*rulespb.AlertInstance, len(active))
	for i, ruleAlert := range active {
		ret[i] = &rulespb.AlertInstance{
			PartialResponseStrategy: s,
			Labels:                  rulespb.PromLabels{Labels: storepb.PromLabelsToLabels(ruleAlert.Labels)},
			Annotations:             rulespb.PromLabels{Labels: storepb.PromLabelsToLabels(ruleAlert.Annotations)},
			State:                   rulespb.AlertState(ruleAlert.State),
			ActiveAt:                &ruleAlert.ActiveAt,
			Value:                   strconv.FormatFloat(ruleAlert.Value, 'e', -1, 64),
		}
	}
	return ret
}

// configRuleGroups is what is parsed from config.
type configRuleGroups struct {
	Groups []configRuleGroup `yaml:"groups"`
}

type configRuleGroup struct {
	rulefmt.RuleGroup
	PartialResponseStrategy *storepb.PartialResponseStrategy
}

// Manager is a partial response strategy and proto compatible Manager.
// Manager also implements rulespb.Rules gRPC service.
type Manager struct {
	workDir string
	mgrs    map[storepb.PartialResponseStrategy]*rules.Manager
	extLset labels.Labels

	mtx       sync.RWMutex
	ruleFiles map[string]string
}

// NewManager creates new Manager.
// QueryFunc from baseOpts will be rewritten.
func NewManager(
	ctx context.Context,
	reg prometheus.Registerer,
	dataDir string,
	baseOpts rules.ManagerOptions,
	queryFuncCreator func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc,
	extLset labels.Labels,
) *Manager {
	m := &Manager{
		workDir:   filepath.Join(dataDir, tmpRuleDir),
		mgrs:      make(map[storepb.PartialResponseStrategy]*rules.Manager),
		extLset:   extLset,
		ruleFiles: make(map[string]string),
	}
	for _, strategy := range storepb.PartialResponseStrategy_value {
		s := storepb.PartialResponseStrategy(strategy)

		opts := baseOpts
		opts.Registerer = extprom.WrapRegistererWith(prometheus.Labels{"strategy": strings.ToLower(s.String())}, reg)
		opts.Context = ctx
		opts.QueryFunc = queryFuncCreator(s)

		m.mgrs[s] = rules.NewManager(&opts)
	}

	return m
}

func (m *Manager) Run() {
	for _, mgr := range m.mgrs {
		mgr.Run()
	}
}

func (m *Manager) Stop() {
	for _, mgr := range m.mgrs {
		mgr.Stop()
	}
}

func (m *Manager) protoRuleGroups() []*rulespb.RuleGroup {
	rg := m.RuleGroups()
	res := make([]*rulespb.RuleGroup, 0, len(rg))
	for _, g := range rg {
		res = append(res, g.toProto())
	}
	return res
}

func (m *Manager) RuleGroups() []Group {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	var res []Group
	for s, r := range m.mgrs {
		for _, group := range r.RuleGroups() {
			res = append(res, Group{
				Group:                   group,
				originalFile:            m.ruleFiles[group.File()],
				PartialResponseStrategy: s,
			})
		}
	}
	return res
}

func (m *Manager) Active() []*rulespb.AlertInstance {
	var res []*rulespb.AlertInstance
	for s, r := range m.mgrs {
		for _, r := range r.AlertingRules() {
			res = append(res, ActiveAlertsToProto(s, r)...)
		}
	}
	return res
}

func (r *configRuleGroup) UnmarshalYAML(unmarshal func(interface{}) error) error {
	rs := struct {
		Strategy string `yaml:"partial_response_strategy"`
	}{}

	if err := unmarshal(&rs); err != nil {
		return err
	}

	r.PartialResponseStrategy = new(storepb.PartialResponseStrategy)

	// Same as YAMl. Quote as JSON unmarshal expects raw JSON field.
	if err := r.PartialResponseStrategy.UnmarshalJSON([]byte("\"" + rs.Strategy + "\"")); err != nil {
		return err
	}

	rg := rulefmt.RuleGroup{}
	if err := unmarshal(&rg); err != nil {
		return errors.Wrap(err, "failed to unmarshal rulefmt.configRuleGroup")
	}
	r.RuleGroup = rg
	return nil
}

func (r configRuleGroup) MarshalYAML() (interface{}, error) {
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
// special field in configRuleGroup file.
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

		var rg configRuleGroups
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

// Rules returns specified rules from manager. This is used by gRPC and locally for HTTP and UI purposes.
func (m *Manager) Rules(r *rulespb.RulesRequest, s rulespb.Rules_RulesServer) error {
	groups := m.protoRuleGroups()

	pgs := make([]*rulespb.RuleGroup, 0, len(groups))
	for _, g := range groups {
		if r.Type == rulespb.RulesRequest_ALL {
			pgs = append(pgs, g)
			continue
		}

		filtered := proto.Clone(g).(*rulespb.RuleGroup)
		filtered.Rules = nil
		for _, rule := range g.Rules {
			if rule.GetAlert() != nil && r.Type == rulespb.RulesRequest_ALERT {
				filtered.Rules = append(filtered.Rules, rule)
				continue
			}
			if rule.GetRecording() != nil && r.Type == rulespb.RulesRequest_RECORD {
				filtered.Rules = append(filtered.Rules, rule)
			}
		}
		pgs = append(pgs, filtered)
	}

	enrichRulesWithExtLabels(pgs, m.extLset)

	for _, pg := range pgs {
		if err := s.Send(&rulespb.RulesResponse{Result: &rulespb.RulesResponse_Group{Group: pg}}); err != nil {
			return err
		}
	}
	return nil
}
