// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/rules"
	"gopkg.in/yaml.v3"

	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const tmpRuleDir = ".tmp-rules"

type Group struct {
	*rules.Group
	OriginalFile            string
	PartialResponseStrategy storepb.PartialResponseStrategy
}

func (g Group) toProto() *rulespb.RuleGroup {
	ret := &rulespb.RuleGroup{
		Name:                    g.Name(),
		File:                    g.OriginalFile,
		Interval:                g.Interval().Seconds(),
		Limit:                   int64(g.Limit()),
		PartialResponseStrategy: g.PartialResponseStrategy,
		// UTC needed due to https://github.com/gogo/protobuf/issues/519.
		LastEvaluation:            g.GetLastEvaluation().UTC(),
		EvaluationDurationSeconds: g.GetEvaluationTime().Seconds(),
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
					DurationSeconds:           rule.HoldDuration().Seconds(),
					Labels:                    labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(rule.Labels())},
					Annotations:               labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(rule.Annotations())},
					Alerts:                    ActiveAlertsToProto(g.PartialResponseStrategy, rule),
					Health:                    string(rule.Health()),
					LastError:                 lastError,
					EvaluationDurationSeconds: rule.GetEvaluationDuration().Seconds(),
					// UTC needed due to https://github.com/gogo/protobuf/issues/519.
					LastEvaluation: rule.GetEvaluationTimestamp().UTC(),
				}}})
		case *rules.RecordingRule:
			ret.Rules = append(ret.Rules, &rulespb.Rule{
				Result: &rulespb.Rule_Recording{Recording: &rulespb.RecordingRule{
					Name:                      rule.Name(),
					Query:                     rule.Query().String(),
					Labels:                    labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(rule.Labels())},
					Health:                    string(rule.Health()),
					LastError:                 lastError,
					EvaluationDurationSeconds: rule.GetEvaluationDuration().Seconds(),
					// UTC needed due to https://github.com/gogo/protobuf/issues/519.
					LastEvaluation: rule.GetEvaluationTimestamp().UTC(),
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
		// UTC needed due to https://github.com/gogo/protobuf/issues/519.
		activeAt := ruleAlert.ActiveAt.UTC()
		ret[i] = &rulespb.AlertInstance{
			PartialResponseStrategy: s,
			Labels:                  labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(ruleAlert.Labels)},
			Annotations:             labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(ruleAlert.Annotations)},
			State:                   rulespb.AlertState(ruleAlert.State),
			ActiveAt:                &activeAt,
			Value:                   strconv.FormatFloat(ruleAlert.Value, 'e', -1, 64),
		}
	}
	return ret
}

// Manager is a partial response strategy and proto compatible Manager.
// Manager also implements rulespb.Rules gRPC service.
type Manager struct {
	workDir string
	mgrs    map[storepb.PartialResponseStrategy]*rules.Manager
	extLset labels.Labels

	mtx         sync.RWMutex
	ruleFiles   map[string]string
	externalURL string
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
	externalURL string,
) *Manager {
	m := &Manager{
		workDir:     filepath.Join(dataDir, tmpRuleDir),
		mgrs:        make(map[storepb.PartialResponseStrategy]*rules.Manager),
		extLset:     extLset,
		ruleFiles:   make(map[string]string),
		externalURL: externalURL,
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

// Run is non blocking, in opposite to TSDB manager, which is blocking.
func (m *Manager) Run() {
	for _, mgr := range m.mgrs {
		go mgr.Run()
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
				OriginalFile:            m.ruleFiles[group.File()],
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

type configRuleAdapter struct {
	PartialResponseStrategy *storepb.PartialResponseStrategy

	group           rulefmt.RuleGroup
	nativeRuleGroup map[string]interface{}
}

func (g *configRuleAdapter) UnmarshalYAML(unmarshal func(interface{}) error) error {
	rs := struct {
		RuleGroup rulefmt.RuleGroup `yaml:",inline"`
		Strategy  string            `yaml:"partial_response_strategy"`
	}{}

	if err := unmarshal(&rs); err != nil {
		return err
	}

	g.PartialResponseStrategy = new(storepb.PartialResponseStrategy)
	// Same as YAMl. Quote as JSON unmarshal expects raw JSON field.
	if err := g.PartialResponseStrategy.UnmarshalJSON([]byte("\"" + rs.Strategy + "\"")); err != nil {
		return err
	}
	g.group = rs.RuleGroup

	var native map[string]interface{}
	if err := unmarshal(&native); err != nil {
		return errors.Wrap(err, "failed to unmarshal rulefmt.configRuleAdapter")
	}
	delete(native, "partial_response_strategy")

	g.nativeRuleGroup = native
	return nil
}

func (g configRuleAdapter) MarshalYAML() (interface{}, error) {
	return struct {
		RuleGroup map[string]interface{} `yaml:",inline"`
	}{
		RuleGroup: g.nativeRuleGroup,
	}, nil
}

// TODO(bwplotka): Replace this with upstream implementation after https://github.com/prometheus/prometheus/issues/7128 is fixed.
func (g configRuleAdapter) validate() (errs []error) {
	set := map[string]struct{}{}
	if g.group.Name == "" {
		errs = append(errs, errors.New("Groupname should not be empty"))
	}

	if _, ok := set[g.group.Name]; ok {
		errs = append(
			errs,
			fmt.Errorf("groupname: %q is repeated in the same file", g.group.Name),
		)
	}

	set[g.group.Name] = struct{}{}

	for i, r := range g.group.Rules {
		for _, node := range r.Validate() {
			var ruleName string
			if r.Alert.Value != "" {
				ruleName = r.Alert.Value
			} else {
				ruleName = r.Record.Value
			}
			errs = append(errs, &rulefmt.Error{
				Group:    g.group.Name,
				Rule:     i,
				RuleName: ruleName,
				Err:      node,
			})
		}
	}

	return errs
}

// ValidateAndCount validates all rules in the rule groups and return overal number of rules in all groups.
// TODO(bwplotka): Replace this with upstream implementation after https://github.com/prometheus/prometheus/issues/7128 is fixed.
func ValidateAndCount(group io.Reader) (numRules int, errs errutil.MultiError) {
	var rgs configGroups
	d := yaml.NewDecoder(group)
	d.KnownFields(true)
	if err := d.Decode(&rgs); err != nil {
		errs.Add(err)
		return 0, errs
	}

	for _, g := range rgs.Groups {
		if err := g.validate(); err != nil {
			for _, e := range err {
				errs.Add(e)
			}
			return 0, errs
		}
	}

	for _, rg := range rgs.Groups {
		numRules += len(rg.group.Rules)
	}
	return numRules, errs
}

type configGroups struct {
	Groups []configRuleAdapter `yaml:"groups"`
}

// Update updates rules from given files to all managers we hold. We decide which groups should go where, based on
// special field in configGroups.configRuleAdapter struct.
func (m *Manager) Update(evalInterval time.Duration, files []string) error {
	var (
		errs            errutil.MultiError
		filesByStrategy = map[storepb.PartialResponseStrategy][]string{}
		ruleFiles       = map[string]string{}
	)

	// Initialize filesByStrategy for existing managers' strategies to make
	// sure that managers are updated when they have no rules configured.
	for strategy := range m.mgrs {
		filesByStrategy[strategy] = make([]string, 0)
	}

	if err := os.RemoveAll(m.workDir); err != nil {
		return errors.Wrapf(err, "remove %s", m.workDir)
	}
	if err := os.MkdirAll(m.workDir, os.ModePerm); err != nil {
		return errors.Wrapf(err, "create %s", m.workDir)
	}

	for _, fn := range files {
		b, err := os.ReadFile(filepath.Clean(fn))
		if err != nil {
			errs.Add(err)
			continue
		}

		var rg configGroups
		if err := yaml.Unmarshal(b, &rg); err != nil {
			errs.Add(errors.Wrap(err, fn))
			continue
		}

		// NOTE: This is very ugly, but we need to write those yaml into tmp dir without the partial partial response field
		// which is not supported, to be able to reuse rules.Manager. The problem is that it uses yaml.UnmarshalStrict.
		groupsByStrategy := map[storepb.PartialResponseStrategy][]configRuleAdapter{}
		for _, rg := range rg.Groups {
			groupsByStrategy[*rg.PartialResponseStrategy] = append(groupsByStrategy[*rg.PartialResponseStrategy], rg)
		}
		for s, rg := range groupsByStrategy {
			b, err := yaml.Marshal(configGroups{Groups: rg})
			if err != nil {
				errs = append(errs, errors.Wrapf(err, "%s: failed to marshal rule groups", fn))
				continue
			}

			// Use full file name appending to work dir, so we can differentiate between different dirs and same filenames(!).
			// This will be also used as key for file group name.
			newFn := filepath.Join(m.workDir, s.String(), fn)
			if err := os.MkdirAll(filepath.Dir(newFn), os.ModePerm); err != nil {
				errs.Add(errors.Wrapf(err, "create %s", filepath.Dir(newFn)))
				continue
			}
			if err := os.WriteFile(newFn, b, os.ModePerm); err != nil {
				errs.Add(errors.Wrapf(err, "write file %v", newFn))
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
			errs.Add(errors.Errorf("no manager found for %v", s))
			continue
		}
		// We add external labels in `pkg/alert.Queue`.
		if err := mgr.Update(evalInterval, fs, m.extLset, m.externalURL, nil); err != nil {
			// TODO(bwplotka): Prometheus logs all error details. Fix it upstream to have consistent error handling.
			errs.Add(errors.Wrapf(err, "strategy %s, update rules", s))
			continue
		}
	}
	m.ruleFiles = ruleFiles
	m.mtx.Unlock()

	return errs.Err()
}

// Rules returns specified rules from manager. This is used by gRPC and locally for HTTP and UI purposes.
func (m *Manager) Rules(r *rulespb.RulesRequest, s rulespb.Rules_RulesServer) (err error) {
	groups := m.protoRuleGroups()

	pgs := make([]*rulespb.RuleGroup, 0, len(groups))
	for _, g := range groups {
		// UTC needed due to https://github.com/gogo/protobuf/issues/519.
		g.LastEvaluation = g.LastEvaluation.UTC()
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
		tracing.DoInSpan(s.Context(), "send_rule_group_response", func(_ context.Context) {
			err = s.Send(&rulespb.RulesResponse{Result: &rulespb.RulesResponse_Group{Group: pg}})
		})
		if err != nil {
			return err
		}
	}
	return nil
}
