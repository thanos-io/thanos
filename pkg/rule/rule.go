package thanosrule

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/rules"
	tsdberrors "github.com/prometheus/prometheus/tsdb/errors"
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

type AlertingRule struct {
	*rules.AlertingRule
	PartialResponseStrategy storepb.PartialResponseStrategy
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
