package thanosrule

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/rules"
	tsdb_errors "github.com/prometheus/tsdb/errors"
	yaml "gopkg.in/yaml.v2"
)

const tmpRuleDir = ".tmp-rules"

type Group struct {
	*rules.Group
	PartialResponseStrategy storepb.PartialResponseStrategy
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

type Managers map[storepb.PartialResponseStrategy]*rules.Manager

func (m Managers) RuleGroups() []Group {
	var res []Group
	for s, r := range m {
		for _, group := range r.RuleGroups() {
			res = append(res, Group{Group: group, PartialResponseStrategy: s})
		}
	}
	return res
}

func (m Managers) AlertingRules() []AlertingRule {
	var res []AlertingRule
	for s, r := range m {
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
		return errors.Wrapf(err, errMsg)
	}

	rg := rulefmt.RuleGroup{}
	if err := unmarshal(&rg); err != nil {
		return errors.Wrapf(err, errMsg)
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
func (m *Managers) Update(dataDir string, evalInterval time.Duration, files []string) error {
	var (
		errs     tsdb_errors.MultiError
		filesMap = map[storepb.PartialResponseStrategy][]string{}
	)

	if err := os.RemoveAll(path.Join(dataDir, tmpRuleDir)); err != nil {
		return errors.Wrapf(err, "rm %s", path.Join(dataDir, tmpRuleDir))
	}
	if err := os.MkdirAll(path.Join(dataDir, tmpRuleDir), os.ModePerm); err != nil {
		return errors.Wrapf(err, "mkdir %s", path.Join(dataDir, tmpRuleDir))
	}

	for _, fn := range files {
		b, err := ioutil.ReadFile(fn)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		var rg RuleGroups
		if err := yaml.Unmarshal(b, &rg); err != nil {
			errs = append(errs, err)
			continue
		}

		// NOTE: This is very ugly, but we need to reparse it into tmp dir without the field to have to reuse
		// rules.Manager. The problem is that it uses yaml.UnmarshalStrict for some reasons.
		mapped := map[storepb.PartialResponseStrategy]*rulefmt.RuleGroups{}
		for _, rg := range rg.Groups {
			if _, ok := mapped[*rg.PartialResponseStrategy]; !ok {
				mapped[*rg.PartialResponseStrategy] = &rulefmt.RuleGroups{}
			}

			mapped[*rg.PartialResponseStrategy].Groups = append(
				mapped[*rg.PartialResponseStrategy].Groups,
				rg.RuleGroup,
			)
		}

		for s, rg := range mapped {
			b, err := yaml.Marshal(rg)
			if err != nil {
				errs = append(errs, err)
				continue
			}

			newFn := path.Join(dataDir, tmpRuleDir, filepath.Base(fn)+"."+s.String())
			if err := ioutil.WriteFile(newFn, b, os.ModePerm); err != nil {
				errs = append(errs, err)
				continue
			}

			filesMap[s] = append(filesMap[s], newFn)
		}

	}

	for s, fs := range filesMap {
		updater, ok := (*m)[s]
		if !ok {
			errs = append(errs, errors.Errorf("no updater found for %v", s))
			continue
		}
		if err := updater.Update(evalInterval, fs); err != nil {
			errs = append(errs, err)
			continue
		}
	}

	return errs.Err()
}
