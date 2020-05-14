// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"fmt"
	"io/ioutil"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
	yamlv3 "gopkg.in/yaml.v3"

	thanosrule "github.com/thanos-io/thanos/pkg/rule"
)

func registerTools(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command("tools", "Tools utility commands")

	registerBucket(m, cmd, "tools")
	registerCheckRules(m, cmd, "tools")
}

func registerCheckRules(m map[string]setupFunc, app *kingpin.CmdClause, pre string) {
	checkRulesCmd := app.Command("rules-check", "Check if the rule files are valid or not.")
	ruleFiles := checkRulesCmd.Flag("rules", "The rule files glob to check (repeated).").Required().ExistingFiles()

	m[pre+" rules-check"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})
		return checkRulesFiles(logger, ruleFiles)
	}
}

func checkRulesFiles(logger log.Logger, files *[]string) error {
	failed := tsdb_errors.MultiError{}

	for _, f := range *files {
		n, errs := checkRules(logger, f)
		if errs.Err() != nil {
			level.Error(logger).Log("result", "FAILED")
			for _, e := range errs {
				level.Error(logger).Log("error", e.Error())
				failed.Add(e)
			}
			level.Info(logger).Log()
			continue
		}
		level.Info(logger).Log("result", "SUCCESS", "rules found", n)
	}
	if failed.Err() != nil {
		return failed
	}
	return nil
}

type ThanosRuleGroup struct {
	PartialResponseStrategy  string `yaml:"partial_response_strategy"`
	thanosrule.PromRuleGroup `yaml:",inline"`
}

type ThanosRuleGroups struct {
	Groups []ThanosRuleGroup `yaml:"groups"`
}

// Validate validates all rules in the rule groups.
// This is copied from Prometheus.
// TODO: Replace this with upstream implementation after https://github.com/prometheus/prometheus/issues/7128 is fixed.
func (g *ThanosRuleGroups) Validate() (errs []error) {
	set := map[string]struct{}{}

	for _, g := range g.Groups {
		if g.Name == "" {
			errs = append(errs, errors.New("Groupname should not be empty"))
		}

		if _, ok := set[g.Name]; ok {
			errs = append(
				errs,
				fmt.Errorf("groupname: \"%s\" is repeated in the same file", g.Name),
			)
		}

		set[g.Name] = struct{}{}

		for i, r := range g.Rules {
			ruleNode := rulefmt.RuleNode{
				Record:      yamlv3.Node{Value: r.Record},
				Alert:       yamlv3.Node{Value: r.Alert},
				Expr:        yamlv3.Node{Value: r.Expr},
				For:         r.For,
				Labels:      r.Labels,
				Annotations: r.Annotations,
			}
			for _, node := range ruleNode.Validate() {
				var ruleName string
				if r.Alert != "" {
					ruleName = r.Alert
				} else {
					ruleName = r.Record
				}
				errs = append(errs, &rulefmt.Error{
					Group:    g.Name,
					Rule:     i,
					RuleName: ruleName,
					Err:      node,
				})
			}
		}
	}

	return errs
}

func checkRules(logger log.Logger, filename string) (int, tsdb_errors.MultiError) {
	level.Info(logger).Log("msg", "checking", "filename", filename)
	checkErrors := tsdb_errors.MultiError{}

	b, err := ioutil.ReadFile(filename)
	if err != nil {
		checkErrors.Add(err)
		return 0, checkErrors
	}

	var rgs ThanosRuleGroups
	if err := yaml.UnmarshalStrict(b, &rgs); err != nil {
		checkErrors.Add(err)
		return 0, checkErrors
	}

	if errs := rgs.Validate(); errs != nil {
		for _, e := range errs {
			checkErrors.Add(e)
		}
		return 0, checkErrors
	}

	numRules := 0
	for _, rg := range rgs.Groups {
		numRules += len(rg.Rules)
	}

	return numRules, checkErrors
}

//func thanosRuleGroupsToPromRuleGroups(ruleGroups ThanosRuleGroups) rulefmt.RuleGroups {
//	promRuleGroups := rulefmt.RuleGroups{Groups: []rulefmt.RuleGroup{}}
//	for _, g := range ruleGroups.Groups {
//		group := rulefmt.RuleGroup{
//			Name:     g.Name,
//			Interval: g.Interval,
//			Rules:    []rulefmt.RuleNode{},
//		}
//		for _, r := range g.Rules {
//			group.Rules = append(
//				group.Rules,
//				rulefmt.RuleNode{
//					Record:      r.Record,
//					Alert:       r.Alert,
//					Expr:        r.Expr,
//					For:         r.For,
//					Labels:      r.Labels,
//					Annotations: r.Annotations,
//				},
//			)
//		}
//		promRuleGroups.Groups = append(
//			promRuleGroups.Groups,
//			group,
//		)
//	}
//	return promRuleGroups
//}
