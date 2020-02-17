// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"io/ioutil"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/tsdb/errors"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
)

func registerChecks(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "Linting tools for Thanos")
	registerCheckRules(m, cmd, name)
}

func registerCheckRules(m map[string]setupFunc, root *kingpin.CmdClause, name string) {
	checkRulesCmd := root.Command("rules", "Check if the rule files are valid or not.")
	ruleFiles := checkRulesCmd.Arg(
		"rule-files",
		"The rule files to check.",
	).Required().ExistingFiles()

	m[name+" rules"] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, _ opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})
		return checkRulesFiles(logger, ruleFiles)
	}
}

func checkRulesFiles(logger log.Logger, files *[]string) error {
	failed := errors.MultiError{}

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
	PartialResponseStrategy string `yaml:"partial_response_strategy"`
	rulefmt.RuleGroup       `yaml:",inline"`
}

type ThanosRuleGroups struct {
	Groups []ThanosRuleGroup `yaml:"groups"`
}

func checkRules(logger log.Logger, filename string) (int, errors.MultiError) {
	level.Info(logger).Log("msg", "checking", "filename", filename)
	checkErrors := errors.MultiError{}

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

	// We need to convert Thanos rules to Prometheus rules so we can use their validation.
	promRgs := thanosRuleGroupsToPromRuleGroups(rgs)
	if errs := promRgs.Validate(); errs != nil {
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

func thanosRuleGroupsToPromRuleGroups(ruleGroups ThanosRuleGroups) rulefmt.RuleGroups {
	promRuleGroups := rulefmt.RuleGroups{Groups: []rulefmt.RuleGroup{}}
	for _, g := range ruleGroups.Groups {
		group := rulefmt.RuleGroup{
			Name:     g.Name,
			Interval: g.Interval,
			Rules:    []rulefmt.Rule{},
		}
		for _, r := range g.Rules {
			group.Rules = append(
				group.Rules,
				rulefmt.Rule{
					Record:      r.Record,
					Alert:       r.Alert,
					Expr:        r.Expr,
					For:         r.For,
					Labels:      r.Labels,
					Annotations: r.Annotations,
				},
			)
		}
		promRuleGroups.Groups = append(
			promRuleGroups.Groups,
			group,
		)
	}
	return promRuleGroups
}
